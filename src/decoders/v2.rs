/// Second iteration of decoder. This one is implemented as a state machine
/// and async iterator (tokio_stream::Stream), fixing the issue of input limits.
use std::collections::VecDeque;
use std::marker::Unpin;

use anyhow::{bail, Result};
use async_stream::stream;
use bytes::Bytes;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_stream::Stream;

use crate::protocol::{DataType, SafeRead};

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("stream idle")]
    StreamIdle,

    #[error("closed stream")]
    StreamClosed,
}

#[derive(Debug, PartialEq)]
enum State {
    ExpectingDataTypeIdent,
    ExpectingSimpleStringChar,
    ExpectingInteger,
    ExpectingBulkStringSize,
    ExpectingBulkStringChar(isize),
    ExpectingErrorData,
    ExpectingArraySize,
}

enum Type {
    SimpleString,
    Integer,
    BulkString,
    NullBulkString,
    Error,
}

impl Type {
    pub fn as_datatype(&self, buf: &Vec<u8>) -> Result<DataType> {
        let dt = match self {
            Type::SimpleString => DataType::SimpleString {
                string: String::from_utf8(buf.clone())?,
            },
            Type::Integer => DataType::Integer {
                number: String::from_utf8(buf.clone())?.parse()?,
            },
            Type::BulkString => DataType::BulkString {
                string: String::from_utf8(buf.clone())?,
            },
            Type::NullBulkString => DataType::NullBulkString,
            Type::Error => {
                let err = String::from_utf8(buf.clone())?;
                DataType::Error {
                    type_: String::new(),
                    error: err,
                }
            }
        };
        return Ok(dt);
    }
}

/// Decode RESP data from an async stream.
///
/// StreamDecoder works as a State Machine that parses the socket data
/// byte by byte, in order to parse large quanitities of data without
/// memory issues.
///
/// (note that a single object big engough could stil overflow memory)
pub struct StreamDecoder<'a, R> {
    stream: &'a mut R,
    pos: usize,
    input_buffer: Bytes,

    state: State,
    parsing_buffer: Vec<u8>,

    /// stack of array buffers for nested arrays
    array_buffer: Vec<Vec<DataType>>,
    /// stack of array buffer remainders
    array_remainders: Vec<isize>,

    expecting_rn: bool,

    /// Queue of parsed packets
    parsed: VecDeque<DataType>,
}

impl<'a, R: AsyncReadExt + Unpin> StreamDecoder<'a, R> {
    pub fn new(stream: &'a mut R) -> Self {
        return StreamDecoder {
            stream: stream,
            state: State::ExpectingDataTypeIdent,
            input_buffer: Bytes::new(),
            parsing_buffer: Vec::new(),
            array_buffer: Vec::new(),
            array_remainders: Vec::new(),
            parsed: VecDeque::new(),
            pos: 0,
            expecting_rn: false,
        };
    }

    /// converts the parser into an async iterator of parsed objects
    pub fn as_stream(&'a mut self) -> impl Stream<Item = Result<DataType>> + 'a {
        stream! {
            loop {
                if self.parsed.len() > 0 {
                    yield Ok(self.parsed.pop_front().unwrap());
                }
                self.parse_next().await?;
            }
        }
    }

    // get next byte in buffer. if buffer is empty read from stream.
    // may return None if stream is closed or on read errors.
    async fn get_byte(&mut self) -> Option<u8> {
        if self.input_buffer.is_empty() {
            let mut buf = [0u8; 1024];
            match self.stream.read(&mut buf).await {
                Ok(0) | Err(_) => return None,
                Ok(_) => {}
            };
            self.input_buffer = Bytes::from(buf.to_vec());
        }
        return self.input_buffer.get_u8_safe().ok();
    }

    /// parses the next character in the stream, this function moves the
    /// state machine forward.
    async fn parse_next(&mut self) -> Result<()> {
        let cur = match self.get_byte().await {
            Some(b) => b,
            None => bail!(ParseError::StreamClosed),
        };
        match self.state {
            State::ExpectingDataTypeIdent => match self.handle_datatype_ident(cur) {
                Ok(_) => {}
                Err(err) => match err.downcast_ref() {
                    Some(ParseError::StreamIdle) => {}
                    _ => bail!(err),
                },
            },
            State::ExpectingSimpleStringChar => self.handle_simple_string_char(cur)?,
            State::ExpectingInteger => self.handle_integer(cur)?,
            State::ExpectingBulkStringSize => self.handle_bulk_string_size(cur)?,
            State::ExpectingBulkStringChar(remaining) => {
                self.handle_bulk_string_char(cur, remaining)?
            }
            State::ExpectingErrorData => self.handle_error_data(cur)?,
            State::ExpectingArraySize => self.handle_array_size(cur)?,
        }
        self.pos += 1;
        Ok(())
    }

    fn handle_datatype_ident(&mut self, byte: u8) -> Result<()> {
        self.state = match byte {
            b'+' => State::ExpectingSimpleStringChar,
            b':' => State::ExpectingInteger,
            b'$' => State::ExpectingBulkStringSize,
            b'-' => State::ExpectingErrorData,
            b'*' => State::ExpectingArraySize,
            _ => bail!(ParseError::StreamIdle),
        };
        return Ok(());
    }

    fn handle_simple_read(&mut self, byte: u8, type_: Type) -> Result<()> {
        match byte {
            b'\r' => self.expecting_rn = true,
            b'\n' if self.expecting_rn => {
                self.expecting_rn = false;
                self.commit_buffer(type_)?;
                self.state = State::ExpectingDataTypeIdent;
            }
            _ if self.expecting_rn => {
                self.parsing_buffer.push(b'\r');
                self.parsing_buffer.push(byte);
            }
            _ => self.parsing_buffer.push(byte),
        }
        Ok(())
    }

    fn handle_simple_string_char(&mut self, byte: u8) -> Result<()> {
        return self.handle_simple_read(byte, Type::SimpleString);
    }

    fn handle_integer(&mut self, byte: u8) -> Result<()> {
        return self.handle_simple_read(byte, Type::Integer);
    }

    fn handle_bulk_string_size(&mut self, byte: u8) -> Result<()> {
        match byte {
            b'\r' => self.expecting_rn = true,
            b'\n' if self.expecting_rn => {
                self.expecting_rn = false;
                let size = self.buffer_as_isize()?;
                if size >= 0 {
                    self.state = State::ExpectingBulkStringChar(size);
                } else if size == -1 {
                    self.commit_buffer(Type::NullBulkString)?;
                    self.state = State::ExpectingDataTypeIdent;
                }
            }
            _ if self.expecting_rn => {
                self.parsing_buffer.push(b'\r');
                self.parsing_buffer.push(byte);
            }
            _ => self.parsing_buffer.push(byte),
        }
        Ok(())
    }

    fn handle_bulk_string_char(&mut self, byte: u8, remaining: isize) -> Result<()> {
        if remaining < 0 && byte != b'\r' {
            bail!("error parsing bulk string")
        }
        match byte {
            b'\r' if remaining == 0 => self.expecting_rn = true,
            b'\n' if self.expecting_rn => {
                self.expecting_rn = false;
                self.commit_buffer(Type::BulkString)?;
                self.state = State::ExpectingDataTypeIdent;
            }
            _ if self.expecting_rn => bail!("error parsing bulk string"),
            _ => {
                self.parsing_buffer.push(byte);
                self.state = State::ExpectingBulkStringChar(remaining - 1);
            }
        }
        Ok(())
    }

    fn handle_error_data(&mut self, byte: u8) -> Result<()> {
        return self.handle_simple_read(byte, Type::Error);
    }

    fn handle_array_size(&mut self, byte: u8) -> Result<()> {
        match byte {
            b'\r' => self.expecting_rn = true,
            b'\n' if self.expecting_rn => {
                self.expecting_rn = false;
                let size = self.buffer_as_isize()?;
                self.array_buffer.push(Vec::with_capacity(size as usize));
                self.array_remainders.push(size);
                self.state = State::ExpectingDataTypeIdent;
            }
            _ if self.expecting_rn => bail!("got '\\r' in the middle of array size"),
            _ => self.parsing_buffer.push(byte),
        }
        Ok(())
    }

    /// commit_buffer parses the current buffer as the given Type (returning a protocol::DataType),
    /// pushes it to either the current array or the final list of parsed items
    /// and empties the buffer.
    fn commit_buffer(&mut self, type_: Type) -> Result<()> {
        let data = type_.as_datatype(&self.parsing_buffer)?;
        if self.array_buffer.len() > 0 {
            // parsing array, item is pushed to last array in stack
            let mut storage = self.array_buffer.pop().unwrap();
            storage.push(data);

            let remainder = self.array_remainders.pop().unwrap();
            let new_remainder = remainder - 1;
            if new_remainder == 0 {
                // current array has been parsed
                let array = DataType::Array { items: storage };
                if self.array_buffer.len() == 0 {
                    // last array done, add to parsed
                    self.parsed.push_back(array);
                } else {
                    // nested array done, add to previous array in stack.
                    let mut parent = self.array_buffer.pop().unwrap();
                    parent.push(array);
                    self.array_buffer.push(parent);
                }
            } else {
                self.array_remainders.push(remainder - 1);
                self.array_buffer.push(storage);
            }
        } else {
            // outside array, add to parsed
            self.parsed.push_back(data);
        }
        // emtpy buffer
        self.parsing_buffer.clear();
        return Ok(());
    }

    fn buffer_as_isize(&mut self) -> Result<isize> {
        let num = String::from_utf8(self.parsing_buffer.clone())?.parse::<isize>()?;
        self.parsing_buffer.clear();
        return Ok(num);
    }
}
