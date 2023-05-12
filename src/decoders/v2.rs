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

#[derive(Error, Debug, PartialEq, Eq)]
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
                    self.parsing_buffer.clear();
                } else if size == -1 {
                    self.commit_buffer(Type::NullBulkString)?;
                    self.state = State::ExpectingDataTypeIdent;
                }
            }
            _ if self.expecting_rn => bail!("error parsing integer"),
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
            self.array_buffer.push(storage);
            // update remainder
            let remainder = self.array_remainders.pop().unwrap();
            let new_remainder = remainder - 1;
            self.array_remainders.push(new_remainder);
            if new_remainder == 0 {
                // commit all completed array buffers..
                while let Some(()) = self.commit_array_buffer() {}
            }
        } else {
            // outside array, add to parsed
            self.parsed.push_back(data);
        }
        self.parsing_buffer.clear();
        return Ok(());
    }

    /// Commits the last array buffer from the stack to either the array above it or
    /// to the buffer of parsed objects. Given that, when commiting to a parent array, the parent array
    /// could itsef be completed (i.e. reminders goes to 0), an option return type is used to signal
    /// if the function should be called again to keep commiting results upwards.
    fn commit_array_buffer(&mut self) -> Option<()> {
        let remainder = self.array_remainders.pop().unwrap();
        if remainder > 0 {
            // Next array is not done, stop calling...
            self.array_remainders.push(remainder);
            return None;
        }
        assert_eq!(remainder, 0);

        let items = self.array_buffer.pop().unwrap();
        let array = DataType::Array { items: items };
        if self.array_buffer.len() > 0 {
            // nested array done, add to previous array in stack and decrease its remainders by 1.
            let mut parent = self.array_buffer.pop().unwrap();
            parent.push(array);
            self.array_buffer.push(parent);
            let parent_remainders = self.array_remainders.pop().unwrap();
            self.array_remainders.push(parent_remainders - 1);
            return Some(()); // Next array could be done so check it by calling again
        } else {
            // last array done, add to parsed
            self.parsed.push_back(array);
            return None; // No more arrays in buffer, stop calling../
        }
    }

    fn buffer_as_isize(&mut self) -> Result<isize> {
        let num = String::from_utf8(self.parsing_buffer.clone())?.parse::<isize>()?;
        self.parsing_buffer.clear();
        return Ok(num);
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use tokio::io::BufReader;

    use tokio_stream::StreamExt;

    use crate::{
        decoders::v2::{ParseError, StreamDecoder},
        protocol::DataType,
    };

    macro_rules! test_decode {
        ($data:ident, $eq:expr) => {
            let mut reader = BufReader::new($data.as_bytes());
            let mut decoder = StreamDecoder::new(&mut reader);
            let mut stream = Box::pin(decoder.as_stream());
            let item = stream
                .next()
                .await
                .expect("should have parsed a value")
                .expect("first parsed value shouldn't be error");
            assert_eq!(item, $eq);
            let end = stream
                .next()
                .await
                .expect("should have a value left in queue")
                .err()
                .unwrap();
            assert!(match end.downcast_ref::<ParseError>() {
                Some(ParseError::StreamClosed) => true,
                _ => false,
            });
        };
    }

    macro_rules! that_array {
        () => {
            DataType::Array {
                items: vec![
                    DataType::Array {
                        items: vec![
                            DataType::Integer { number: 1 },
                            DataType::Integer { number: 2 },
                            DataType::Integer { number: 3 },
                        ],
                    },
                    DataType::Array {
                        items: vec![
                            DataType::SimpleString {
                                string: String::from("Hello"),
                            },
                            DataType::Error {
                                type_: String::new(),
                                error: String::from("World"),
                            },
                            DataType::BulkString {
                                string: String::from("Hello\nWorld"),
                            },
                        ],
                    },
                ],
            }
        };
    }

    #[tokio::test]
    async fn test_decode_simple_string() {
        let expected = String::from("some string");
        let orig = format!("+{expected}\r\n");
        test_decode!(orig, DataType::SimpleString { string: expected });
    }

    #[tokio::test]
    async fn test_decode_integer() {
        let tests = &[204123, 0, -1, -2300123, -0];
        for expected in tests {
            let orig = format!(":{expected}\r\n");
            test_decode!(orig, DataType::Integer { number: *expected });
        }
    }

    #[tokio::test]
    async fn test_decode_bulk_string() {
        let tests = &["", "hello", "hello\r\nhello", "hello\nhello"];
        for test in tests {
            let expected = String::from(*test);
            let orig = format!("${}\r\n{}\r\n", expected.len(), expected);
            test_decode!(
                orig,
                DataType::BulkString {
                    string: String::from(*test)
                }
            );
        }
    }

    #[tokio::test]
    async fn test_decode_null_bulk_string() {
        let orig = "$-1\r\n";
        test_decode!(orig, DataType::NullBulkString);
    }

    #[tokio::test]
    async fn test_decode_error() {
        let expected = String::from("some error");
        let orig = format!("-{expected}\r\n");
        test_decode!(
            orig,
            DataType::Error {
                type_: String::new(),
                error: expected
            }
        );
    }

    #[tokio::test]
    async fn test_decode_array() {
        let orig = String::from(
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*3\r\n+Hello\r\n-World\r\n$11\r\nHello\nWorld\r\n",
        );
        test_decode!(orig, that_array!());
    }

    #[tokio::test]
    async fn test_all() {
        let expected_err = String::from("some error");
        let expected_simple_string = String::from("hellohello");
        let expected_bulk_string = String::from("hello\nhello");
        let expected_int = -4231232;
        let orig = String::from(format!(
            "{}{}{}{}{}{}",
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*3\r\n+Hello\r\n-World\r\n$11\r\nHello\nWorld\r\n",
            format!("-{expected_err}\r\n"),
            format!(
                "${}\r\n{}\r\n",
                expected_bulk_string.len(),
                expected_bulk_string
            ),
            format!(":{expected_int}\r\n"),
            format!("+{expected_simple_string}\r\n"),
            "$-1\r\n",
        ));
        let mut reader = BufReader::new(orig.as_bytes());
        let mut decoder = StreamDecoder::new(&mut reader);
        let stream = decoder.as_stream();
        let item: Vec<Result<DataType>> = stream.collect().await;
        let values: Vec<&DataType> = item
            .iter()
            .filter(|r| r.is_ok())
            .map(|r| r.as_ref().unwrap())
            .collect();
        assert_eq!(
            values,
            vec![
                &that_array!(),
                &DataType::Error {
                    type_: String::new(),
                    error: expected_err
                },
                &DataType::BulkString {
                    string: expected_bulk_string
                },
                &DataType::Integer {
                    number: expected_int
                },
                &DataType::SimpleString {
                    string: expected_simple_string
                },
                &DataType::NullBulkString,
            ]
        );
    }
}
