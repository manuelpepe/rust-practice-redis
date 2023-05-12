/// First iteration of decoder.
///
/// `Decoder` can only parse inputs of 1024 bytes at a time.
/// Returns an error if an uncompleted input is parsed.
///
/// The `DataTypeFrom` trait is also exported implementing synchronous decoding
/// of DataTypes using DataType::from_bytes.
use crate::protocol::{DataType, SafeRead};

use std::io::Read;

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use thiserror::Error;
use tokio::io::AsyncReadExt;

#[derive(Error, Debug)]
pub enum ScanError {
    #[error("stream ended")]
    StreamEnded,

    #[error("unkown datatype {0}")]
    UnkownDataType(char),

    #[error("closed stream")]
    StreamClosed,
}

pub trait DataTypeFrom {
    fn from_bytes(bytes: &mut Bytes) -> Result<DataType>;
}

impl DataTypeFrom for DataType {
    fn from_bytes(bytes: &mut Bytes) -> Result<DataType> {
        let typechar = bytes.get_u8_safe()? as char;
        match typechar {
            '+' => {
                let string = decode_simple_string(bytes)?;
                return Ok(DataType::SimpleString { string });
            }
            '-' => {
                let (type_, error) = decode_error(bytes)?;
                return Ok(DataType::Error { type_, error });
            }
            ':' => {
                let number = decode_integer(bytes)?;
                return Ok(DataType::Integer { number });
            }
            '$' => {
                return match decode_bulk_string(bytes)? {
                    Some(string) => Ok(DataType::BulkString { string }),
                    None => Ok(DataType::NullBulkString),
                };
            }
            '*' => {
                let items = decode_array(bytes)?;
                return Ok(DataType::Array { items });
            }
            '\0' => bail!(ScanError::StreamEnded),
            _ => bail!(ScanError::UnkownDataType(typechar)),
        };
    }
}

/// Reads from Bytes until '\r\n' is found.
fn read_until_rn(bytes: &mut Bytes, buf: &mut Vec<u8>) -> Result<()> {
    loop {
        let mut c = bytes.get_u8_safe()?;
        if c == b'\r' {
            let mut next = bytes.get_u8_safe()?;
            while next == b'\r' {
                // handle cases like '\r\r\r\n'
                buf.push(c);
                c = next;
                next = bytes.get_u8_safe()?;
            }
            if next != b'\n' {
                buf.push(c);
                buf.push(next);
                continue;
            }
            break;
        }
        buf.push(c);
    }
    return Ok(());
}

fn read_until_rn_string(bytes: &mut Bytes) -> Result<String> {
    let mut buf = Vec::new();
    read_until_rn(bytes, &mut buf)?;
    return Ok(String::from_utf8(buf)?);
}

fn read_until_rn_integer(bytes: &mut Bytes) -> Result<isize> {
    return Ok(read_until_rn_string(bytes)?.parse::<isize>()?);
}

/// Decoder for DataType::SimpleString
fn decode_simple_string(bytes: &mut Bytes) -> Result<String> {
    return read_until_rn_string(bytes);
}

/// Decoder for DataType::Error
/// TODO: pending error types implementation
fn decode_error(bytes: &mut Bytes) -> Result<(String, String)> {
    return Ok((String::new(), decode_simple_string(bytes)?));
}

/// Decoder for DataType::Integer
fn decode_integer(bytes: &mut Bytes) -> Result<isize> {
    return read_until_rn_integer(bytes);
}

/// Decoder for DataType::BulkString and DataType::NullBulkString
fn decode_bulk_string(bytes: &mut Bytes) -> Result<Option<String>> {
    let size = read_until_rn_integer(bytes)?;
    if size == -1 {
        return Ok(None);
    }
    if size < -1 {
        bail!("invalid bulk string length");
    }
    let mut data_buf = Vec::with_capacity(size as usize);
    bytes.reader().read_exact(&mut data_buf)?;
    for _ in 0..size {
        data_buf.push(bytes.get_u8_safe()?);
    }
    if bytes.get_u8_safe()? != b'\r' || bytes.get_u8_safe()? != b'\n' {
        bail!("invalid string termination");
    }
    return Ok(Some(String::from_utf8(data_buf)?));
}

/// Decoder for DataType::Array
fn decode_array(bytes: &mut Bytes) -> Result<Vec<DataType>> {
    let size = read_until_rn_integer(bytes)?;
    let mut items = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let created = DataType::from_bytes(bytes)?;
        items.push(created);
    }
    return Ok(items);
}

/// Decode RESP data from a stream
pub struct Decoder<'a, R> {
    stream: &'a mut R,
}

impl<'a, R: AsyncReadExt + std::marker::Unpin> Decoder<'a, R> {
    pub fn new(stream: &'a mut R) -> Self {
        return Decoder { stream: stream };
    }

    pub async fn parse(&mut self) -> Result<Vec<DataType>> {
        // NOTE: Only reads 1024 bytes, so bigger inputs will fail.
        // This is fixed on `decoders::v2::StreamDecoder`.
        let mut buf = [0u8; 1024];
        match self.stream.read(&mut buf).await {
            Ok(0) => bail!(ScanError::StreamClosed),
            _ => {}
        };

        let mut parsed = Vec::new();
        let mut bytes = Bytes::from(buf.to_vec());
        while !bytes.is_empty() {
            let datatype = DataType::from_bytes(&mut bytes);
            match datatype {
                Ok(t) => parsed.push(t),
                Err(e) => match e.downcast_ref() {
                    Some(ScanError::StreamEnded) => break,
                    Some(_) => bail!(e),
                    _ => bail!(e),
                },
            };
        }
        return Ok(parsed);
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use super::{read_until_rn, DataType, DataTypeFrom};

    #[test]
    fn test_read_until_rn_basic() {
        let tests = &[
            "Some string",
            "Some string\nwith new\nlines",
            "with multiple \r\r",
        ];
        for test in tests {
            let expected = String::from(*test);
            let mut buf = Vec::new();
            let mut bytes = Bytes::from(format!("{}\r\n", expected));
            read_until_rn(&mut bytes, &mut buf).unwrap();
            assert_eq!(String::from_utf8(buf).unwrap(), expected);
        }
    }

    #[test]
    fn test_read_until_rn_multiple_rn() {
        let data = String::from("Some string\r\nother string");
        let mut buf = Vec::new();
        let mut bytes = Bytes::from(format!("{}\r\n", data));
        read_until_rn(&mut bytes, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "Some string");
        assert_eq!(
            String::from_utf8(bytes.to_vec()).unwrap(),
            "other string\r\n"
        );
    }

    #[test]
    fn test_read_until_rn_without_delim() {
        let expected = String::from("not finished");
        let mut buf = Vec::new();
        let mut bytes = Bytes::from(expected.clone());
        match read_until_rn(&mut bytes, &mut buf) {
            Err(e) => assert_eq!(e.to_string(), "no bytes left"),
            Ok(..) => assert!(false, "collect should fail"),
        };
        // input buffer was still modified
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_simple_string() {
        let expected = String::from("some string");
        let orig = format!("+{expected}\r\n");
        let mut data = Bytes::from(orig.clone());
        let parsed = DataType::from_bytes(&mut data).unwrap();
        assert_eq!(parsed, DataType::SimpleString { string: expected });
        let encoded = DataType::encode(&parsed).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            String::from(orig),
            "string encoded data differs from original data"
        );
    }

    #[test]
    fn test_decode_error() {
        let expected = String::from("some error");
        let orig = format!("-{expected}\r\n");
        let mut data = Bytes::from(orig.clone());
        let parsed = DataType::from_bytes(&mut data).unwrap();
        assert_eq!(
            parsed,
            DataType::Error {
                type_: String::new(),
                error: expected
            }
        );
        let encoded = DataType::encode(&parsed).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            String::from(orig),
            "string encoded data differs from original data"
        );
    }

    #[test]
    fn test_decode_integer() {
        let tests = &[204123, 0, -1, -2300123, -0];
        for expected in tests {
            let orig = format!(":{expected}\r\n");
            let mut data = Bytes::from(orig.clone());
            let parsed = DataType::from_bytes(&mut data).unwrap();
            assert_eq!(parsed, DataType::Integer { number: *expected });
            let encoded = DataType::encode(&parsed).unwrap();
            assert_eq!(
                String::from_utf8(encoded).unwrap(),
                String::from(orig),
                "string encoded data differs from original data"
            );
        }
    }

    #[test]
    fn test_decode_bulk_string() {
        let tests = &["", "hello", "hello\r\nhello", "hello\nhello"];
        for test in tests {
            let expected = String::from(*test);
            let orig = format!("${}\r\n{}\r\n", expected.len(), expected);
            let mut data = Bytes::from(orig.clone());
            let parsed = DataType::from_bytes(&mut data).unwrap();
            assert_eq!(
                parsed,
                DataType::BulkString {
                    string: String::from(*test)
                }
            );
            let encoded = DataType::encode(&parsed).unwrap();
            assert_eq!(
                String::from_utf8(encoded).unwrap(),
                String::from(orig),
                "string encoded data differs from original data"
            );
        }
    }

    #[test]
    fn test_decode_null_bulk_string() {
        let orig = "$-1\r\n";
        let mut data = Bytes::from(orig);
        let parsed = DataType::from_bytes(&mut data).unwrap();
        assert_eq!(parsed, DataType::NullBulkString);
        let encoded = DataType::encode(&parsed).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            String::from(orig),
            "string encoded data differs from original data"
        );
    }

    #[test]
    fn test_decode_array() {
        let orig =
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*3\r\n+Hello\r\n-World\r\n$11\r\nHello\nWorld\r\n";
        let mut data = Bytes::from(orig);
        let parsed = DataType::from_bytes(&mut data).unwrap();
        assert_eq!(
            parsed,
            DataType::Array {
                items: vec![
                    DataType::Array {
                        items: vec![
                            DataType::Integer { number: 1 },
                            DataType::Integer { number: 2 },
                            DataType::Integer { number: 3 },
                        ]
                    },
                    DataType::Array {
                        items: vec![
                            DataType::SimpleString {
                                string: String::from("Hello")
                            },
                            DataType::Error {
                                type_: String::new(),
                                error: String::from("World")
                            },
                            DataType::BulkString {
                                string: String::from("Hello\nWorld")
                            }
                        ]
                    },
                ]
            }
        );

        let encoded = DataType::encode(&parsed).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            String::from(orig),
            "array encoded data differs from original data"
        );
    }
}
