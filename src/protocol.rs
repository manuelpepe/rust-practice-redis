use std::io::{BufReader, Read};

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use thiserror::Error;

trait SafeRead {
    fn get_u8_safe(&mut self) -> Result<u8>;
}

impl SafeRead for Bytes {
    fn get_u8_safe(&mut self) -> Result<u8> {
        if self.remaining() == 0 {
            bail!("no bytes left");
        }
        return Ok(self.get_u8());
    }
}

#[derive(Error, Debug)]
pub enum ScanError {
    #[error("stream ended")]
    StreamEnded,

    #[error("unkown datatype {0}")]
    UnkownDataType(char),
}

/// DataType represents the available data types on [RESP](https://redis.io/docs/reference/protocol-spec/#resp-protocol-description)
#[derive(Debug, Eq, PartialEq)]
pub enum DataType {
    /// Simple Strings are encoded as follows: a plus character, followed by a string that cannot
    /// contain a CR or LF character (no newlines are allowed), and terminated by CRLF (that is "\r\n").
    ///
    /// Simple Strings are used to transmit non binary-safe strings with minimal overhead. For example,
    /// many Redis commands reply with just "OK" on success. The RESP Simple String is encoded with
    /// the following 5 bytes:
    /// ```
    /// "+OK\r\n"
    /// ```
    /// In order to send binary-safe strings, use RESP Bulk Strings instead.
    ///
    /// When Redis replies with a Simple String, a client library should respond with a string composed
    /// of the first character after the '+' up to the end of the string, excluding the final CRLF bytes.
    SimpleString {
        string: String,
    },

    /// RESP has a specific data type for errors. They are similar to RESP Simple Strings, but the first
    /// character is a minus '-' character instead of a plus. The real difference between Simple Strings
    /// and Errors in RESP is that clients treat errors as exceptions, and the string that composes the
    /// Error type is the error message itself.
    ///
    /// The basic format is:
    /// ```
    /// "-Error message\r\n"
    /// ```
    /// Error replies are only sent when something goes wrong, for instance if you try to perform an
    /// operation against the wrong data type, or if the command does not exist. The client should
    /// raise an exception when it receives an Error reply.
    ///
    /// The following are examples of error replies:
    ///
    /// ```
    /// -ERR unknown command 'helloworld'
    /// -WRONGTYPE Operation against a key holding the wrong kind of value
    /// ```
    ///
    /// The first word after the "-", up to the first space or newline, represents the kind of error returned.
    /// This is just a convention used by Redis and is not part of the RESP Error format.
    ///
    /// For example, ERR is the generic error, while WRONGTYPE is a more specific error that implies that the
    /// client tried to perform an operation against the wrong data type. This is called an Error Prefix and
    /// is a way to allow the client to understand the kind of error returned by the server without checking
    /// the exact error message.
    ///
    /// A client implementation may return different types of exceptions for different errors or provide a generic
    /// way to trap errors by directly providing the error name to the caller as a string.
    ///
    /// However, such a feature should not be considered vital as it is rarely useful, and a limited client
    /// implementation may simply return a generic error condition, such as false.
    Error {
        type_: String,
        error: String,
    },

    /// This type is just a CRLF-terminated string that represents an integer, prefixed by a ":" byte.
    /// For example, `":0\r\n"` and `":1000\r\n"` are integer replies.
    ///
    /// Many Redis commands return RESP Integers, like INCR, LLEN, and LASTSAVE.
    ///
    /// There is no special meaning for the returned integer. It is just an incremental number for
    /// INCR, a UNIX time for LASTSAVE, and so forth. However, the returned integer is guaranteed
    /// to be in the range of a signed 64-bit integer.
    ///
    /// Integer replies are also used in order to return true or false. For instance, commands
    /// like EXISTS or SISMEMBER will return 1 for true and 0 for false.
    ///
    /// Other commands like SADD, SREM, and SETNX will return 1 if the operation was actually performed and 0 otherwise.
    ///
    /// The following commands will reply with an integer:
    /// SETNX, DEL, EXISTS, INCR, INCRBY, DECR, DECRBY, DBSIZE, LASTSAVE, RENAMENX, MOVE, LLEN, SADD, SREM, SISMEMBER, SCARD.
    Integer {
        number: isize,
    },

    /// Bulk Strings are used in order to represent a single binary-safe string up to 512 MB in length.
    ///
    /// Bulk Strings are encoded in the following way:
    ///
    /// - A "$" byte followed by the number of bytes composing the string (a prefixed length), terminated by CRLF.
    /// - The actual string data.
    /// - A final CRLF.
    ///
    /// So the string "hello" is encoded as follows:
    /// ```
    /// "$5\r\nhello\r\n"
    /// ```
    ///
    /// An empty string is encoded as:
    /// ```
    /// "$0\r\n\r\n"
    /// ```
    ///
    /// RESP Bulk Strings can also be used in order to signal non-existence of a value using a special
    /// format to represent a Null value. In this format, the length is -1, and there is no data. Null is represented as:
    /// ```
    /// "$-1\r\n"
    /// ```
    ///
    /// This is called a Null Bulk String.
    ///
    /// The client library API should not return an empty string, but a nil object, when the server replies with
    /// a Null Bulk String. For example, a Ruby library should return 'nil' while a C library should return NULL
    /// (or set a special flag in the reply object).
    BulkString {
        string: String,
    },
    NullBulkString,

    ///Clients send commands to the Redis server using RESP Arrays. Similarly, certain Redis commands, that return collections of elements to the client, use RESP Arrays as their replies. An example is the LRANGE command that returns elements of a list.
    ///
    /// RESP Arrays are sent using the following format:
    ///
    /// - A * character as the first byte, followed by the number of elements in the array as a decimal number, followed by CRLF.
    /// - An additional RESP type for every element of the Array.
    ///
    /// So an empty Array is just the following:
    /// ```
    /// "*0\r\n"
    /// ```
    ///
    /// While an array of two RESP Bulk Strings "hello" and "world" is encoded as:
    /// ```
    /// "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    /// ```
    ///
    /// As you can see after the *<count>CRLF part prefixing the array, the other data types composing the array
    /// are just concatenated one after the other. For example, an Array of three integers is encoded as follows:
    /// ```
    /// "*3\r\n:1\r\n:2\r\n:3\r\n"
    /// ```
    ///
    /// Arrays can contain mixed types, so it's not necessary for the elements to be of the same type.
    /// For instance, a list of four integers and a bulk string can be encoded as follows:
    ///
    /// ```
    /// *5\r\n
    /// :1\r\n
    /// :2\r\n
    /// :3\r\n
    /// :4\r\n
    /// $5\r\n
    /// hello\r\n
    /// ```
    /// (The reply was split into multiple lines for clarity).
    ///
    /// The first line the server sent is *5\r\n in order to specify that five replies will follow.
    /// Then every reply constituting the items of the Multi Bulk reply are transmitted.
    ///
    /// Null Arrays exist as well and are an alternative way to specify a Null value (usually the Null Bulk String
    /// is used, but for historical reasons we have two formats).
    ///
    /// For instance, when the BLPOP command times out, it returns a Null Array that has a count of -1 as in the following example:
    /// ```
    /// "*-1\r\n"
    /// ```
    ///
    /// A client library API should return a null object and not an empty Array when Redis replies with a Null Array.
    /// This is necessary to distinguish between an empty list and a different condition (for instance the timeout
    /// condition of the BLPOP command).
    ///
    /// Nested arrays are possible in RESP. For example a nested array of two arrays is encoded as follows:
    ///
    /// ```
    /// *2\r\n
    /// *3\r\n
    /// :1\r\n
    /// :2\r\n
    /// :3\r\n
    /// *2\r\n
    /// +Hello\r\n
    /// -World\r\n
    /// ```
    /// (The format was split into multiple lines to make it easier to read).
    ///
    /// The above RESP data type encodes a two-element Array consisting of an Array that contains three Integers
    /// (1, 2, 3) and an array of a Simple String and an Error.
    ///
    /// ## Null elements in Arrays
    ///
    /// Single elements of an Array may be Null. This is used in Redis replies to signal that these elements are
    /// missing and not empty strings. This can happen with the SORT command when used with the GET pattern option
    /// if the specified key is missing. Example of an Array reply containing a Null element:
    ///
    /// ```
    /// *3\r\n
    /// $5\r\n
    /// hello\r\n
    /// $-1\r\n
    /// $5\r\n
    /// world\r\n
    /// ```
    ///
    /// The second element is a Null. The client library should return something like this:
    ///
    /// ```
    /// ["hello",nil,"world"]
    /// ```
    ///
    /// Note that this is not an exception to what was said in the previous sections, but an example to further specify the protocol.
    Array {
        items: Vec<DataType>,
    },
}

impl DataType {
    fn from(bytes: &mut Bytes) -> Result<Self> {
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

    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            DataType::SimpleString { string } => encode_string(string),
            _ => bail!("not implemented"),
        }
    }
}

fn encode_string(string: &String) -> Result<Vec<u8>> {
    // TODO: Check string does not contain '\r\n'
    let formatted = format!("+{string}\r\n");
    return Ok(formatted.as_bytes().to_vec());
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
fn decode_error(bytes: &mut Bytes) -> Result<(String, String)> {
    // TODO: pending error types implementation
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
        if bytes.get_u8_safe()? != b'\r' || bytes.get_u8_safe()? != b'\n' {
            bail!("invalid null string format");
        }
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
        let created = DataType::from(bytes)?;
        items.push(created);
    }
    return Ok(items);
}

/// Decode RESP data from a stream
pub struct Decoder<R> {
    stream: BufReader<R>,
}

impl<R: Read> Decoder<R> {
    pub fn new(stream: R) -> Self {
        return Decoder {
            stream: BufReader::new(stream),
        };
    }

    pub fn parse(&mut self) -> Result<Vec<DataType>> {
        // TODO: find out how to best read the stream
        let mut buf = [0u8; 1024];
        self.stream.read(&mut buf)?;

        let mut parsed = Vec::new();
        let mut bytes = Bytes::from(buf.to_vec());
        while !bytes.is_empty() {
            let datatype = DataType::from(&mut bytes);
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

    use super::{read_until_rn, DataType};

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
        let mut data = Bytes::from(format!("+{expected}\r\n"));
        let parsed = DataType::from(&mut data).unwrap();
        assert_eq!(parsed, DataType::SimpleString { string: expected });
    }

    #[test]
    fn test_decode_error() {
        let expected = String::from("some error");
        let mut data = Bytes::from(format!("-{expected}\r\n"));
        let parsed = DataType::from(&mut data).unwrap();
        assert_eq!(
            parsed,
            DataType::Error {
                type_: String::new(),
                error: expected
            }
        );
    }

    #[test]
    fn test_decode_integer() {
        let tests = &[204123, 0, -1, -2300123, -0];
        for expected in tests {
            let mut data = Bytes::from(format!(":{expected}\r\n"));
            let parsed = DataType::from(&mut data).unwrap();
            assert_eq!(parsed, DataType::Integer { number: *expected });
        }
    }

    #[test]
    fn test_decode_bulk_string() {
        let tests = &["", "hello", "hello\r\nhello", "hello\nhello"];
        for test in tests {
            let expected = String::from(*test);
            let mut data = Bytes::from(format!("${}\r\n{}\r\n", expected.len(), expected));
            let parsed = DataType::from(&mut data).unwrap();
            assert_eq!(
                parsed,
                DataType::BulkString {
                    string: String::from(*test)
                }
            );
        }
    }

    #[test]
    fn test_decode_null_bulk_string() {
        let mut data = Bytes::from("$-1\r\n\r\n");
        let parsed = DataType::from(&mut data).unwrap();
        assert_eq!(parsed, DataType::NullBulkString);
    }

    #[test]
    fn test_decode_array() {
        let mut data = Bytes::from(
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*3\r\n+Hello\r\n-World\r\n$11\r\nHello\nWorld\r\n",
        );
        let parsed = DataType::from(&mut data).unwrap();
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
    }

    #[test]
    fn test() {}
}
