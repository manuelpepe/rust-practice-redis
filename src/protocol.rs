use anyhow::{bail, Result};
use bytes::{Buf, Bytes};

pub trait SafeRead {
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
    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            DataType::Integer { number } => encode_integer(*number),
            DataType::SimpleString { string } => encode_simple_string(string),
            DataType::BulkString { string } => encode_bulk_string(string),
            DataType::NullBulkString => encode_null_string(),
            DataType::Error { type_, error } => encode_error(type_, error),
            DataType::Array { items } => encode_array(items),
        }
    }
}

fn encode_integer(number: isize) -> Result<Vec<u8>> {
    let formatted = format!(":{number}\r\n");
    return Ok(formatted.as_bytes().to_vec());
}

fn encode_simple_string(string: &String) -> Result<Vec<u8>> {
    // TODO: Check string does not contain '\r\n'
    let formatted = format!("+{string}\r\n");
    return Ok(formatted.as_bytes().to_vec());
}

fn encode_bulk_string(string: &String) -> Result<Vec<u8>> {
    let formatted = format!("${}\r\n{}\r\n", string.len(), string);
    return Ok(formatted.as_bytes().to_vec());
}

fn encode_null_string() -> Result<Vec<u8>> {
    return Ok("$-1\r\n".as_bytes().to_vec());
}

fn encode_error(type_: &String, string: &String) -> Result<Vec<u8>> {
    let mut ftype = type_.clone();
    if !type_.eq("") {
        ftype = format!("{type_} ")
    }
    let formatted = format!("-{ftype}{string}\r\n");
    return Ok(formatted.as_bytes().to_vec());
}

fn encode_array(items: &Vec<DataType>) -> Result<Vec<u8>> {
    let mut buf = format!("*{}\r\n", items.len()).as_bytes().to_vec();
    for item in items {
        let mut item_data = DataType::encode(&item)?;
        buf.append(&mut item_data);
    }
    return Ok(buf);
}
