use crate::{
    db::{DBValue, Map},
    protocol::DataType,
};

use anyhow::{bail, Result};
use bytes::Bytes;
use thiserror::Error;

macro_rules! get_type_or_bad_arguments {
    ($array:ident, $ix:literal, $goodmatch:pat => $val:ident) => {
        match $array.get($ix) {
            $goodmatch => $val,
            _ => bail!(ParseError::BadArguments),
        }
    };
}

macro_rules! get_string_or_bad_args {
    ($array:ident, $ix:literal) => {
        get_type_or_bad_arguments!{$array, $ix, Some(DataType::SimpleString { string } | DataType::BulkString { string }) => string}
    };
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("empty array")]
    EmptyArray,

    #[error("unkown command: {0}")]
    UnkownCommand(String),

    #[error("invalid first attribute for command. should be SimpleString or BulkString.")]
    InvalidFirstAttribute,

    #[error("invalid command datatype. datatype should be array.")]
    InvalidCommandDataType,

    #[error("invalid format for command arguments.")]
    BadArguments,

    #[error("Option '{0}' for {1} not supported")]
    UnsupportedOption(String, String),
}

#[derive(Debug)]
pub enum Commands {
    /// PING responds with PONG
    PING,

    /// COMMAND responds with an Array of available server features.
    COMMAND,

    /// ECHO responds with the received message as a BulkString.
    ECHO { message: String },

    /// SET stores 'value' under 'key' in the in-memory database.
    /// The value can have a optional 'expiry' (PX option).
    /// If the key is already set, responds with old value as a BulkString.
    /// Otherwise responds "OK" as a SimpleString.
    SET {
        key: String,
        value: Bytes,
        expiry: usize,
    },

    /// GET returns the value of 'key' in the in-memory database as a BulkString .
    /// If the key is not set or expired, responds with a NullBulkString.
    GET { key: String },
}

impl Commands {
    pub fn from_vec(array: Vec<DataType>) -> Result<Self> {
        let cmd = array.get(0).ok_or(ParseError::EmptyArray)?;

        return match cmd {
            DataType::BulkString { string } | DataType::SimpleString { string } => {
                match string.to_uppercase().as_str() {
                    "PING" => Ok(Commands::PING),
                    "COMMAND" => Ok(Commands::COMMAND),
                    "ECHO" => {
                        let message = get_string_or_bad_args!(array, 1);
                        return Ok(Commands::ECHO {
                            message: message.clone(),
                        });
                    }
                    "SET" => {
                        let key = get_string_or_bad_args!(array, 1);
                        let value = get_string_or_bad_args!(array, 2);
                        let opt: &String;
                        let mut msdelay: isize = 0;
                        if array.len() > 4 {
                            opt = get_string_or_bad_args!(array, 3);
                            if !opt.to_uppercase().eq("PX") {
                                bail!(ParseError::UnsupportedOption(
                                    opt.clone(),
                                    "SET".to_string()
                                ))
                            }
                            msdelay = get_string_or_bad_args!(array, 4).parse()?;
                        }
                        return Ok(Commands::SET {
                            key: key.clone(),
                            value: Bytes::from(value.clone()),
                            expiry: msdelay as usize,
                        });
                    }
                    "GET" => {
                        let key = get_string_or_bad_args!(array, 1);
                        return Ok(Commands::GET { key: key.clone() });
                    }
                    _ => bail!(ParseError::UnkownCommand(string.clone())),
                }
            }
            _ => bail!(ParseError::InvalidFirstAttribute),
        };
    }

    pub fn execute(&self, map: Map) -> Result<DataType> {
        let response = match self {
            Commands::PING => DataType::SimpleString {
                string: "PONG".to_string(),
            },
            Commands::COMMAND => DataType::SimpleString {
                string: "".to_string(),
            },
            Commands::ECHO { message } => DataType::BulkString {
                string: message.clone(),
            },
            Commands::SET { key, value, expiry } => {
                let mut map = map.lock().unwrap();
                let new_value = DBValue::with_expiration(value.clone(), *expiry);
                let old_value = map.insert(key.clone(), new_value);
                let resp = match old_value {
                    Some(v) if !v.is_expired() => DataType::BulkString {
                        string: String::from_utf8(v.value.to_vec())?,
                    },
                    _ => DataType::SimpleString {
                        string: String::from("OK"),
                    },
                };
                resp
            }
            Commands::GET { key } => {
                let map = map.lock().unwrap();
                match map.get(key) {
                    Some(v) if !v.is_expired() => DataType::BulkString {
                        string: String::from_utf8(v.value.to_vec())?,
                    },
                    _ => DataType::NullBulkString {},
                }
            }
        };
        return Ok(response);
    }
}

pub fn parse_command(data: DataType) -> Result<Commands> {
    let cmd = match data {
        DataType::Array { items } => Commands::from_vec(items)?,
        _ => bail!(ParseError::InvalidCommandDataType),
    };
    return Ok(cmd);
}
