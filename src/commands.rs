use crate::{protocol::DataType, Map};

use anyhow::{bail, Result};
use bytes::Bytes;
use thiserror::Error;

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
}

#[derive(Debug)]
pub enum Commands {
    PING,
    COMMAND,
    ECHO { message: String },
    SET { key: String, value: Bytes },
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
                        let message = match array.get(1) {
                            Some(
                                DataType::SimpleString { string } | DataType::BulkString { string },
                            ) => string,
                            _ => bail!(ParseError::BadArguments),
                        };
                        return Ok(Commands::ECHO {
                            message: message.clone(),
                        });
                    }
                    "SET" => {
                        let key = match array.get(1) {
                            Some(
                                DataType::SimpleString { string } | DataType::BulkString { string },
                            ) => string,
                            _ => bail!(ParseError::BadArguments),
                        };
                        let value = match array.get(2) {
                            Some(
                                DataType::SimpleString { string } | DataType::BulkString { string },
                            ) => string,
                            _ => bail!(ParseError::BadArguments),
                        };
                        return Ok(Commands::SET {
                            key: key.clone(),
                            value: Bytes::from(value.clone()),
                        });
                    }
                    "GET" => {
                        let key = match array.get(1) {
                            Some(
                                DataType::SimpleString { string } | DataType::BulkString { string },
                            ) => string,
                            _ => bail!(ParseError::BadArguments),
                        };
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
            Commands::SET { key, value } => {
                let mut map = map.lock().unwrap();
                let mut resp = DataType::SimpleString {
                    string: String::from("OK"),
                };
                if let Some(v) = map.get(key) {
                    resp = DataType::BulkString {
                        string: String::from_utf8(v.to_vec())?,
                    };
                }
                map.insert(key.clone(), value.clone());
                resp
            }
            Commands::GET { key } => {
                let map = map.lock().unwrap();
                match map.get(key) {
                    Some(v) => DataType::BulkString {
                        string: String::from_utf8(v.to_vec())?,
                    },
                    None => DataType::NullBulkString {},
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
