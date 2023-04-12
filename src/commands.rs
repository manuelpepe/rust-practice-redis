use crate::protocol::DataType;

use anyhow::{bail, Result};
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
                    _ => bail!(ParseError::UnkownCommand(string.clone())),
                }
            }
            _ => bail!(ParseError::InvalidFirstAttribute),
        };
    }

    pub fn execute(&self) -> Result<DataType> {
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
