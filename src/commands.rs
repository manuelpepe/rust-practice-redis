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
}

#[derive(Debug)]
pub enum Commands {
    PING,
    COMMAND,
}

impl Commands {
    pub fn from_vec(array: Vec<DataType>) -> Result<Self> {
        let cmd = array.get(0).ok_or(ParseError::EmptyArray)?;
        return match cmd {
            DataType::BulkString { string } | DataType::SimpleString { string } => {
                match string.as_str() {
                    "ping" => Ok(Commands::PING),
                    "COMMAND" => Ok(Commands::COMMAND),
                    _ => bail!(ParseError::UnkownCommand(string.clone())),
                }
            }
            _ => bail!(ParseError::InvalidFirstAttribute),
        };
    }

    pub fn get_response(&self) -> Result<DataType> {
        let response = match self {
            Commands::PING => DataType::SimpleString {
                string: "PONG".to_string(),
            },
            Commands::COMMAND => DataType::SimpleString {
                string: "".to_string(),
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
