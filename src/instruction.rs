use anyhow::{anyhow, Context, Result};
use bytes::Bytes;

use crate::error::ParseError;

#[derive(Debug, Clone)]
pub enum Instruction {
    Set {
        key: String,
        expiry: u128,
        data_size: usize,
        data: Bytes,
    },
    Get {
        key: String,
    },
    Append {
        key: String,
        expiry: u128,
        data_size: usize,
        data: Bytes,
    },
    Prepend {
        key: String,
        expiry: u128,
        data_size: usize,
        data: Bytes,
    },
    Add {
        key: String,
        expiry: u128,
        data_size: usize,
        data: Bytes,
    },
    Replace {
        key: String,
        expiry: u128,
        data_size: usize,
        data: Bytes,
    },
}

pub fn complete_ins(ins: Instruction, data: Bytes) -> Instruction {
    match ins {
        Instruction::Set {
            key,
            expiry,
            data_size,
            data: _,
        } => {
            return Instruction::Set {
                key,
                expiry,
                data_size,
                data,
            };
        }
        Instruction::Append {
            key,
            expiry,
            data_size,
            data: _,
        } => {
            return Instruction::Append {
                key,
                expiry,
                data_size,
                data,
            };
        }
        Instruction::Prepend {
            key,
            expiry,
            data_size,
            data: _,
        } => {
            return Instruction::Prepend {
                key,
                expiry,
                data_size,
                data,
            };
        }
        Instruction::Add {
            key,
            expiry,
            data_size,
            data: _,
        } => {
            return Instruction::Add {
                key,
                expiry,
                data_size,
                data,
            };
        }
        Instruction::Replace {
            key,
            expiry,
            data_size,
            data: _,
        } => {
            return Instruction::Replace {
                key,
                expiry,
                data_size,
                data,
            };
        }
        _ => {
            return ins;
        }
    };
}

pub fn parse_ins_with_data(line: String, data: Bytes) -> Result<Instruction> {
    let parts = line.split_whitespace();
    let mut parts = parts.into_iter();
    match parts.next() {
        Some("set") | Some("append") | Some("prepend") => match parse_string(line) {
            Ok(ins) => {
                return Ok(complete_ins(ins, data));
            }
            Err(err) => match err.downcast_ref() {
                Some(ParseError::InsufficientWaiting(ins, _)) => {
                    return Ok(complete_ins(ins.clone(), data));
                }
                _ => return Err(err),
            },
        },
        _ => return Err(anyhow!(ParseError::InvalidInstruction)),
    };
}

pub fn parse_string(line: String) -> Result<Instruction> {
    let parts = line.split_whitespace();
    let mut parts = parts.into_iter();
    match parts.next() {
        Some("set") => {
            let key = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .to_string();
            let expiry = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<u128>()
                .context(anyhow!(ParseError::InvalidInstruction))?;
            let data_size = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<usize>()
                .context(anyhow!(ParseError::InvalidInstruction))?;

            let iw = anyhow!(ParseError::InsufficientWaiting(
                Instruction::Set {
                    key,
                    expiry,
                    data_size,
                    data: Bytes::new(),
                },
                data_size
            ));
            return Err(iw);
        }
        Some("get") => {
            let key = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .to_string();
            return Ok(Instruction::Get { key });
        }
        Some("append") => {
            let key = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .to_string();
            let expiry = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<u128>()
                .context(anyhow!(ParseError::InvalidInstruction))?;
            let data_size = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<usize>()
                .context(anyhow!(ParseError::InvalidInstruction))?;

            let iw = anyhow!(ParseError::InsufficientWaiting(
                Instruction::Append {
                    key,
                    expiry,
                    data_size,
                    data: Bytes::new(),
                },
                data_size
            ));
            return Err(iw);
        }
        Some("prepend") => {
            let key = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .to_string();
            let expiry = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<u128>()
                .context(anyhow!(ParseError::InvalidInstruction))?;
            let data_size = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<usize>()
                .context(anyhow!(ParseError::InvalidInstruction))?;

            let iw = anyhow!(ParseError::InsufficientWaiting(
                Instruction::Prepend {
                    key,
                    expiry,
                    data_size,
                    data: Bytes::new(),
                },
                data_size
            ));
            return Err(iw);
        }
        Some("add") => {
            let key = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .to_string();
            let expiry = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<u128>()
                .context(anyhow!(ParseError::InvalidInstruction))?;
            let data_size = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<usize>()
                .context(anyhow!(ParseError::InvalidInstruction))?;

            let iw = anyhow!(ParseError::InsufficientWaiting(
                Instruction::Add {
                    key,
                    expiry,
                    data_size,
                    data: Bytes::new(),
                },
                data_size
            ));
            return Err(iw);
        }
        Some("replace") => {
            let key = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .to_string();
            let expiry = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<u128>()
                .context(anyhow!(ParseError::InvalidInstruction))?;
            let data_size = parts
                .next()
                .context(anyhow!(ParseError::InvalidInstruction))?
                .parse::<usize>()
                .context(anyhow!(ParseError::InvalidInstruction))?;

            let iw = anyhow!(ParseError::InsufficientWaiting(
                Instruction::Replace {
                    key,
                    expiry,
                    data_size,
                    data: Bytes::new(),
                },
                data_size
            ));
            return Err(iw);
        }
        _ => return Err(anyhow!(ParseError::InvalidInstruction)),
    };
}
