use anyhow::{anyhow, Context, Ok, Result};
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
        _ => {
            return ins;
        }
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
        _ => return Err(anyhow!(ParseError::InvalidInstruction)),
    };
}
