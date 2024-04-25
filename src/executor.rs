use anyhow::{Context, Result};

use crate::{instruction::Instruction, Db};

pub fn execute(ins: Instruction, cache: Db) -> Result<String> {
    match ins {
        Instruction::Set {
            key,
            expiry: _,
            data_size: _,
            data,
        } => {
            cache.insert(key, data);
            return Ok("STORED".to_owned());
        }
        Instruction::Get { key } => match cache.get(&key) {
            Some(data) => {
                let result = format!(
                    "VALUE {} {}",
                    String::from_utf8(data.value().to_vec()).context("Not valid UTF-8")?,
                    data.value().len()
                );
                return Ok(result);
            }
            None => anyhow::bail!("END"),
        },
    }
}
