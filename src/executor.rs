use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::{instruction::Instruction, Db};

pub fn execute(ins: Instruction, cache: Db) -> Result<String> {
    let mut key_to_delete: Option<String> = None;
    let res: Result<String> = match ins {
        Instruction::Set {
            key,
            expiry,
            data_size: _,
            data,
        } => {
            let mut expiry_milis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("TIME ERROR")?
                .as_millis()
                + expiry * 1000;
            if expiry == 0 {
                expiry_milis = 0;
            }
            cache.insert(key, (expiry_milis, data));
            Ok("STORED".to_owned())
        }
        Instruction::Get { key } => match cache.get(&key) {
            Some(val) => {
                let (expiry, data) = val.value();
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("TIME ERROR")?
                    .as_millis();
                if current_time > *expiry && *expiry != 0 {
                    // Removing the key directly here can cause a deadlock
                    key_to_delete = Some(key);
                }
                let result = format!(
                    "VALUE {} {}",
                    String::from_utf8(data.to_vec()).context("Not valid UTF-8")?,
                    data.len()
                );
                Ok(result)
            }
            None => anyhow::bail!("END"),
        },
    };

    if key_to_delete.is_some() {
        cache.remove(&key_to_delete.unwrap());
        anyhow::bail!("END");
    }
    res
}
