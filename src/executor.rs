use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::{instruction::Instruction, DBItem, Db};

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
            cache.insert(
                key,
                DBItem {
                    expiry_secs: expiry,
                    expiry_timestamp: expiry_milis,
                    value: data,
                },
            );
            Ok("STORED".to_owned())
        }
        Instruction::Get { key } => match cache.get(&key) {
            Some(val) => {
                let db_item = val.value();
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("TIME ERROR")?
                    .as_millis();
                if current_time > db_item.expiry_timestamp && db_item.expiry_timestamp != 0 {
                    // Removing the key directly here can cause a deadlock
                    key_to_delete = Some(key);
                }

                let result = match String::from_utf8(db_item.value.to_vec()) {
                    Ok(value) => format!(
                        "VALUE {} {} {}",
                        value,
                        db_item.expiry_secs,
                        db_item.value.len()
                    ),
                    Err(_) => format!(
                        "VALUE [object] {} {}",
                        db_item.expiry_secs,
                        db_item.value.len()
                    ),
                };
                Ok(result)
            }
            None => anyhow::bail!("END"),
        },
    };

    if let Some(key_to_delete) = key_to_delete {
        cache.remove(&key_to_delete);
        anyhow::bail!("END");
    }
    res
}
