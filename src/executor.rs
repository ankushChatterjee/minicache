use std::{
    sync::RwLock,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};

use crate::{instruction::Instruction, DBItem, Db, LockManager};

pub fn execute(ins: Instruction, cache: Db, lock_manager: LockManager) -> Result<String> {
    let mut key_to_delete: Option<String> = None;
    let mut key_delete_msg: Option<String> = None;
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
                key.clone(),
                DBItem {
                    expiry_secs: expiry,
                    expiry_timestamp: expiry_milis,
                    value: data,
                },
            );
            lock_manager.insert(key, RwLock::new(true));
            Ok("STORED".to_owned())
        }
        Instruction::Get { key } => match cache.get(&key) {
            Some(val) => {
                let db_item = val.value();
                lock_manager.get(&key).unwrap().read();
                if is_expired(&db_item) {
                    // Removing the key directly here can cause a deadlock
                    key_to_delete = Some(key.clone());
                    key_delete_msg = Some("END".to_owned());
                }

                let result = match String::from_utf8(db_item.value.to_vec()) {
                    Ok(value) => format!(
                        "VALUE {} {} {} \n\r{} \n\rEND",
                        key,
                        db_item.expiry_secs,
                        db_item.value.len(),
                        value
                    ),
                    Err(_) => {
                        format!(
                            "VALUE {} {} {} \n\r[object] \n\rEND",
                            key,
                            db_item.expiry_secs,
                            db_item.value.len()
                        )
                    }
                };
                Ok(result)
            }
            None => anyhow::bail!("END"),
        },
        Instruction::Append {
            key,
            expiry: _,
            data_size: _,
            data,
        } => {
            if !lock_manager.contains_key(&key) {
                anyhow::bail!("NOT_STORED");
            }
            lock_manager.get(&key).unwrap().write();
            let mut value_to_insert: Option<DBItem> = None;
            match cache.get(&key) {
                Some(val) => {
                    let db_item = val.value();
                    if is_expired(&db_item) {
                        // Removing the key directly here can cause a deadlock
                        key_to_delete = Some(key.clone());
                        key_delete_msg = Some("NOT_STORED".to_owned());
                    } else {
                        let mut result = BytesMut::new();
                        result.put(db_item.value.clone());
                        result.put(data);

                        value_to_insert = Some(DBItem {
                            expiry_secs: db_item.expiry_secs,
                            expiry_timestamp: db_item.expiry_timestamp,
                            value: Bytes::from(result.to_vec()),
                        });
                    }
                }
                None => anyhow::bail!("NOT_STORED"),
            }
            if let Some(val) = value_to_insert {
                cache.insert(key, val);
                Ok("STORED".to_owned())
            } else {
                Err(anyhow!("NOT_STORED"))
            }
        }
        Instruction::Prepend {
            key,
            expiry: _,
            data_size: _,
            data,
        } => {
            if !lock_manager.contains_key(&key) {
                anyhow::bail!("NOT_STORED");
            }
            lock_manager.get(&key).unwrap().write();
            let mut value_to_insert: Option<DBItem> = None;
            match cache.get(&key) {
                Some(val) => {
                    let db_item = val.value();
                    if is_expired(&db_item) {
                        // Removing the key directly here can cause a deadlock
                        key_to_delete = Some(key.clone());
                        key_delete_msg = Some("NOT_STORED".to_owned());
                    } else {
                        let mut result = BytesMut::new();
                        result.put(data);
                        result.put(db_item.value.clone());

                        value_to_insert = Some(DBItem {
                            expiry_secs: db_item.expiry_secs,
                            expiry_timestamp: db_item.expiry_timestamp,
                            value: Bytes::from(result.to_vec()),
                        });
                    }
                }
                None => anyhow::bail!("NOT_STORED"),
            }
            if let Some(val) = value_to_insert {
                cache.insert(key, val);
                Ok("STORED".to_owned())
            } else {
                Err(anyhow!("NOT_STORED"))
            }
        }
        Instruction::Add {
            key,
            expiry,
            data_size: _,
            data,
        } => {
            if cache.contains_key(&key) {
                println!("contains key");
                lock_manager.get(&key).unwrap().read();
                // check if expired
                let db_item = cache.get(&key).unwrap();
                let db_item = db_item.value();
                lock_manager.get(&key).unwrap().read();
                if !is_expired(&db_item) {
                    return Err(anyhow!("NOT_STORED"));
                }
            }

            return insert_key(key, expiry, data, cache, lock_manager);
        }
        Instruction::Replace {
            key,
            expiry,
            data_size: _,
            data,
        } => {
            let mut insert_value = false;

            if lock_manager.contains_key(&key) {
                lock_manager.get(&key).unwrap().write();
                let db_item = cache.get(&key).unwrap();
                let db_item = db_item.value();
                if !is_expired(db_item) {
                    insert_value = true;
                };
            }
            if insert_value {
                return insert_key(key, expiry, data, cache.clone(), lock_manager);
            } else {
                return Err(anyhow!("NOT_STORED"));
            }
        }
    };

    if let Some(del) = key_to_delete.clone() {
        lock_manager.get(&del).unwrap().write();
        cache.remove(&del);
    }

    if let Some(kdel) = key_to_delete {
        lock_manager.remove(&kdel);
        anyhow::bail!(key_delete_msg.unwrap());
    }
    res
}

fn is_expired(db_item: &DBItem) -> bool {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("TIME ERROR")
        .unwrap()
        .as_millis();
    current_time > db_item.expiry_timestamp && db_item.expiry_timestamp != 0
}

fn insert_key(
    key: String,
    expiry: u128,
    data: Bytes,
    cache: Db,
    lock_manager: LockManager,
) -> Result<String> {
    let mut expiry_milis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("SYSTEM_ERROR")?
        .as_millis()
        + expiry * 1000;
    if expiry == 0 {
        expiry_milis = 0;
    }
    cache.insert(
        key.clone(),
        DBItem {
            expiry_secs: expiry,
            expiry_timestamp: expiry_milis,
            value: data,
        },
    );
    lock_manager.insert(key, RwLock::new(true));
    Ok("STORED".to_owned())
}
