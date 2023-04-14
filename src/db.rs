use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub type MapInner = HashMap<String, DBValue>;
pub type Map = Arc<Mutex<MapInner>>;

fn timestamp() -> usize {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    return since_the_epoch.as_millis() as usize;
}

#[derive(Clone)]
pub struct DBValue {
    pub value: Bytes,
    pub expiration: usize,
}

impl DBValue {
    pub fn with_expiration(value: Bytes, mut expiration: usize) -> Self {
        if expiration > 0 {
            let now = timestamp();
            expiration = now + expiration;
        }
        return DBValue {
            value: value,
            expiration: expiration,
        };
    }

    pub fn is_expired(&self) -> bool {
        let now = timestamp();
        return self.expiration != 0 && self.expiration <= now;
    }
}
