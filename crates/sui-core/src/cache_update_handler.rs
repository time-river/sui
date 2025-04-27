use std::{
    fs, sync::{
        atomic::{AtomicBool, Ordering}, Arc
    }
};

use anyhow::Result;
use dashmap::DashSet;
use serde_json;
use tokio::{
    io::AsyncWriteExt,
    sync::Mutex,
    net::{UnixListener, UnixStream}
};
use sui_types::{
    base_types::ObjectID,
    object::Object
};

pub const SOCKET_PATH: &str = "/tmp/sui_cache_updates.sock";
pub const POOL_RELATED_OBJECTS_PATH: &str = "/home/ubuntu/pool_related_ids.txt";
const DISABLE_POOL_RELATED_OBJECTS: &str = "__DISABLE_POOL_RELATED_OBJECTS_FILE__";

pub fn load_poll_related_ids() -> DashSet<ObjectID> {
    let result = std::env::var(DISABLE_POOL_RELATED_OBJECTS);
    if result.is_ok() {
        return DashSet::new();
    }

    match fs::exists(POOL_RELATED_OBJECTS_PATH) {
        Ok(exist) if exist => fs::read_to_string(POOL_RELATED_OBJECTS_PATH)
            .expect("Failed to read poll related ids file {POOL_RELATED_OBJECTS_PATH}")
            .trim()
            .split('\n')
            .map(|line| line.parse().expect("Failed to parse poll related id: {line} in file {POOL_RELATED_OBJECTS_PATH}"))
            .collect(),

        _ => panic!("Poll related ids file {POOL_RELATED_OBJECTS_PATH} does not exist"),
    }
}

#[derive(Debug)]
pub struct CacheUpdateHandler {
    path: String,
    running: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<UnixStream>>>,
}

impl Default for CacheUpdateHandler {
    fn default() -> Self {
        Self::new(SOCKET_PATH)
    }
}

impl Drop for CacheUpdateHandler {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        let _ = fs::remove_file(&self.path);
    }
}

impl CacheUpdateHandler {
    pub fn new(path: &str) -> Self {
        match fs::exists(path) {
            Ok(exists) if exists => fs::remove_file(path).expect("Failed to remove socket file {path}"),
            _ => {}
        }

        let running = Arc::new(AtomicBool::new(true));
        let listener = UnixListener::bind(path).expect("Failed to bind socket {path}");
        let conns = Arc::new(Mutex::new(vec![]));
        let conns_inside = Arc::clone(&conns);
        let running_internal = running.clone();

        tokio::spawn(async move {
            while running_internal.load(Ordering::SeqCst) {
                let conn = match listener.accept().await {
                    Ok((conn, _)) => conn,
                    Err(_) => continue,
                };

                let mut conns = conns_inside.lock().await;
                conns.push(conn);
            }
        });

        Self {
            path: path.to_string(),
            running,
            conns,
        }
    }

    pub fn notify_written(
        &self,
        objects: Vec<(ObjectID, Object)>
    ) -> Result<()> {
        let msg_bytes = serde_json::to_vec(&objects)?;
        let msg_bytes_len = (msg_bytes.len() as u32).to_be_bytes();

        let conns_internal = Arc::clone(&self.conns);

        tokio::spawn(async move {
            let mut conns = conns_internal.lock().await;
            let mut active_conns = Vec::new();

            while let Some(mut conn) = conns.pop() {
                let result: Result<()> = async {
                    conn.write_all(&msg_bytes_len).await?;
                    conn.write_all(&msg_bytes).await?;

                    Ok(())
                }.await;

                if result.is_ok() {
                    active_conns.push(conn);
                }
            }

            *conns = active_conns;
        });

        Ok(())
    }
}
