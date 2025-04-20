use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering}
    }
};
use serde::{Serialize, Deserialize};
use serde_json;
use anyhow::Result;
use tokio::{
    io::AsyncWriteExt,
    sync::Mutex,
    net::{UnixListener, UnixStream}
};
use sui_types::effects::TransactionEffects;
use sui_json_rpc_types::SuiEvent;

pub const TX_SOCKET_PATH: &str = "/tmp/sui_tx.sock";

#[derive(Debug, Serialize, Deserialize)]
pub struct TxHandlerEvent {
    pub effects: TransactionEffects,
    pub events: Vec<SuiEvent>,
}

#[derive(Clone)]
pub struct TxHandler {
    path: String,
    running: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<UnixStream>>>,
}

impl Default for TxHandler {
    fn default() -> Self {
        Self::new(TX_SOCKET_PATH)
    }
}

impl Drop for TxHandler {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        let _ = fs::remove_file(&self.path);
    }
}

impl TxHandler {
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

    pub fn send_tx_effects_and_events(
        &self,
        effects: &TransactionEffects,
        events: Vec<SuiEvent>
    ) -> Result<()> {
        let msg = TxHandlerEvent { effects: effects.clone(), events };
        let msg_bytes = serde_json::to_vec(&msg)?;
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