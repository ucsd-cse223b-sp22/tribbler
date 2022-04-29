use std::net::ToSocketAddrs; // final version

use crate::lab1::client::StorageClient;
use crate::lab1::server::StorageServer;
use tonic::transport::Server;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};
/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    let storage_server = StorageServer::new(config.storage);
    let mut addr_iter = match config.addr.to_socket_addrs() {
        Ok(v) => v,
        Err(e) => {
            if let Some(tx) = config.ready.clone() {
                let _ = match tx.send(false) {
                    Ok(v) => v,
                    Err(e) => return Err(Box::new(e)),
                };
            }
            return Err(Box::new(e));
        }
    };
    let addr_str = match addr_iter.next() {
        Some(v) => v,
        None => {
            if let Some(tx) = config.ready.clone() {
                let _ = match tx.send(false) {
                    Ok(v) => v,
                    Err(e) => return Err(Box::new(e)),
                };
            }
            return Err("Empty Addr".into());
        }
    };
    if let Some(tx) = config.ready.clone() {
        let _ = match tx.send(true) {
            Ok(v) => v,
            Err(e) => return Err(Box::new(e)),
        };
    }
    match Server::builder()
        .add_service(TribStorageServer::new(storage_server))
        .serve_with_shutdown(addr_str, async {
            if let Some(mut rx) = config.shutdown {
                rx.recv().await;
            } else {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                rx.recv().await;
            }
        })
        .await
    {
        Ok(v) => v,
        Err(e) => {
            if let Some(tx) = config.ready.clone() {
                let _ = match tx.send(false) {
                    Ok(v) => v,
                    Err(e) => return Err(Box::new(e)),
                };
            }
        }
    };
    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    Ok(Box::new(StorageClient {
        addr: addr.to_string(),
    }))
}
