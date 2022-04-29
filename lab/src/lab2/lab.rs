use crate::lab2::bin_store::BinStore;
use std::{error::Error, sync::Mutex};
use tokio::time;
use tribbler::{
    config::KeeperConfig,
    err::TribResult,
    storage::{BinStorage, Storage},
    trib::Server,
};

use super::{frontend::FrontEnd, storage_client::StorageClient};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStore {
        back_addrs: backs.clone(),
    }))
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let mut interval = time::interval(time::Duration::from_secs(1));
    let mut storage_clients: Vec<StorageClient> = Vec::new();
    for address in kc.backs {
        storage_clients.push(StorageClient {
            addr: format!("http://{}", address.clone()),
        });
    }
    if let Some(tx) = kc.ready.clone() {
        let _ = match tx.send(true) {
            Ok(v) => v,
            Err(e) => return Err(Box::new(e)),
        };
    }
    tokio::select! {
        _ = async move {
            let mut current_clock = 0;
            let mut max_so_far = 0;
            loop {
                interval.tick().await;
                for client in storage_clients.iter() {
                    let clock = match client.clock(current_clock).await {
                        Ok(v) => v,
                        Err(ex) => {
                            if let Some(tx) = kc.ready.clone(){
                                let _ = match tx.send(false) {
                                    Ok(v) => v,
                                    Err(e) => return Result::<(), Box<dyn Error + Send + Sync>>::Err(ex)
                                };
                            }
                            return Result::<(), Box<dyn Error + Send + Sync>>::Err(ex)
                        }
                    };
                    if clock > max_so_far {
                        max_so_far = clock;
                    }
                }
                current_clock = max_so_far + 1;
            }
        } => { }
        _ = async {
            if let Some(mut rx) = kc.shutdown {
                rx.recv().await;
            } else {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                rx.recv().await;
            }
        } => {}
    }
    Ok(())
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontEnd {
        bin_storage: bin_storage,
        cached_users: Mutex::<Vec<String>>::new(vec![]),
    }))
}
