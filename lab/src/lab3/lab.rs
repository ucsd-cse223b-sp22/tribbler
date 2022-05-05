use crate::{
    keeper::{keeper_sync_client::KeeperSyncClient, keeper_sync_server::KeeperSyncServer},
    lab2::bin_store::BinStore,
};
use std::{
    cmp::min,
    mem,
    sync::{Arc, Mutex, RwLock},
};
use tokio::time::{self, interval};
use tribbler::{
    config::KeeperConfig,
    err::TribResult,
    storage::{BinStorage, Storage},
    trib::Server,
};

use super::{
    frontend::FrontEnd, keeper_impl::Keeper, lab3_bin_store_client::LiveBackends,
    storage_client::StorageClient,
};

// use super::{frontend::FrontEnd, storage_client::StorageClient};

pub struct Allocation(i32, i32);

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
    let keeper = Keeper {
        max_clock_so_far: Arc::new(RwLock::new(0u64)),
        live_backends_list_bc: Arc::new(RwLock::new(LiveBackends {
            backs: vec![],
            is_alive_list: vec![],
        })),
        live_backends_list_kp: Arc::new(RwLock::new(LiveBackends {
            backs: vec![],
            is_alive_list: vec![],
        })),
        first_bc_index: Arc::new(RwLock::new((kc.this * 30) as u32)),
        last_bc_index: Arc::new(RwLock::new((kc.this * 30 + 29) as u32)),
        backs: kc.backs.clone(),
        addrs: kc.addrs.clone(),
        index: kc.this as u32,
        cached_conn: vec![None; kc.addrs.len()],
    };

    // keeper.clone().sync_clocks_and_compute_live_bc();

    // initial backend allocation code start
    let bc_len = kc.backs.len();
    let kp_len = kc.addrs.len();

    let n = bc_len / kp_len;

    let mut allocations: Vec<Allocation> = vec![];
    let mut curr_start = 0;
    for i in 0..kp_len {
        if curr_start >= bc_len {
            allocations.push(Allocation(-1, (bc_len - 1) as i32));
            continue;
        } else {
            allocations.push(Allocation(
                curr_start as i32,
                min((curr_start + n - 1) as i32, (bc_len - 1) as i32),
            ));
            curr_start += n;
        }
    }
    // initial backend allocation code end
    // 1 round keeper-backend
    // 1 round keeper-keeper
    // send ready signal
    if let Some(tx) = kc.ready.clone() {
        let _ = match tx.send(true) {
            Ok(v) => v,
            Err(e) => return Err(Box::new(e)),
        };
    }

    let mut handles = vec![];

    // this thread runs server that listens for get_heartbeat requests
    handles.push(tokio::spawn(build_and_serve_keeper_server(keeper.clone())));

    // this thread periodically calls get_heartbeat on all keepers
    handles.push(tokio::spawn(run_keeper_heartbeat(keeper.clone())));

    handles.push(tokio::spawn(sync_clocks_and_compute_live_bc(
        keeper.clone(),
    )));

    if let Some(mut rx) = kc.shutdown {
        rx.recv().await;
    } else {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        rx.recv().await;
    }

    // for handle in handles {
    //     match join!(handle) {
    //         (Ok(_),) => (),
    //         (Err(e),) => {}
    //     };
    // }

    // let mut interval = time::interval(time::Duration::from_secs(1));
    // let mut storage_clients: Vec<StorageClient> = Vec::new();
    // for address in kc.backs {
    //     storage_clients.push(StorageClient {
    //         addr: format!("http://{}", address.clone()),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     });
    // }
    // if let Some(tx) = kc.ready.clone() {
    //     let _ = match tx.send(true) {
    //         Ok(v) => v,
    //         Err(e) => return Err(Box::new(e)),
    //     };
    // }
    // tokio::select! {
    //     _ = async move {
    //         let mut current_clock = 0;
    //         let mut max_so_far = 0;
    //         loop {
    //             interval.tick().await;
    //             for client in storage_clients.iter() {
    //                 let clock = match client.clock(current_clock).await {
    //                     Ok(v) => v,
    //                     Err(ex) => {
    //                         if let Some(tx) = kc.ready.clone(){
    //                             let _ = match tx.send(false) {
    //                                 Ok(v) => v,
    //                                 Err(e) => return Result::<(), Box<dyn Error + Send + Sync>>::Err(ex)
    //                             };
    //                         }
    //                         return Result::<(), Box<dyn Error + Send + Sync>>::Err(ex)
    //                     }
    //                 };
    //                 if clock > max_so_far {
    //                     max_so_far = clock;
    //                 }
    //             }
    //             current_clock = max_so_far + 1;
    //         }
    //     } => { }
    //     _ = async {
    //         if let Some(mut rx) = kc.shutdown {
    //             rx.recv().await;
    //         } else {
    //             let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    //             rx.recv().await;
    //         }
    //     } => {}
    // }
    Ok(())
}

pub async fn sync_clocks_and_compute_live_bc(keeper: Keeper) -> TribResult<()> {
    let max_clock_so_far = Arc::clone(&keeper.max_clock_so_far);
    *max_clock_so_far.write().unwrap() = 0;
    let mut storage_clients = Vec::new();

    for address in keeper.backs[*keeper.first_bc_index.read().unwrap() as usize
        ..(*keeper.last_bc_index.read().unwrap() + 1) as usize]
        .iter()
    {
        storage_clients.push(StorageClient {
            addr: format!("http://{}", address.clone()),
            cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
        });
    }

    // 1 second ticker
    let mut interval = time::interval(time::Duration::from_millis(1000));

    loop {
        interval.tick().await;
        helper(keeper.clone(), &storage_clients).await?;
    }
}

async fn helper(mut keeper: Keeper, storage_clients: &Vec<StorageClient>) -> TribResult<()> {
    let max_clock_so_far = Arc::clone(&keeper.max_clock_so_far);
    let c = *max_clock_so_far.read().unwrap();
    for (idx, storage_client) in storage_clients.iter().enumerate() {
        let mut live_backends_list_bc = keeper.live_backends_list_bc.write().unwrap();
        let offset = *keeper.first_bc_index.read().unwrap() as usize;
        live_backends_list_bc.backs[offset + idx] = storage_client.addr.clone();

        let storage_client_clock = storage_client.clock(c + 1u64).await;
        match storage_client_clock {
            Ok(clock_val) => {
                live_backends_list_bc.is_alive_list[offset + idx] = true;
                *max_clock_so_far.write().unwrap() =
                    if *max_clock_so_far.read().unwrap() < clock_val {
                        clock_val
                    } else {
                        *max_clock_so_far.read().unwrap()
                    };
            }
            Err(_) => {
                // trigger migration procedure only if there is a mismatch with _kp list
                // compute live_bc_list
                live_backends_list_bc.is_alive_list[offset + idx] = false;
            }
        }
    }

    let live_backends_list_bc = keeper.live_backends_list_bc.read().unwrap();
    let live_backends_list_kp = keeper.live_backends_list_kp.read().unwrap();
    let start = *keeper.first_bc_index.read().unwrap() as usize;
    let end = *keeper.last_bc_index.read().unwrap() as usize;
    for (idx, bc_elem) in &mut live_backends_list_bc.is_alive_list[start..end]
        .iter()
        .enumerate()
    {
        let kp_elem = live_backends_list_kp.is_alive_list[start..end][idx];
        if !(*bc_elem) && kp_elem {
            // backend went down
            tokio::spawn(sync_clocks_and_compute_live_bc(keeper.clone()))
            //keeper.clone().migrate_data(start + idx).await?
        } else if *bc_elem && !kp_elem {
            // backend came up
            keeper.clone().migrate_data_when_up(start + idx).await?
        }
    }
    // keeper.migrate_data(idx).await?; // TODO: Spawn thread for this

    *max_clock_so_far.write().unwrap() += 1u64;
    let c = *max_clock_so_far.read().unwrap();
    for storage_client in storage_clients.iter() {
        storage_client.clock(c + 1u64).await?; // TODO: Replace ? with match, continue if error
    }

    Ok(())
}

pub async fn build_and_serve_keeper_server(keeper: Keeper) -> TribResult<()> {
    tonic::transport::Server::builder()
        .add_service(KeeperSyncServer::new(keeper.clone()))
        .serve(
            keeper.addrs[keeper.index as usize]
                .parse() // TODO: change this to use to_socket_addrs
                .expect("Unable to parse socket address"),
        )
        .await?;
    Ok(())
}

pub async fn run_keeper_heartbeat(mut keeper: Keeper) -> TribResult<()> {
    let mut heartbeat_interval = interval(time::Duration::from_millis(1000)); // 1 second ticker
    let max_clock_so_far = Arc::clone(&keeper.max_clock_so_far);
    loop {
        heartbeat_interval.tick();
        for (keeper_idx, addr) in keeper.addrs.iter().enumerate() {
            if keeper_idx as u32 == keeper.index {
                continue;
            }
            if keeper.cached_conn[keeper_idx].is_none() {
                let keeper_client_res = KeeperSyncClient::connect(addr.clone()).await;
                let keeper_client = match keeper_client_res {
                    Ok(keeper_client) => keeper_client,
                    Err(_) => {
                        // trigger keeper reallocation process
                        todo!()
                    }
                };
                keeper.cached_conn[keeper_idx] = Some(keeper_client);
            }

            let heartbeat_res = keeper.cached_conn[keeper_idx]
                .clone()
                .unwrap()
                .get_heartbeat(tonic::Request::new(()))
                .await;
            let mut heartbeat = match heartbeat_res {
                Ok(heartbeat) => heartbeat,
                Err(_) => {
                    // trigger keeper reallocation process
                    todo!()
                }
            };

            *max_clock_so_far.write().unwrap() =
                if *max_clock_so_far.read().unwrap() < heartbeat.get_mut().max_clock {
                    heartbeat.get_mut().max_clock
                } else {
                    *max_clock_so_far.read().unwrap()
                };

            let live_back_list: LiveBackends =
                serde_json::from_str(&heartbeat.get_mut().live_back_list)?;
            let first_back_index = heartbeat.get_mut().first_back_index;
            let last_back_index = heartbeat.get_mut().last_back_index;

            for i in first_back_index..last_back_index + 1 {
                let live_backends_list_bc = Arc::clone(&keeper.live_backends_list_bc);
                live_backends_list_bc.write().unwrap().backs[i as usize] =
                    live_back_list.backs[i as usize].clone();
                live_backends_list_bc.write().unwrap().is_alive_list[i as usize] =
                    live_back_list.is_alive_list[i as usize];
            }
        }
        *max_clock_so_far.write().unwrap() += 1;
    }
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
pub fn new_front(bin_storage: Box<dyn BinStorage>) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontEnd {
        bin_storage: bin_storage,
        cached_users: Mutex::<Vec<String>>::new(vec![]),
    }))
}
