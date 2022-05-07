use crate::{
    keeper::{keeper_sync_client::KeeperSyncClient, keeper_sync_server::KeeperSyncServer},
    lab2::bin_store::BinStore,
};
use std::{
    cmp::min,
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hasher,
    net::ToSocketAddrs,
    sync::Arc,
};
use tokio::sync::Mutex;

use tokio::time::{self, interval};
use tribbler::{
    colon,
    config::KeeperConfig,
    err::TribResult,
    storage::{self, BinStorage, KeyList, KeyString, KeyValue, Storage},
    trib::Server,
};

use super::{
    bin_store_client::BinStoreClient,
    frontend::FrontEnd,
    keeper_impl::Keeper,
    lab3_bin_store_client::{
        LiveBackends, UpdateLog, KEY_LIVE_BACKENDS_LIST, KEY_PRIMARY_LIST, KEY_SECONDARY_LIST,
        KEY_UPDATE_LOG,
    },
    storage_client::StorageClient,
};

// use super::{frontend::FrontEnd, storage_client::StorageClient};

pub struct Allocation {
    pub start: usize,
    pub end: usize,
    pub is_alive: bool,
}

// static mut allocations: Vec<Allocation> = vec![];

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
    // initialize values and then pass copy of these values to all the keeper structs that you create.
    // so that all instantiations point to same data
    let MAX_CLOCK_SO_FAR: Arc<Mutex<u64>> = Arc::new(Mutex::new(0u64));
    let LIVE_BACKENDS_LIST_KP: Arc<Mutex<LiveBackends>> = Arc::new(Mutex::new(LiveBackends {
        backs: kc.backs.clone(),
        is_alive_list: vec![true; kc.backs.len()],
        is_migration_list: vec![false; kc.backs.len()],
    }));
    let LIVE_BACKENDS_LIST_BC: Arc<Mutex<LiveBackends>> = Arc::new(Mutex::new(LiveBackends {
        backs: kc.backs.clone(),
        is_alive_list: vec![true; kc.backs.len()],
        is_migration_list: vec![false; kc.backs.len()],
    }));
    let FIRST_BC_ID: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let LAST_BC_ID: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let BACK_ADDRS = kc.backs.clone();
    let KEEPER_ADDRS = kc.addrs.clone();
    let MY_ID = kc.this as u32;
    let CACHED_CONN = Arc::new(Mutex::new(vec![None; kc.addrs.len()]));

    let keeper = Arc::new(Keeper {
        max_clock_so_far: MAX_CLOCK_SO_FAR.clone(),
        live_backends_list_bc: LIVE_BACKENDS_LIST_BC.clone(),
        live_backends_list_kp: LIVE_BACKENDS_LIST_KP.clone(),
        first_bc_index: FIRST_BC_ID.clone(),
        last_bc_index: LAST_BC_ID.clone(),
        back_addrs: BACK_ADDRS.clone(),
        keeper_addrs: KEEPER_ADDRS.clone(),
        my_id: MY_ID,
        cached_conn: CACHED_CONN.clone(),
    });
    // STARTED RPC SERVER
    tokio::spawn(build_and_serve_keeper_server(keeper.clone()));

    // INITIAL BACKEND ALLOCATION TO KEEPER
    let bc_len = kc.backs.len();
    let kp_len = kc.addrs.len();
    let n: u32 = (bc_len / kp_len) as u32;
    let mut allocations: Vec<Allocation> = vec![];
    let mut curr_start = 0;
    for i in 0..kp_len {
        if i == kc.this {
            let clone_arc = keeper.first_bc_index.clone();
            let mut locked_val = clone_arc.lock().await;
            *locked_val = curr_start;
            drop(locked_val);

            let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
            let mut locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
            *locked_val = (curr_start + n - 1) as u32;
            if i == kp_len - 1 {
                *locked_val = (bc_len - 1) as u32;
            }
            drop(locked_val)
        }
        curr_start += n;
    }
    // 1 ROUND OF KEEPER TO BACKEND COMMUNICATION
    sync_clocks_and_compute_live_bc(keeper.clone(), true).await;
    // 1 ROUND OF KEEPER TO KEEPER COMMUNICATION
    run_keeper_heartbeat(keeper.clone(), true).await;
    // send ready signal
    if let Some(tx) = kc.ready.clone() {
        let _ = match tx.send(true) {
            Ok(v) => v,
            Err(e) => return Err(Box::new(e)),
        };
    }

    tokio::spawn(sync_clocks_and_compute_live_bc(keeper.clone(), false));
    tokio::spawn(run_keeper_heartbeat(keeper.clone(), false));

    if let Some(mut rx) = kc.shutdown {
        rx.recv().await;
    } else {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        rx.recv().await;
    }

    Ok(())
}

pub async fn sync_clocks_and_compute_live_bc(keeper: Arc<Keeper>, initial: bool) -> TribResult<()> {
    // 1 second ticker
    let mut interval = time::interval(time::Duration::from_millis(1000));

    loop {
        interval.tick().await;
        // let my_keeper = Arc::clone(&keeper);
        // helper(my_keeper, &storage_clients, initial).await?;
        let clone_arc = keeper.max_clock_so_far.clone();
        let locked_val = clone_arc.lock().await;
        let mut max_clock_so_far = *locked_val;
        let mut read_max_clock_so_far = 0u64;
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_kp.clone();
        let locked_val = clone_arc.lock().await;
        let mut live_backends_list_kp = (*locked_val).clone();
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_bc.clone();
        let locked_val = clone_arc.lock().await;
        let mut live_backends_list_bc = (*locked_val).clone();
        drop(locked_val);

        let my_id = (*keeper).my_id;
        let bc_len = (*keeper).back_addrs.len();
        let back_addrs = (*keeper).back_addrs.clone();

        let clone_arc = keeper.first_bc_index.clone();
        let locked_val = clone_arc.lock().await;
        let first_back_index = *locked_val;
        drop(locked_val);

        let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
        let locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
        let last_back_index = *locked_val;
        drop(locked_val);

        let mut storage_clients = Vec::new();
        for address in back_addrs {
            storage_clients.push(StorageClient {
                addr: format!("http://{}", address.clone()),
                cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
            });
        }

        if last_back_index < first_back_index {
            for index in first_back_index..bc_len as u32 {
                let clk = match storage_clients[index as usize]
                    .clock(max_clock_so_far)
                    .await
                {
                    Ok(v) => {
                        live_backends_list_bc.is_alive_list[index as usize] = true;
                        v
                    }
                    Err(e) => {
                        live_backends_list_bc.is_alive_list[index as usize] = false;
                        continue;
                    }
                };
                if clk > read_max_clock_so_far {
                    read_max_clock_so_far = clk;
                }
            }
            for index in 0..last_back_index + 1 as u32 {
                let clk = match storage_clients[index as usize]
                    .clock(max_clock_so_far)
                    .await
                {
                    Ok(v) => {
                        live_backends_list_bc.is_alive_list[index as usize] = true;
                        v
                    }
                    Err(e) => {
                        live_backends_list_bc.is_alive_list[index as usize] = false;
                        continue;
                    }
                };
                if clk > read_max_clock_so_far {
                    read_max_clock_so_far = clk;
                }
            }
            if read_max_clock_so_far > max_clock_so_far {
                max_clock_so_far = read_max_clock_so_far + 1;
            }
            for index in first_back_index..bc_len as u32 {
                // send live list and updated clock
                if live_backends_list_bc.is_alive_list[index as usize] {
                    // if alive send the live list and the clock
                    let _ = match storage_clients[index as usize]
                        .set(&KeyValue {
                            key: KEY_LIVE_BACKENDS_LIST.to_string(),
                            value: serde_json::to_string(&live_backends_list_bc)?,
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                    let _ = match storage_clients[index as usize]
                        .clock(max_clock_so_far)
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                }
                if initial {
                    live_backends_list_kp.is_alive_list[index as usize] =
                        live_backends_list_bc.is_alive_list[index as usize];
                } else {
                    if !live_backends_list_kp.is_alive_list[index as usize] // node has come alive
                        && live_backends_list_bc.is_alive_list[index as usize] && !live_backends_list_bc.is_migration_list[index as usize]
                    {
                        tokio::spawn(migrate_data_when_up(keeper.clone(), index as usize));
                        live_backends_list_bc.is_migration_list[index as usize] = true;
                        live_backends_list_kp.is_migration_list[index as usize] = true;
                    }
                    if live_backends_list_kp.is_alive_list[index as usize] // node has died
                        && !live_backends_list_bc.is_alive_list[index as usize] && !live_backends_list_bc.is_migration_list[index as usize]
                    {
                        tokio::spawn(migrate_data_when_down(keeper.clone(), index as usize));
                        live_backends_list_bc.is_migration_list[index as usize] = true;
                        live_backends_list_kp.is_migration_list[index as usize] = true;
                    }
                }
            }
            for index in 0..last_back_index + 1 as u32 {
                // send live list and updated clock
                if live_backends_list_bc.is_alive_list[index as usize] {
                    // if alive send the live list and the clock
                    let _ = match storage_clients[index as usize]
                        .set(&KeyValue {
                            key: KEY_LIVE_BACKENDS_LIST.to_string(),
                            value: serde_json::to_string(&live_backends_list_bc)?,
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                    let _ = match storage_clients[index as usize]
                        .clock(max_clock_so_far)
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                }
                if initial {
                    live_backends_list_kp.is_alive_list[index as usize] =
                        live_backends_list_bc.is_alive_list[index as usize];
                } else {
                    if !live_backends_list_kp.is_alive_list[index as usize] // node has come alive
                        && live_backends_list_bc.is_alive_list[index as usize] && !live_backends_list_bc.is_migration_list[index as usize]
                    {
                        tokio::spawn(migrate_data_when_up(keeper.clone(), index as usize));
                        live_backends_list_bc.is_migration_list[index as usize] = true;
                        live_backends_list_kp.is_migration_list[index as usize] = true;
                    }
                    if live_backends_list_kp.is_alive_list[index as usize] // node has died
                        && !live_backends_list_bc.is_alive_list[index as usize] && !live_backends_list_bc.is_migration_list[index as usize]
                    {
                        tokio::spawn(migrate_data_when_down(keeper.clone(), index as usize));
                        live_backends_list_bc.is_migration_list[index as usize] = true;
                        live_backends_list_kp.is_migration_list[index as usize] = true;
                    }
                }
            }
        } else {
            for index in first_back_index..last_back_index + 1 {
                let clk = match storage_clients[index as usize]
                    .clock(max_clock_so_far)
                    .await
                {
                    Ok(v) => {
                        live_backends_list_bc.is_alive_list[index as usize] = true;
                        v
                    }
                    Err(e) => {
                        live_backends_list_bc.is_alive_list[index as usize] = false;
                        continue;
                    }
                };
                if clk > read_max_clock_so_far {
                    read_max_clock_so_far = clk;
                }
            }
            if read_max_clock_so_far > max_clock_so_far {
                max_clock_so_far = read_max_clock_so_far + 1;
            }
            for index in first_back_index..last_back_index + 1 {
                // send new clock and live list
                // send live list and updated clock
                //update _kp
                if live_backends_list_bc.is_alive_list[index as usize] {
                    // if alive send the live list and the clock
                    let _ = match storage_clients[index as usize]
                        .set(&KeyValue {
                            key: KEY_LIVE_BACKENDS_LIST.to_string(),
                            value: serde_json::to_string(&live_backends_list_bc)?,
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                    let _ = match storage_clients[index as usize]
                        .clock(max_clock_so_far)
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                }
                if initial {
                    live_backends_list_kp.is_alive_list[index as usize] =
                        live_backends_list_bc.is_alive_list[index as usize];
                } else {
                    if !live_backends_list_kp.is_alive_list[index as usize] // node has come alive
                        && live_backends_list_bc.is_alive_list[index as usize] && !live_backends_list_bc.is_migration_list[index as usize]
                    {
                        tokio::spawn(migrate_data_when_up(keeper.clone(), index as usize));
                        live_backends_list_bc.is_migration_list[index as usize] = true;
                        live_backends_list_kp.is_migration_list[index as usize] = true;
                    }
                    if live_backends_list_kp.is_alive_list[index as usize] // node has died
                        && !live_backends_list_bc.is_alive_list[index as usize] && !live_backends_list_bc.is_migration_list[index as usize]
                    {
                        tokio::spawn(migrate_data_when_down(keeper.clone(), index as usize));
                        live_backends_list_bc.is_migration_list[index as usize] = true;
                        live_backends_list_kp.is_migration_list[index as usize] = true;
                    }
                }
            }
        }
        let clone_arc = keeper.max_clock_so_far.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = max_clock_so_far;
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_kp.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = live_backends_list_kp.clone();
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_bc.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = live_backends_list_bc.clone();
        drop(locked_val);

        let clone_arc = keeper.first_bc_index.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = first_back_index;
        drop(locked_val);

        let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
        let mut locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
        *locked_val = last_back_index;
        drop(locked_val);
        if initial {
            break;
        }
    }
    Ok(())
}

pub async fn build_and_serve_keeper_server(keeper: Arc<Keeper>) -> TribResult<()> {
    let mut addr_iter = match (*keeper).keeper_addrs[(*keeper).my_id as usize].to_socket_addrs() {
        Ok(v) => v,
        Err(e) => {
            return Err(Box::new(e));
        }
    };
    let addr_str = match addr_iter.next() {
        Some(v) => v,
        None => {
            return Err("Empty Addr".into());
        }
    };
    let k = Keeper {
        max_clock_so_far: (*keeper).max_clock_so_far.clone(),
        live_backends_list_bc: (*keeper).live_backends_list_bc.clone(),
        live_backends_list_kp: (*keeper).live_backends_list_kp.clone(),
        first_bc_index: (*keeper).first_bc_index.clone(),
        last_bc_index: (*keeper).last_bc_index.clone(),
        back_addrs: (*keeper).back_addrs.clone(),
        keeper_addrs: (*keeper).keeper_addrs.clone(),
        my_id: (*keeper).my_id,
        cached_conn: (*keeper).cached_conn.clone(),
    };
    tonic::transport::Server::builder()
        .add_service(KeeperSyncServer::new(k))
        .serve(addr_str)
        .await?;
    Ok(())
}

pub async fn run_keeper_heartbeat(keeper: Arc<Keeper>, initial: bool) -> TribResult<()> {
    // INITIAL BACKEND ALLOCATION TO KEEPER
    let bc_len = (*keeper).back_addrs.len();
    let kp_len = (*keeper).keeper_addrs.len();
    let n = bc_len / kp_len;
    let mut allocations: Vec<Allocation> = vec![];
    let mut curr_start = 0;
    for i in 0..kp_len {
        let mut a = curr_start + n - 1;
        if i == kp_len - 1 {
            a = bc_len - 1;
        }
        allocations.push(Allocation {
            start: curr_start, // here no need to check wraparound because we will stop at last index
            end: a,
            is_alive: true,
        });
        curr_start += n;
    }

    let mut heartbeat_interval = interval(time::Duration::from_millis(1000)); // 1 second ticker

    loop {
        heartbeat_interval.tick().await;

        let clone_arc = keeper.max_clock_so_far.clone();
        let locked_val = clone_arc.lock().await;
        let mut max_clock_so_far = *locked_val;
        let mut read_max_clock_so_far = 0u64;
        drop(locked_val);

        let clone_arc = keeper.cached_conn.clone();
        let locked_val = clone_arc.lock().await;
        let mut cached_conn = (*locked_val).clone();
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_kp.clone();
        let locked_val = clone_arc.lock().await;
        let mut live_backends_list_kp = (*locked_val).clone();
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_bc.clone();
        let locked_val = clone_arc.lock().await;
        let mut live_backends_list_bc = (*locked_val).clone();
        drop(locked_val);

        let keeper_addrs = (*keeper).keeper_addrs.clone();
        let my_id = (*keeper).my_id;
        let bc_len = (*keeper).back_addrs.len();
        let back_addrs = (*keeper).back_addrs.clone();

        let clone_arc = keeper.first_bc_index.clone();
        let locked_val = clone_arc.lock().await;
        let mut my_first_back_index = (*locked_val).clone();
        drop(locked_val);

        let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
        let locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
        let mut my_last_back_index = (*locked_val).clone();
        drop(locked_val);

        for (keeper_idx, addr) in keeper_addrs.iter().enumerate() {
            if keeper_idx as u32 == my_id {
                continue;
            }
            if cached_conn[keeper_idx].is_none() {
                cached_conn[keeper_idx] =
                    match KeeperSyncClient::connect(format!("http://{}", addr.clone())).await {
                        Ok(keeper_client) => {
                            allocations[keeper_idx].is_alive = true;
                            Some(keeper_client)
                        }
                        Err(_) => {
                            allocations[keeper_idx].is_alive = false;
                            None
                        }
                    };
            }

            if !cached_conn[keeper_idx].is_none() {
                if let Ok(mut heartbeat) = cached_conn[keeper_idx]
                    .as_mut()
                    .unwrap()
                    .get_heartbeat(tonic::Request::new(()))
                    .await
                {
                    allocations[keeper_idx].is_alive = true;
                    if read_max_clock_so_far < heartbeat.get_mut().max_clock {
                        read_max_clock_so_far = heartbeat.get_mut().max_clock;
                    }

                    let remote_live_back_list: LiveBackends =
                        serde_json::from_str(&heartbeat.get_mut().live_back_list)?;

                    let remote_first_back_index = heartbeat.get_mut().first_back_index;
                    let remote_last_back_index = heartbeat.get_mut().last_back_index;
                    if remote_last_back_index < remote_first_back_index {
                        for index in remote_first_back_index..bc_len as u32 {
                            live_backends_list_bc.is_alive_list[index as usize] =
                                remote_live_back_list.is_alive_list[index as usize];
                            live_backends_list_bc.is_migration_list[index as usize] =
                                remote_live_back_list.is_migration_list[index as usize];
                            live_backends_list_bc.backs[index as usize] =
                                remote_live_back_list.backs[index as usize].clone();

                            live_backends_list_kp.is_alive_list[index as usize] =
                                remote_live_back_list.is_alive_list[index as usize];
                            live_backends_list_kp.is_migration_list[index as usize] =
                                remote_live_back_list.is_migration_list[index as usize];
                            live_backends_list_kp.backs[index as usize] =
                                remote_live_back_list.backs[index as usize].clone();
                        }
                        for index in 0..remote_last_back_index + 1 as u32 {
                            live_backends_list_bc.is_alive_list[index as usize] =
                                remote_live_back_list.is_alive_list[index as usize];
                            live_backends_list_bc.is_migration_list[index as usize] =
                                remote_live_back_list.is_migration_list[index as usize];
                            live_backends_list_bc.backs[index as usize] =
                                remote_live_back_list.backs[index as usize].clone();

                            live_backends_list_kp.is_alive_list[index as usize] =
                                remote_live_back_list.is_alive_list[index as usize];
                            live_backends_list_kp.is_migration_list[index as usize] =
                                remote_live_back_list.is_migration_list[index as usize];
                            live_backends_list_kp.backs[index as usize] =
                                remote_live_back_list.backs[index as usize].clone();
                        }
                    } else {
                        for index in remote_first_back_index..remote_last_back_index + 1 as u32 {
                            live_backends_list_bc.is_alive_list[index as usize] =
                                remote_live_back_list.is_alive_list[index as usize];
                            live_backends_list_bc.is_migration_list[index as usize] =
                                remote_live_back_list.is_migration_list[index as usize];
                            live_backends_list_bc.backs[index as usize] =
                                remote_live_back_list.backs[index as usize].clone();

                            live_backends_list_kp.is_alive_list[index as usize] =
                                remote_live_back_list.is_alive_list[index as usize];
                            live_backends_list_kp.is_migration_list[index as usize] =
                                remote_live_back_list.is_migration_list[index as usize];
                            live_backends_list_kp.backs[index as usize] =
                                remote_live_back_list.backs[index as usize].clone();
                        }
                    }
                } else {
                    allocations[keeper_idx].is_alive = false;
                }
            } else {
                allocations[keeper_idx].is_alive = false;
            }
        }
        if read_max_clock_so_far > max_clock_so_far {
            max_clock_so_far = read_max_clock_so_far + 1;
        }

        let kp_len = keeper_addrs.len();
        for j in 1..kp_len {
            if !allocations[(my_id as usize + j) % kp_len].is_alive {
                my_last_back_index = allocations[(my_id as usize + j) % kp_len].end as u32;
            } else {
                my_last_back_index = ((allocations[(my_id as usize + j) % kp_len].start + bc_len
                    - 1)
                    % bc_len) as u32;
                break;
            }
        }
        let clone_arc = keeper.max_clock_so_far.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = max_clock_so_far;
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_kp.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = live_backends_list_kp.clone();
        drop(locked_val);

        let clone_arc = keeper.live_backends_list_bc.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = live_backends_list_bc.clone();
        drop(locked_val);

        let clone_arc = keeper.first_bc_index.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = my_first_back_index;
        drop(locked_val);

        let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
        let mut locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
        *locked_val = my_last_back_index;
        drop(locked_val);

        let clone_arc = keeper.cached_conn.clone();
        let mut locked_val = clone_arc.lock().await;
        *locked_val = cached_conn;
        drop(locked_val);

        if initial {
            break;
        }
    }
    Ok(())
}

pub async fn migrate_data_when_down(
    keeper: Arc<Keeper>,
    storage_client_idx: usize,
) -> TribResult<()> {
    // Backend failure
    let clone_arc = keeper.live_backends_list_kp.clone();
    let locked_val = clone_arc.lock().await;
    let mut live_backends_list_kp = (*locked_val).clone();
    drop(locked_val);

    let clone_arc = keeper.live_backends_list_bc.clone();
    let locked_val = clone_arc.lock().await;
    let mut live_backends_list_bc = (*locked_val).clone();
    drop(locked_val);

    let keeper_addrs = (*keeper).keeper_addrs.clone();
    let my_id = (*keeper).my_id;
    let bc_len = (*keeper).back_addrs.len();
    let back_addrs = (*keeper).back_addrs.clone();

    let clone_arc = keeper.first_bc_index.clone();
    let locked_val = clone_arc.lock().await;
    let mut my_first_back_index = (*locked_val).clone();
    drop(locked_val);

    let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
    let locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
    let mut my_last_back_index = (*locked_val).clone();
    drop(locked_val);

    let mut i_plus_1_id = 0;
    for i in 1..bc_len {
        if live_backends_list_bc.is_alive_list[(storage_client_idx + i) % bc_len] {
            i_plus_1_id = (storage_client_idx + i) % bc_len;
            break;
        }
    }
    let sc_i_plus_1 = StorageClient {
        addr: format!("http://{}", back_addrs[i_plus_1_id as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let mut i_minus_1_id = 0;
    for i in 1..bc_len {
        if live_backends_list_bc.is_alive_list[(storage_client_idx - i + bc_len) % bc_len] {
            i_minus_1_id = (storage_client_idx - i + bc_len) % bc_len;
            break;
        }
    }
    let sc_i_minus_1 = StorageClient {
        addr: format!("http://{}", back_addrs[i_minus_1_id as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let mut i_plus_2_id = 0;
    let mut one_found = false;
    for i in 1..bc_len {
        if live_backends_list_bc.is_alive_list[(storage_client_idx + i) % bc_len] && one_found {
            i_plus_2_id = (storage_client_idx + i) % bc_len;
            break;
        } else if live_backends_list_bc.is_alive_list[(storage_client_idx + i) % bc_len] {
            one_found = true;
        }
    }
    let sc_i_plus_2 = StorageClient {
        addr: format!("http://{}", back_addrs[i_plus_2_id as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let sc_i_plus_1_secondary_list = sc_i_plus_1.list_get(KEY_SECONDARY_LIST).await?;
    let mut bin_name_hashset = HashSet::new();

    for bin_name in sc_i_plus_1_secondary_list.0 {
        if bin_name_hashset.contains(&bin_name) {
            continue;
        } else {
            bin_name_hashset.insert(bin_name.clone());
        }
        let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let bin_store_client_i_plus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i_plus_1.clone(),
        };

        let bin_store_client_i_plus_2 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i_plus_2.clone(),
        };

        let storage::List(fetched_log) = bin_store_client_i_plus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        // regenerate data from log and serve query
        let mut deserialized_log: Vec<UpdateLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<UpdateLog>>();

        // sort the deserialized log as per timestamp
        // keep the fields in the required order so that they are sorted in that same order.
        deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
        deserialized_log = remove_duplicates_from_log(deserialized_log);
        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        //let mut max_seen_seq_num = 0u64;

        //let mut reserialized_log = vec![];
        for log in deserialized_log {
            let serialized_log_elem = serde_json::to_string(&log)?;

            let append_kv = storage::KeyValue {
                key: KEY_UPDATE_LOG.to_string(),
                value: serialized_log_elem,
            };

            bin_store_client_i_plus_2.list_append(&append_kv).await?;
        }

        // append binname to secondary list
        let bin_name_append_kv = storage::KeyValue {
            key: KEY_SECONDARY_LIST.to_string().clone(),
            value: bin_name.clone(),
        };
        sc_i_plus_2.list_append(&bin_name_append_kv).await?;

        // move the keys from secondary list of sc_i+1 to primary list to make it a primary for those keys
        let primary_move_kv = storage::KeyValue {
            key: KEY_PRIMARY_LIST.to_string().clone(),
            value: bin_name.clone(),
        };
        sc_i_plus_1.list_append(&primary_move_kv).await?;
        sc_i_plus_1.list_remove(&bin_name_append_kv).await?;

        // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    }

    // create a second copy of primary data in i-1 in i+1
    let sc_i_minus_1_primary_list = sc_i_minus_1.list_get(KEY_PRIMARY_LIST).await?;
    let mut bin_name_hashset = HashSet::new();

    for bin_name in sc_i_minus_1_primary_list.0 {
        if bin_name_hashset.contains(&bin_name) {
            continue;
        } else {
            bin_name_hashset.insert(bin_name.clone());
        }

        let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let bin_store_client_i_minus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i_minus_1.clone(),
        };

        let bin_store_client_i_plus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i_plus_1.clone(),
        };

        let storage::List(fetched_log) =
            bin_store_client_i_minus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        // regenerate data from log and serve query
        let mut deserialized_log: Vec<UpdateLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<UpdateLog>>();

        // sort the deserialized log as per timestamp
        // keep the fields in the required order so that they are sorted in that same order.
        deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
        deserialized_log = remove_duplicates_from_log(deserialized_log);
        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        //let mut max_seen_seq_num = 0u64;

        //let mut reserialized_log = vec![];
        for log in deserialized_log {
            let serialized_log_elem = serde_json::to_string(&log)?;

            let append_kv = storage::KeyValue {
                key: KEY_UPDATE_LOG.to_string(),
                value: serialized_log_elem,
            };

            bin_store_client_i_plus_1.list_append(&append_kv).await?;
        }

        // append binname to secondary list
        let bin_name_append_kv = storage::KeyValue {
            key: KEY_SECONDARY_LIST.to_string().clone(),
            value: bin_name.clone(),
        };
        sc_i_plus_1.list_append(&bin_name_append_kv).await?;

        // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    }
    live_backends_list_kp.is_alive_list[storage_client_idx as usize] = false;
    live_backends_list_kp.is_migration_list[storage_client_idx as usize] = false;

    let clone_arc = keeper.live_backends_list_kp.clone();
    let mut locked_val = clone_arc.lock().await;
    *locked_val = live_backends_list_kp.clone();
    drop(locked_val);
    Ok(())
}

pub async fn migrate_data_when_up(
    keeper: Arc<Keeper>,
    storage_client_idx: usize,
) -> TribResult<()> {
    let clone_arc = keeper.live_backends_list_kp.clone();
    let locked_val = clone_arc.lock().await;
    let mut live_backends_list_kp = (*locked_val).clone();
    drop(locked_val);

    let clone_arc = keeper.live_backends_list_bc.clone();
    let locked_val = clone_arc.lock().await;
    let mut live_backends_list_bc = (*locked_val).clone();
    drop(locked_val);

    let keeper_addrs = (*keeper).keeper_addrs.clone();
    let my_id = (*keeper).my_id;
    let bc_len = (*keeper).back_addrs.len();
    let back_addrs = (*keeper).back_addrs.clone();

    let clone_arc = keeper.first_bc_index.clone();
    let locked_val = clone_arc.lock().await;
    let mut my_first_back_index = (*locked_val).clone();
    drop(locked_val);

    let clone_arc = keeper.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
    let locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
    let mut my_last_back_index = (*locked_val).clone();
    drop(locked_val);

    let mut i_plus_1_id = 0;
    for i in 1..bc_len {
        if live_backends_list_bc.is_alive_list[(storage_client_idx + i) % bc_len] {
            i_plus_1_id = (storage_client_idx + i) % bc_len;
            break;
        }
    }
    let sc_i_plus_1 = StorageClient {
        addr: format!("http://{}", back_addrs[i_plus_1_id as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let mut i_minus_1_id = 0;
    for i in 1..bc_len {
        if live_backends_list_bc.is_alive_list[(storage_client_idx - i + bc_len) % bc_len] {
            i_minus_1_id = (storage_client_idx - i + bc_len) % bc_len;
            break;
        }
    }
    let sc_i_minus_1 = StorageClient {
        addr: format!("http://{}", back_addrs[i_minus_1_id as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let mut i_plus_2_id = 0;
    let mut one_found = false;
    for i in 1..bc_len {
        if live_backends_list_bc.is_alive_list[(storage_client_idx + i) % bc_len] && one_found {
            i_plus_2_id = (storage_client_idx + i) % bc_len;
            break;
        } else if live_backends_list_bc.is_alive_list[(storage_client_idx + i) % bc_len] {
            one_found = true;
        }
    }
    let sc_i_plus_2 = StorageClient {
        addr: format!("http://{}", back_addrs[i_plus_2_id as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let sc_i = StorageClient {
        addr: format!("http://{}", back_addrs[storage_client_idx as usize]),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let sc_i_minus_1_primary_list = sc_i_minus_1.list_get(KEY_PRIMARY_LIST).await?;
    let mut bin_name_hashset = HashSet::new();

    for bin_name in sc_i_minus_1_primary_list.0 {
        if bin_name_hashset.contains(&bin_name) {
            continue;
        } else {
            bin_name_hashset.insert(bin_name.clone());
        }

        let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let bin_store_client_i_minus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i_minus_1.clone(),
        };

        let bin_store_client_i = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i.clone(),
        };

        let storage::List(fetched_log) =
            bin_store_client_i_minus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        // regenerate data from log and serve query
        let mut deserialized_log: Vec<UpdateLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<UpdateLog>>();

        // sort the deserialized log as per timestamp
        // keep the fields in the required order so that they are sorted in that same order.
        deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
        deserialized_log = remove_duplicates_from_log(deserialized_log);
        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        //let mut max_seen_seq_num = 0u64;

        //let mut reserialized_log = vec![];
        for log in deserialized_log {
            let serialized_log_elem = serde_json::to_string(&log)?;

            let append_kv = storage::KeyValue {
                key: KEY_UPDATE_LOG.to_string(),
                value: serialized_log_elem,
            };

            bin_store_client_i.list_append(&append_kv).await?;
        }

        // append binname to secondary list
        let bin_name_append_kv = storage::KeyValue {
            key: KEY_SECONDARY_LIST.to_string().clone(),
            value: bin_name.clone(),
        };
        sc_i.list_append(&bin_name_append_kv).await?;

        sc_i_plus_1.list_remove(&bin_name_append_kv).await?;

        // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    }

    // Recovery step 1
    let sc_i_plus_1_primary_list = sc_i_plus_1.list_get(KEY_PRIMARY_LIST).await?;
    let mut bin_name_hashset = HashSet::new();

    let n = bc_len as u64;
    let mut hasher = DefaultHasher::new();

    for bin_name in sc_i_plus_1_primary_list.0 {
        if bin_name_hashset.contains(&bin_name) {
            continue;
        } else {
            bin_name_hashset.insert(bin_name.clone());
        }

        hasher.write(bin_name.clone().as_bytes());

        let hash = hasher.finish();
        if (hash % n) as u32 > storage_client_idx as u32 {
            continue;
        }

        let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let bin_store_client_i_plus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i_plus_1.clone(),
        };

        let bin_store_client_i = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![],
            bin_client: sc_i.clone(),
        };

        let storage::List(fetched_log) = bin_store_client_i_plus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        // regenerate data from log and serve query
        let mut deserialized_log: Vec<UpdateLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<UpdateLog>>();

        // sort the deserialized log as per timestamp
        // keep the fields in the required order so that they are sorted in that same order.
        deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
        deserialized_log = remove_duplicates_from_log(deserialized_log);
        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        //let mut max_seen_seq_num = 0u64;

        //let mut reserialized_log = vec![];
        for log in deserialized_log {
            let serialized_log_elem = serde_json::to_string(&log)?;

            let append_kv = storage::KeyValue {
                key: KEY_UPDATE_LOG.to_string(),
                value: serialized_log_elem,
            };

            bin_store_client_i.list_append(&append_kv).await?;
        }

        // append binname to secondary list
        let bin_name_append_kv = storage::KeyValue {
            key: KEY_PRIMARY_LIST.to_string().clone(),
            value: bin_name.clone(),
        };
        sc_i.list_append(&bin_name_append_kv).await?;

        let bin_name_move_primary_to_secondary = storage::KeyValue {
            key: KEY_SECONDARY_LIST.to_string().clone(),
            value: bin_name.clone(),
        };
        sc_i_plus_1
            .list_append(&bin_name_move_primary_to_secondary)
            .await?;
        sc_i_plus_1.list_remove(&bin_name_append_kv).await?;

        sc_i_plus_2
            .list_remove(&bin_name_move_primary_to_secondary)
            .await?;

        // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    }

    live_backends_list_kp.is_alive_list[storage_client_idx as usize] = true;
    live_backends_list_kp.is_migration_list[storage_client_idx as usize] = false;

    let clone_arc = keeper.live_backends_list_kp.clone();
    let mut locked_val = clone_arc.lock().await;
    *locked_val = live_backends_list_kp.clone();
    drop(locked_val);
    Ok(())
}

fn remove_duplicates_from_log(log: Vec<UpdateLog>) -> Vec<UpdateLog> {
    let max_seen_seq_num = 0;
    let mut ret_vec = vec![];
    for item in log {
        if item.seq_num > max_seen_seq_num {
            ret_vec.push(item);
        }
    }
    return ret_vec;
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
// #[allow(unused_variables)]
// pub fn new_front(bin_storage: Box<dyn BinStorage>) -> TribResult<Box<dyn Server + Send + Sync>> {
//     Ok(Box::new(FrontEnd {
//         bin_storage: bin_storage,
//         cached_users: std::sync::Mutex::<Vec<String>>::new(vec![]),
//     }))
// }
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontEnd {
        bin_storage: bin_storage,
        cached_users: std::sync::Mutex::<Vec<String>>::new(vec![]),
    }))
}
