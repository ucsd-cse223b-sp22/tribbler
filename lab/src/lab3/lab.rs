// TODO
// 1. allocation updation in run_keeper_heartbeat - done i think
// 2. take care of wrap around cases
// 3. Update _kp list after migration - done i think
// 4. send live list to backends

use crate::{
    keeper::{keeper_sync_client::KeeperSyncClient, keeper_sync_server::KeeperSyncServer},
    lab2::bin_store::BinStore,
};
use std::{
    cmp::min,
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hasher,
    mem,
    sync::{Arc, Mutex, RwLock},
};
use tokio::time::{self, interval};
use tribbler::{
    colon,
    config::KeeperConfig,
    err::TribResult,
    storage::{self, BinStorage, KeyList, Storage},
    trib::Server,
};

use super::{
    bin_store_client::BinStoreClient,
    frontend::FrontEnd,
    keeper_impl::Keeper,
    lab3_bin_store_client::{
        LiveBackends, UpdateLog, KEY_PRIMARY_LIST, KEY_SECONDARY_LIST, KEY_UPDATE_LOG,
    },
    storage_client::StorageClient,
};

// use super::{frontend::FrontEnd, storage_client::StorageClient};

pub struct Allocation {
    pub start: i32,
    pub end: i32,
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

    // initial backend allocation to keeper code start: NEED TO CHECK THIS
    let bc_len = kc.backs.len();
    let kp_len = kc.addrs.len();

    let n = bc_len / kp_len;

    let mut allocations: Vec<Allocation> = vec![];
    let mut curr_start = 0;
    for i in 0..kp_len {
        if curr_start >= bc_len {
            allocations.push(Allocation {
                start: -1,
                end: (bc_len - 1) as i32,
                is_alive: false,
            });
            continue;
        } else {
            allocations.push(Allocation {
                start: curr_start as i32,
                end: min((curr_start + n - 1) as i32, (bc_len - 1) as i32),
                is_alive: true,
            });
            curr_start += n;
        }
    }
    // initial backend allocation to keeper code end
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
    handles.push(tokio::spawn(run_keeper_heartbeat(
        keeper.clone(),
        allocations,
    )));

    handles.push(tokio::spawn(sync_clocks_and_compute_live_bc(keeper)));

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

async fn helper(keeper: Keeper, storage_clients: &Vec<StorageClient>) -> TribResult<()> {
    let max_clock_so_far = Arc::clone(&keeper.max_clock_so_far);
    let c = *max_clock_so_far.read().unwrap();

    let my_live_backends_list_bc = Arc::clone(&keeper.live_backends_list_bc);
    let my_live_backends_list_kp = Arc::clone(&keeper.live_backends_list_kp);

    let mut live_backends_list_bc = (*my_live_backends_list_bc.write().unwrap()).clone();

    let start = *keeper.first_bc_index.read().unwrap() as usize;
    let end = *keeper.last_bc_index.read().unwrap() as usize;

    for (idx, storage_client) in storage_clients.iter().enumerate() {
        live_backends_list_bc.backs[start + idx] = storage_client.addr.clone(); // take care of wrap around

        let storage_client_clock = storage_client.clock(c + 1u64).await;
        match storage_client_clock {
            Ok(clock_val) => {
                live_backends_list_bc.is_alive_list[start + idx] = true;
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
                live_backends_list_bc.is_alive_list[start + idx] = false;
            }
        }
    }

    let live_backends_list_bc_read = (*my_live_backends_list_bc.read().unwrap()).clone();
    let live_backends_list_kp_read = (*my_live_backends_list_kp.read().unwrap()).clone();

    for (idx, bc_elem) in &mut live_backends_list_bc_read.is_alive_list[start..end]
        .iter()
        .enumerate()
    {
        let kp_elem = live_backends_list_kp_read.is_alive_list[start..end][idx]; // take care of wrap around
        if !(*bc_elem) && kp_elem {
            // backend went down
            tokio::spawn(migrate_data(keeper.clone(), start + idx));
        } else if *bc_elem && !kp_elem {
            // backend came up
            tokio::spawn(migrate_data_when_up(keeper.clone(), start + idx));
        }
    }

    *max_clock_so_far.write().unwrap() += 1u64;
    let c = *max_clock_so_far.read().unwrap();
    for storage_client in storage_clients.iter() {
        let clock_res = storage_client.clock(c + 1u64).await;
        match clock_res {
            Ok(_) => {}
            Err(_) => {
                continue;
            }
        };
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

pub async fn run_keeper_heartbeat(
    mut keeper: Keeper,
    mut allocations: Vec<Allocation>,
) -> TribResult<()> {
    let mut heartbeat_interval = interval(time::Duration::from_millis(1000)); // 1 second ticker
    let max_clock_so_far = Arc::clone(&keeper.max_clock_so_far);
    loop {
        heartbeat_interval.tick().await;
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

            // let heartbeat_res = keeper.cached_conn[keeper_idx]
            //     .clone()
            //     .unwrap()
            //     .get_heartbeat(tonic::Request::new(()))
            //     .await;

            if let Ok(mut heartbeat) = keeper.cached_conn[keeper_idx]
                .clone()
                .unwrap()
                .get_heartbeat(tonic::Request::new(()))
                .await
            {
                allocations[keeper_idx].is_alive = true;

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
            } else {
                allocations[keeper_idx].is_alive = false;
            }

            // TODO: Update keeper to backend allocations
            let kp_len = keeper.addrs.len();
            let my_end_index = Arc::clone(&keeper.last_bc_index);
            let mut end_index = my_end_index.write().unwrap();
            for j in 1..kp_len {
                if !allocations[(keeper.index as usize + j) % kp_len].is_alive {
                    *end_index = allocations[(keeper.index as usize + j) % kp_len].end as u32;
                } else {
                    *end_index =
                        (allocations[(keeper.index as usize + j) % kp_len].start - 1) as u32;
                }
            }

            // let mut heartbeat = match heartbeat_res {
            //     Ok(heartbeat) => {
            //         allocations[keeper_idx].is_alive = true;
            //         heartbeat
            //     }
            //     Err(_) => {
            //         // trigger keeper reallocation process; this will be handled by the
            //         todo!()
            //     }
            // };

            // *max_clock_so_far.write().unwrap() =
            //     if *max_clock_so_far.read().unwrap() < heartbeat.get_mut().max_clock {
            //         heartbeat.get_mut().max_clock
            //     } else {
            //         *max_clock_so_far.read().unwrap()
            //     };

            // let live_back_list: LiveBackends =
            //     serde_json::from_str(&heartbeat.get_mut().live_back_list)?;
            // let first_back_index = heartbeat.get_mut().first_back_index;
            // let last_back_index = heartbeat.get_mut().last_back_index;

            // for i in first_back_index..last_back_index + 1 {
            //     let live_backends_list_bc = Arc::clone(&keeper.live_backends_list_bc);
            //     live_backends_list_bc.write().unwrap().backs[i as usize] =
            //         live_back_list.backs[i as usize].clone();
            //     live_backends_list_bc.write().unwrap().is_alive_list[i as usize] =
            //         live_back_list.is_alive_list[i as usize];
            // }
        }
        *max_clock_so_far.write().unwrap() += 1;
    }
}

pub async fn migrate_data(keeper: Keeper, storage_client_idx: usize) -> TribResult<()> {
    // Backend failure
    let target_storage_client = *keeper.first_bc_index.read().unwrap() + storage_client_idx as u32;
    let sc_i_plus_1 = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client + 1) % keeper.backs.len() as u32) as usize]
        ),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };
    // let bin_store_client_i_plus_1 = BinStoreClient {
    //     name: String,
    //     colon_escaped_name: String,
    //     clients: Vec<StorageClient>,
    //     bin_client: sc_i_plus_1,
    // };

    let sc_i_minus_1 = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client - 1) % keeper.backs.len() as u32) as usize]
        ),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };
    // let bin_store_client_i_minus_1 = BinStoreClient {
    //     name: String,
    //     colon_escaped_name: String,
    //     clients: Vec<StorageClient>,
    //     bin_client: sc_i_minus_1,
    // };

    let sc_i_plus_2 = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client + 2) % keeper.backs.len() as u32) as usize]
        ),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };
    // let bin_store_client_i_plus_2 = BinStoreClient {
    //     name: String,
    //     colon_escaped_name: String,
    //     clients: Vec<StorageClient>,
    //     bin_client: sc_i_plus_2,
    // };

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
            clients: vec![], // TODO: Check later
            bin_client: sc_i_plus_1.clone(),
        };

        let bin_store_client_i_plus_2 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![], // TODO: Check later
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
                key: bin_name.clone(),
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
            clients: vec![], // TODO: Check later
            bin_client: sc_i_minus_1.clone(),
        };

        let bin_store_client_i_plus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![], // TODO: Check later
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
                key: bin_name.clone(),
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

    let mut live_backends_list_kp = keeper.live_backends_list_kp.write().unwrap();
    live_backends_list_kp.is_alive_list[target_storage_client as usize] = false;

    Ok(())
}

pub async fn migrate_data_when_up(keeper: Keeper, up_storage_client_idx: usize) -> TribResult<()> {
    let target_storage_client =
        *keeper.first_bc_index.read().unwrap() + up_storage_client_idx as u32;
    let sc_i_plus_1 = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client + 1) % keeper.backs.len() as u32) as usize]
        ),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let sc_i = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client) % keeper.backs.len() as u32) as usize]
        ),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let sc_i_minus_1 = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client - 1) % keeper.backs.len() as u32) as usize]
        ),
        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    };

    let sc_i_plus_2 = StorageClient {
        addr: format!(
            "http://{}",
            keeper.backs[((target_storage_client + 2) % keeper.backs.len() as u32) as usize]
        ),
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
            clients: vec![], // TODO: Check later
            bin_client: sc_i_minus_1.clone(),
        };

        let bin_store_client_i = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![], // TODO: Check later
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
                key: bin_name.clone(),
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

    let n = keeper.backs.len() as u64;
    let mut hasher = DefaultHasher::new();

    for bin_name in sc_i_plus_1_primary_list.0 {
        if bin_name_hashset.contains(&bin_name) {
            continue;
        } else {
            bin_name_hashset.insert(bin_name.clone());
        }

        hasher.write(bin_name.clone().as_bytes());

        let hash = hasher.finish();
        if (hash % n) as u32 > target_storage_client {
            continue;
        }

        let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let bin_store_client_i_plus_1 = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![], // TODO: Check later
            bin_client: sc_i_plus_1.clone(),
        };

        let bin_store_client_i = BinStoreClient {
            name: bin_name.clone(),
            colon_escaped_name: colon_escaped_name.clone(),
            clients: vec![], // TODO: Check later
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
                key: bin_name.clone(),
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

    let mut live_backends_list_kp = keeper.live_backends_list_kp.write().unwrap();
    live_backends_list_kp.is_alive_list[target_storage_client as usize] = true;

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
#[allow(unused_variables)]
pub fn new_front(bin_storage: Box<dyn BinStorage>) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontEnd {
        bin_storage: bin_storage,
        cached_users: Mutex::<Vec<String>>::new(vec![]),
    }))
}
