use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hasher,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use tokio::time;
use tonic::transport::Channel;
use tribbler::{
    colon,
    err::TribResult,
    storage::{self, KeyList, Storage},
};

use crate::keeper::{
    keeper_sync_client::KeeperSyncClient, keeper_sync_server::KeeperSync, Heartbeat,
};

use super::{
    bin_store_client::BinStoreClient,
    lab3_bin_store_client::{LiveBackends, UpdateLog},
    storage_client::StorageClient,
};
#[derive(Clone)]
pub struct Keeper {
    pub max_clock_so_far: Arc<RwLock<u64>>,
    pub live_backends_list_bc: Arc<RwLock<LiveBackends>>,
    pub live_backends_list_kp: Arc<RwLock<LiveBackends>>,
    pub first_bc_index: Arc<RwLock<u32>>,
    pub last_bc_index: Arc<RwLock<u32>>,
    pub backs: Vec<String>,
    pub addrs: Vec<String>,
    pub index: u32,
    pub cached_conn: Vec<Option<KeeperSyncClient<Channel>>>,
}

impl Keeper {
    // pub async fn sync_clocks_and_compute_live_bc(&mut self) -> TribResult<()> {
    //     *self.max_clock_so_far.write().unwrap() = 0;
    //     let mut storage_clients = Vec::new();

    //     for address in self.backs[*self.first_bc_index.read().unwrap() as usize
    //         ..(*self.last_bc_index.read().unwrap() + 1) as usize]
    //         .iter()
    //     {
    //         storage_clients.push(StorageClient {
    //             addr: format!("http://{}", address.clone()),
    //             cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //         });
    //     }

    //     // 1 second ticker
    //     let mut interval = time::interval(time::Duration::from_millis(1000));

    //     loop {
    //         interval.tick().await;
    //         self.helper(&storage_clients).await?;
    //     }
    // }

    // async fn helper(&mut self, storage_clients: &Vec<StorageClient>) -> TribResult<()> {
    //     // TODO: make this asynchronous instead of serial
    //     for (idx, storage_client) in storage_clients.iter().enumerate() {
    //         let storage_client_clock = storage_client
    //             .clock(*self.max_clock_so_far.write().unwrap() + 1u64)
    //             .await;
    //         match storage_client_clock {
    //             Ok(clock_val) => {
    //                 let mut max_clock_so_far = self.max_clock_so_far.write().unwrap();
    //                 *max_clock_so_far = if *max_clock_so_far < clock_val {
    //                     clock_val
    //                 } else {
    //                     *max_clock_so_far
    //                 };
    //             }
    //             Err(_) => {
    //                 // trigger migration procedure
    //                 self.migrate_data(idx).await?; // TODO: Spawn thread for this
    //             }
    //         }
    //     }

    //     *self.max_clock_so_far.write().unwrap() += 1u64;

    //     for storage_client in storage_clients.iter() {
    //         let clock_res = storage_client
    //             .clock(*self.max_clock_so_far.write().unwrap() + 1u64)
    //             .await;
    //         match clock_res {
    //             Ok(_) => {}
    //             Err(_) => {
    //                 continue;
    //             }
    //         };
    //     }

    //     Ok(())
    // }

    // pub async fn migrate_data(&mut self, storage_client_idx: usize) -> TribResult<()> {
    //     // Backend failure
    //     let target_storage_client =
    //         *self.first_bc_index.read().unwrap() + storage_client_idx as u32;
    //     let sc_i_plus_1 = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client + 1) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };
    //     // let bin_store_client_i_plus_1 = BinStoreClient {
    //     //     name: String,
    //     //     colon_escaped_name: String,
    //     //     clients: Vec<StorageClient>,
    //     //     bin_client: sc_i_plus_1,
    //     // };

    //     let sc_i_minus_1 = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client - 1) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };
    //     // let bin_store_client_i_minus_1 = BinStoreClient {
    //     //     name: String,
    //     //     colon_escaped_name: String,
    //     //     clients: Vec<StorageClient>,
    //     //     bin_client: sc_i_minus_1,
    //     // };

    //     let sc_i_plus_2 = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client + 2) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };
    //     // let bin_store_client_i_plus_2 = BinStoreClient {
    //     //     name: String,
    //     //     colon_escaped_name: String,
    //     //     clients: Vec<StorageClient>,
    //     //     bin_client: sc_i_plus_2,
    //     // };

    //     let sc_i_plus_1_secondary_list = sc_i_plus_1.list_get(KEY_SECONDARY_LIST).await?;
    //     let mut bin_name_hashset = HashSet::new();

    //     for bin_name in sc_i_plus_1_secondary_list.0 {
    //         if bin_name_hashset.contains(&bin_name) {
    //             continue;
    //         } else {
    //             bin_name_hashset.insert(bin_name.clone());
    //         }
    //         let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
    //         colon_escaped_name.push_str(&"::".to_string());

    //         let bin_store_client_i_plus_1 = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i_plus_1.clone(),
    //         };

    //         let bin_store_client_i_plus_2 = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i_plus_2.clone(),
    //         };

    //         let storage::List(fetched_log) =
    //             bin_store_client_i_plus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

    //         // regenerate data from log and serve query
    //         let mut deserialized_log: Vec<UpdateLog> = fetched_log
    //             .iter()
    //             .map(|x| serde_json::from_str(&x).unwrap())
    //             .collect::<Vec<UpdateLog>>();

    //         // sort the deserialized log as per timestamp
    //         // keep the fields in the required order so that they are sorted in that same order.
    //         deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
    //         deserialized_log = Keeper::remove_duplicates_from_log(deserialized_log);
    //         // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
    //         // Based on the output of the set operations, generate result for the requested get op and return
    //         // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

    //         // just need to keep track of seq_num to avoid considering duplicate ops in log
    //         //let mut max_seen_seq_num = 0u64;

    //         //let mut reserialized_log = vec![];
    //         for log in deserialized_log {
    //             let serialized_log_elem = serde_json::to_string(&log)?;

    //             let append_kv = storage::KeyValue {
    //                 key: bin_name.clone(),
    //                 value: serialized_log_elem,
    //             };

    //             bin_store_client_i_plus_2.list_append(&append_kv).await?;
    //         }

    //         // append binname to secondary list
    //         let bin_name_append_kv = storage::KeyValue {
    //             key: KEY_SECONDARY_LIST.to_string().clone(),
    //             value: bin_name.clone(),
    //         };
    //         sc_i_plus_2.list_append(&bin_name_append_kv).await?;

    //         // move the keys from secondary list of sc_i+1 to primary list to make it a primary for those keys
    //         let primary_move_kv = storage::KeyValue {
    //             key: KEY_PRIMARY_LIST.to_string().clone(),
    //             value: bin_name.clone(),
    //         };
    //         sc_i_plus_1.list_append(&primary_move_kv).await?;
    //         sc_i_plus_1.list_remove(&bin_name_append_kv).await?;

    //         // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    //     }

    //     // create a second copy of primary data in i-1 in i+1
    //     let sc_i_minus_1_primary_list = sc_i_minus_1.list_get(KEY_PRIMARY_LIST).await?;
    //     let mut bin_name_hashset = HashSet::new();

    //     for bin_name in sc_i_minus_1_primary_list.0 {
    //         if bin_name_hashset.contains(&bin_name) {
    //             continue;
    //         } else {
    //             bin_name_hashset.insert(bin_name.clone());
    //         }

    //         let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
    //         colon_escaped_name.push_str(&"::".to_string());

    //         let bin_store_client_i_minus_1 = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i_minus_1.clone(),
    //         };

    //         let bin_store_client_i_plus_1 = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i_plus_1.clone(),
    //         };

    //         let storage::List(fetched_log) =
    //             bin_store_client_i_minus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

    //         // regenerate data from log and serve query
    //         let mut deserialized_log: Vec<UpdateLog> = fetched_log
    //             .iter()
    //             .map(|x| serde_json::from_str(&x).unwrap())
    //             .collect::<Vec<UpdateLog>>();

    //         // sort the deserialized log as per timestamp
    //         // keep the fields in the required order so that they are sorted in that same order.
    //         deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
    //         deserialized_log = Keeper::remove_duplicates_from_log(deserialized_log);
    //         // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
    //         // Based on the output of the set operations, generate result for the requested get op and return
    //         // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

    //         // just need to keep track of seq_num to avoid considering duplicate ops in log
    //         //let mut max_seen_seq_num = 0u64;

    //         //let mut reserialized_log = vec![];
    //         for log in deserialized_log {
    //             let serialized_log_elem = serde_json::to_string(&log)?;

    //             let append_kv = storage::KeyValue {
    //                 key: bin_name.clone(),
    //                 value: serialized_log_elem,
    //             };

    //             bin_store_client_i_plus_1.list_append(&append_kv).await?;
    //         }

    //         // append binname to secondary list
    //         let bin_name_append_kv = storage::KeyValue {
    //             key: KEY_SECONDARY_LIST.to_string().clone(),
    //             value: bin_name.clone(),
    //         };
    //         sc_i_plus_1.list_append(&bin_name_append_kv).await?;

    //         // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    //     }

    //     Ok(())
    // }

    // pub async fn migrate_data_when_up(&self, up_storage_client_idx: usize) -> TribResult<()> {
    //     let target_storage_client =
    //         *self.first_bc_index.read().unwrap() + up_storage_client_idx as u32;
    //     let sc_i_plus_1 = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client + 1) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };

    //     let sc_i = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };

    //     let sc_i_minus_1 = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client - 1) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };

    //     let sc_i_plus_2 = StorageClient {
    //         addr: format!(
    //             "http://{}",
    //             self.backs[((target_storage_client + 2) % self.backs.len() as u32) as usize]
    //         ),
    //         cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
    //     };

    //     let sc_i_minus_1_primary_list = sc_i_minus_1.list_get(KEY_PRIMARY_LIST).await?;
    //     let mut bin_name_hashset = HashSet::new();

    //     for bin_name in sc_i_minus_1_primary_list.0 {
    //         if bin_name_hashset.contains(&bin_name) {
    //             continue;
    //         } else {
    //             bin_name_hashset.insert(bin_name.clone());
    //         }

    //         let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
    //         colon_escaped_name.push_str(&"::".to_string());

    //         let bin_store_client_i_minus_1 = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i_minus_1.clone(),
    //         };

    //         let bin_store_client_i = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i.clone(),
    //         };

    //         let storage::List(fetched_log) =
    //             bin_store_client_i_minus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

    //         // regenerate data from log and serve query
    //         let mut deserialized_log: Vec<UpdateLog> = fetched_log
    //             .iter()
    //             .map(|x| serde_json::from_str(&x).unwrap())
    //             .collect::<Vec<UpdateLog>>();

    //         // sort the deserialized log as per timestamp
    //         // keep the fields in the required order so that they are sorted in that same order.
    //         deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
    //         deserialized_log = Keeper::remove_duplicates_from_log(deserialized_log);
    //         // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
    //         // Based on the output of the set operations, generate result for the requested get op and return
    //         // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

    //         // just need to keep track of seq_num to avoid considering duplicate ops in log
    //         //let mut max_seen_seq_num = 0u64;

    //         //let mut reserialized_log = vec![];
    //         for log in deserialized_log {
    //             let serialized_log_elem = serde_json::to_string(&log)?;

    //             let append_kv = storage::KeyValue {
    //                 key: bin_name.clone(),
    //                 value: serialized_log_elem,
    //             };

    //             bin_store_client_i.list_append(&append_kv).await?;
    //         }

    //         // append binname to secondary list
    //         let bin_name_append_kv = storage::KeyValue {
    //             key: KEY_SECONDARY_LIST.to_string().clone(),
    //             value: bin_name.clone(),
    //         };
    //         sc_i.list_append(&bin_name_append_kv).await?;

    //         sc_i_plus_1.list_remove(&bin_name_append_kv).await?;

    //         // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    //     }

    //     // Recovery step 1
    //     let sc_i_plus_1_primary_list = sc_i_plus_1.list_get(KEY_PRIMARY_LIST).await?;
    //     let mut bin_name_hashset = HashSet::new();

    //     let n = self.backs.len() as u64;
    //     let mut hasher = DefaultHasher::new();

    //     for bin_name in sc_i_plus_1_primary_list.0 {
    //         if bin_name_hashset.contains(&bin_name) {
    //             continue;
    //         } else {
    //             bin_name_hashset.insert(bin_name.clone());
    //         }

    //         hasher.write(bin_name.clone().as_bytes());

    //         let hash = hasher.finish();
    //         if (hash % n) as u32 > target_storage_client {
    //             continue;
    //         }
    //         let backend_addr = &self.backs[(hash % n) as usize];

    //         let mut colon_escaped_name: String = colon::escape(bin_name.clone()).to_owned();
    //         colon_escaped_name.push_str(&"::".to_string());

    //         let bin_store_client_i_plus_1 = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i_plus_1.clone(),
    //         };

    //         let bin_store_client_i = BinStoreClient {
    //             name: bin_name.clone(),
    //             colon_escaped_name: colon_escaped_name.clone(),
    //             clients: vec![], // TODO: Check later
    //             bin_client: sc_i.clone(),
    //         };

    //         let storage::List(fetched_log) =
    //             bin_store_client_i_plus_1.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

    //         // regenerate data from log and serve query
    //         let mut deserialized_log: Vec<UpdateLog> = fetched_log
    //             .iter()
    //             .map(|x| serde_json::from_str(&x).unwrap())
    //             .collect::<Vec<UpdateLog>>();

    //         // sort the deserialized log as per timestamp
    //         // keep the fields in the required order so that they are sorted in that same order.
    //         deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num
    //         deserialized_log = Keeper::remove_duplicates_from_log(deserialized_log);
    //         // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
    //         // Based on the output of the set operations, generate result for the requested get op and return
    //         // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

    //         // just need to keep track of seq_num to avoid considering duplicate ops in log
    //         //let mut max_seen_seq_num = 0u64;

    //         //let mut reserialized_log = vec![];
    //         for log in deserialized_log {
    //             let serialized_log_elem = serde_json::to_string(&log)?;

    //             let append_kv = storage::KeyValue {
    //                 key: bin_name.clone(),
    //                 value: serialized_log_elem,
    //             };

    //             bin_store_client_i.list_append(&append_kv).await?;
    //         }

    //         // append binname to secondary list
    //         let bin_name_append_kv = storage::KeyValue {
    //             key: KEY_PRIMARY_LIST.to_string().clone(),
    //             value: bin_name.clone(),
    //         };
    //         sc_i.list_append(&bin_name_append_kv).await?;

    //         let bin_name_move_primary_to_secondary = storage::KeyValue {
    //             key: KEY_SECONDARY_LIST.to_string().clone(),
    //             value: bin_name.clone(),
    //         };
    //         sc_i_plus_1
    //             .list_append(&bin_name_move_primary_to_secondary)
    //             .await?;
    //         sc_i_plus_1.list_remove(&bin_name_append_kv).await?;

    //         sc_i_plus_2
    //             .list_remove(&bin_name_move_primary_to_secondary)
    //             .await?;

    //         // let reserialized_log = serde_json::to_string(&deserialized_log)?;
    //     }

    //     Ok(())
    // }
}

#[async_trait]
impl KeeperSync for Keeper {
    async fn get_heartbeat(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<Heartbeat>, tonic::Status> {
        let r: Result<tonic::Response<Heartbeat>, tonic::Status> =
            Ok(tonic::Response::new(Heartbeat {
                max_clock: *self.max_clock_so_far.read().unwrap(),
                live_back_list: serde_json::to_string(&self.live_backends_list_kp).unwrap(),
                keeper_index: self.index,
                first_back_index: *self.first_bc_index.read().unwrap(),
                last_back_index: *self.last_bc_index.read().unwrap(),
            }));
        return r;
    }
}
