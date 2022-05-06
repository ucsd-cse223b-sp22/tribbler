// This is a wrapper over lab2_bin_store_client

use crate::lab2::storage_client::StorageClient;
use async_trait::async_trait;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};
use tribbler::err::TribblerError;
use tribbler::storage::{KeyList, KeyString, Storage};
use tribbler::{err::TribResult, storage};

use super::bin_store_client::BinStoreClient;

pub static KEY_UPDATE_LOG: &str = "update_log";
pub static KEY_PRIMARY_LIST: &str = "primary_list";
pub static KEY_SECONDARY_LIST: &str = "secondary_list";
pub static KEY_LIVE_BACKENDS_LIST: &str = "live_backends_list";

pub struct Lab3BinStoreClient {
    pub name: String,
    pub colon_escaped_name: String,
    pub back_addrs: Vec<String>,
    pub clients: Vec<StorageClient>,
    pub bin_store_client: Arc<Mutex<BinStoreClient>>,
    pub bin_client: Arc<Mutex<StorageClient>>,
    pub bin_client_index: Arc<Mutex<usize>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum UpdateOperation {
    Set,
    ListAppend,
    ListRemove,
}

/// A type comprising key-value pair
/// Overriding the trib storage key value to handle serialization of this
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KeyValue {
    /// the key
    pub key: String,
    /// the value
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UpdateLog {
    seq_num: u64,
    update_operation: UpdateOperation,
    kv_params: KeyValue, // override  KeyValue and implement serialize for it
}

impl Ord for UpdateLog {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq_num.cmp(&other.seq_num)
    }
}

impl Eq for UpdateLog {}

impl PartialOrd for UpdateLog {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.seq_num.partial_cmp(&other.seq_num)
    }
}

impl PartialEq for UpdateLog {
    fn eq(&self, other: &Self) -> bool {
        self.seq_num == other.seq_num
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LiveBackends {
    backs: Vec<String>,
    is_alive_list: Vec<bool>,
}

impl Lab3BinStoreClient {
    async fn find_next_iter(&self, curr_bin_client_index: usize) -> TribResult<BinStoreClient> {
        //log::info!("In find_next_iter()");

        let n = self.back_addrs.len() as u64;

        // log::info!(
        //     "In find_next_iter()::curr_bin_client_index: {}",
        //     &curr_bin_client_index
        // );

        let mut primary_backend_index = curr_bin_client_index;

        let mut is_next_live_found = false;

        for backend_index_iter in 0..n {
            // log::info!(
            //     "In find_next_iter()::iteration: {}",
            //     backend_index_iter + curr_bin_client_index as u64
            // );

            let backend_addr = &self.back_addrs
                [((backend_index_iter + curr_bin_client_index as u64) % n) as usize]; // start from hashed_backend_index

            // log::info!(
            //     "In find_next_iter()::backend_addr: {}",
            //     backend_addr.clone()
            // );

            let client = StorageClient {
                addr: format!("http://{}", backend_addr.clone())
                    .as_str()
                    .to_owned(),
                cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
            };

            // perform clock() rpc call to check if the backend is alive
            match client.clock(0).await {
                Ok(_) => {
                    // log::info!("In find_next_iter()::getting clock val");

                    primary_backend_index =
                        ((backend_index_iter + curr_bin_client_index as u64) % n) as usize;
                    is_next_live_found = true;
                    break;
                } // backend alive make it primary
                Err(_) => {} // backend not alive, continue iteration
            };
        }

        if is_next_live_found {
            // log::info!("In find_next_iter()::next_alive_found",);

            // get a client to the primary backend
            let clone_bin_client_index = Arc::clone(&self.bin_client_index);
            let mut locked_bin_client_index = clone_bin_client_index.lock().await;

            // log::info!(
            //     "In find_next_iter()::primary_index: {}",
            //     primary_backend_index.clone()
            // );

            *locked_bin_client_index = primary_backend_index.clone() as usize;

            // log::info!(
            //     "In find_next_iter()::primary_backend_index: {}",
            //     (*locked_bin_client_index).clone()
            // );
            std::mem::drop(locked_bin_client_index);

            let backend_addr = &self.back_addrs[primary_backend_index as usize].clone();

            // log::info!(
            //     "In find_next_iter()::backend_addr: {}",
            //     backend_addr.clone()
            // );

            let clone_bin_client = Arc::clone(&self.bin_client); // TODO: rename bin_client to STORAGE_CLIENT for clarity
            let mut locked_bin_client = clone_bin_client.lock().await;
            *locked_bin_client = StorageClient {
                addr: format!("http://{}", backend_addr.clone())
                    .as_str()
                    .to_owned(),
                cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
            };

            // generate corresponding BinStoreClient
            let clone_bin_store_client = Arc::clone(&self.bin_store_client);
            let mut locked_bin_store_client = clone_bin_store_client.lock().await;
            *locked_bin_store_client = BinStoreClient {
                name: self.name.clone(),
                colon_escaped_name: self.colon_escaped_name.clone(),
                clients: self.clients.clone(),
                bin_client: (*locked_bin_client).clone(),
            };

            return Ok((*locked_bin_store_client).clone());
        } else {
            // no live backend found, return error
            return Err(Box::new(TribblerError::Unknown(
                "No live backend found".to_string(),
            )));
        }
    }

    async fn find_next_iter_secondary(
        &self,
        curr_bin_client_index: usize,
    ) -> TribResult<BinStoreClient> {
        //log::info!("In find_next_iter()");

        let n = self.back_addrs.len() as u64;

        // log::info!(
        //     "In find_next_iter()::curr_bin_client_index: {}",
        //     &curr_bin_client_index
        // );

        let mut secondary_backend_index = curr_bin_client_index;

        let mut is_next_live_found = false;

        for backend_index_iter in 0..n {
            // log::info!(
            //     "In find_next_iter()::iteration: {}",
            //     backend_index_iter + curr_bin_client_index as u64
            // );

            let backend_addr = &self.back_addrs
                [((backend_index_iter + curr_bin_client_index as u64) % n) as usize]; // start from hashed_backend_index

            // log::info!(
            //     "In find_next_iter()::backend_addr: {}",
            //     backend_addr.clone()
            // );

            let client = StorageClient {
                addr: format!("http://{}", backend_addr.clone())
                    .as_str()
                    .to_owned(),
                cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
            };

            // perform clock() rpc call to check if the backend is alive
            match client.clock(0).await {
                Ok(_) => {
                    // log::info!("In find_next_iter()::getting clock val");

                    secondary_backend_index =
                        ((backend_index_iter + curr_bin_client_index as u64) % n) as usize;
                    is_next_live_found = true;
                    break;
                } // backend alive make it primary
                Err(_) => {} // backend not alive, continue iteration
            };
        }

        if is_next_live_found {
            // log::info!("In find_next_iter()::next_alive_found",);

            let backend_addr = &self.back_addrs[secondary_backend_index as usize].clone();

            let secondary_bin_client = StorageClient {
                addr: format!("http://{}", backend_addr.clone())
                    .as_str()
                    .to_owned(),
                cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
            };

            // generate corresponding BinStoreClient
            let secondary_bin_store_client = BinStoreClient {
                name: self.name.clone(),
                colon_escaped_name: self.colon_escaped_name.clone(),
                clients: self.clients.clone(),
                bin_client: secondary_bin_client.clone(),
            };

            return Ok(secondary_bin_store_client.clone());
        } else {
            // no live backend found, return error
            return Err(Box::new(TribblerError::Unknown(
                "No live backend found".to_string(),
            )));
        }
    }

    async fn find_next_from_live_list(&self) -> TribResult<BinStoreClient> {
        let clone_bin_client = Arc::clone(&self.bin_client);
        let locked_bin_client = clone_bin_client.lock().await;
        let cached_bin_client = (*locked_bin_client).clone();
        std::mem::drop(locked_bin_client);

        match cached_bin_client // DONE-TODO: change to storage_client
            .get(KEY_LIVE_BACKENDS_LIST) // live backends list is a serialized KeyValue where value is a serialized map of backends and their alive status
            .await
        {
            Ok(Some(live_backends_map)) => {
                // Got live backends map, find the secondary node

                // deserialize map
                let deserialized_live_backends_map: LiveBackends =
                    serde_json::from_str(&live_backends_map)?;

                // iterate in live backends list, start from location of the primary and take the next alive as secondary
                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let primary_backend_index = *locked_bin_client_index;
                std::mem::drop(locked_bin_client_index);

                let mut secondary_addr = String::from(""); // initialize it to empty for now

                //iterate over len modulo n
                let len_live_backends_list =
                    deserialized_live_backends_map.is_alive_list.len() as usize;
                for live_index in 1..len_live_backends_list {
                    // since primary is already found need to start from primary + 1 so start from index 1
                    if deserialized_live_backends_map.is_alive_list
                        [((live_index + primary_backend_index) % len_live_backends_list) as usize]
                    {
                        secondary_addr = deserialized_live_backends_map.backs[((live_index
                            + primary_backend_index)
                            % len_live_backends_list)
                            as usize]
                            .clone();
                        break;
                    }
                }

                if secondary_addr.eq("") {
                    // no live secondary backend found, return error
                    return Err(Box::new(TribblerError::Unknown(
                        "No live secondary backend found".to_string(),
                    )));
                }

                // generate storage client and bin store client for secondary
                let secondary_bin_client = StorageClient {
                    addr: format!("http://{}", secondary_addr.clone())
                        .as_str()
                        .to_owned(),
                    cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                };

                let secondary_bin_store_client = BinStoreClient {
                    name: self.name.clone(),
                    colon_escaped_name: self.colon_escaped_name.clone(),
                    clients: self.clients.clone(),
                    bin_client: secondary_bin_client,
                };

                return Ok(secondary_bin_store_client);
            }
            Ok(None) => {
                // no live secondary backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "Error in getting live backends list".to_string(),
                )));
            }
            Err(_) => {
                // no live secondary backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "Error in getting live backends list".to_string(),
                )));
            }
        }
    }
}

// use mutex

// Check the original functionality for list_get, list_set etc

#[async_trait]
impl storage::KeyString for Lab3BinStoreClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // fetch log by forwarding request to bin_store_client. It will handle prepending name and handling bin part

        //log::info!("In get");

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        }; // this will work with the current bin_client_index being the hashed value
           // this will update the next alive in the cached bin store client and cached storage client.

        // log::info!("back in get after finding next alive");

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        let mut combined_log: Vec<UpdateLog> = Vec::new();

        //let storage::List(fetched_log) =
        let fetched_log_result: Option<storage::List> =
            match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                Ok(v) => Some(v), // 1st shot, got log
                Err(_) => {
                    // A live backend just crashed; 2nd attempt by iterating through the list to find the next alive

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    let _ = match self.find_next_iter(curr_bin_client_index).await {
                        Ok(val) => val,
                        Err(_) => {
                            // no live backend found, return error
                            return Err(Box::new(TribblerError::Unknown(
                                "No live backend found".to_string(),
                            )));
                        }
                    };

                    let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                    let locked_bin_store_client = clone_bin_store_client.lock().await;
                    let cached_bin_store_client = (*locked_bin_store_client).clone();
                    std::mem::drop(locked_bin_store_client);

                    match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        Ok(v) => Some(v), // got the log
                        Err(_) => {
                            // this should not return error
                            // continue to secondary
                            None
                        }
                    }
                }
            };

        if let Some(storage::List(fetched_log)) = fetched_log_result {
            // regenerate data from log and serve query
            let mut deserialized_log: Vec<UpdateLog> = fetched_log
                .iter()
                .map(|x| serde_json::from_str(&x).unwrap())
                .collect::<Vec<UpdateLog>>();

            // sort the deserialized log as per timestamp
            // keep the fields in the required order so that they are sorted in that same order.
            // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

            combined_log.append(&mut deserialized_log);
        }

        // get from secondary as well
        let secondary_bin_store_client_result: Option<BinStoreClient> =
            match self.find_next_from_live_list().await {
                //let secondary_bin_store_client = match self.find_next_from_live_list().await {
                Ok(val) => Some(val),
                Err(_) => {
                    // iterate all if live backends list not able to give secondary

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    match self
                        .find_next_iter_secondary(curr_bin_client_index + 1)
                        .await
                    {
                        Ok(val) => Some(val),
                        Err(_) => {
                            // no live backend found, return None since a backend exists but doesn't have data
                            None
                        } // do nothing now
                    }
                }
            };

        // secondary bin store client now contains pointer to secondary node

        if let Some(secondary_bin_store_client) = secondary_bin_store_client_result {
            // get log
            let fetched_log_result: Option<storage::List> =
                match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                    Ok(val) => Some(val),
                    Err(_) => {
                        // no log data available
                        // do nothing
                        None
                    }
                }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

            if let Some(storage::List(fetched_log)) = fetched_log_result {
                let mut deserialized_log: Vec<UpdateLog> = fetched_log
                    .iter()
                    .map(|x| serde_json::from_str(&x).unwrap())
                    .collect::<Vec<UpdateLog>>();

                combined_log.append(&mut deserialized_log);
            }
        }

        combined_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        let mut max_seen_seq_num = 0u64;

        let mut return_value: String = String::from("");

        for each_log in combined_log {
            if matches!(each_log.update_operation, UpdateOperation::Set) {
                if each_log.kv_params.key.eq(key) {
                    if each_log.seq_num > max_seen_seq_num {
                        return_value = each_log.kv_params.value.clone();
                        max_seen_seq_num = each_log.seq_num;
                    }
                }
            }
        }

        // Logs not found in this primary - a new primary where the migration might not be complete. iterate its live backends list and contact the next live

        // if return_value.eq("") {
        //     // get live backends list
        //     let secondary_bin_store_client = match self.find_next_from_live_list().await {
        //         Ok(val) => val,
        //         Err(_) => {
        //             // iterate all if live backends list not able to give secondary

        //             let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        //             let locked_bin_client_index = clone_bin_client_index.lock().await;
        //             let curr_bin_client_index = *locked_bin_client_index;
        //             std::mem::drop(locked_bin_client_index);

        //             match self
        //                 .find_next_iter_secondary(curr_bin_client_index + 1)
        //                 .await
        //             {
        //                 Ok(val) => val,
        //                 Err(_) => {
        //                     // no live backend found, return None since a backend exists but doesn't have data
        //                     return Ok(None);
        //                 }
        //             }
        //         }
        //     };

        //     // secondary bin store client now contains pointer to secondary node

        //     // get log
        //     let storage::List(fetched_log) =
        //         match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
        //             Ok(val) => val,
        //             Err(_) => {
        //                 // no log data available
        //                 return Ok(None);
        //             }
        //         }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        //     // regenerate data from log and serve query
        //     let mut deserialized_log: Vec<UpdateLog> = fetched_log
        //         .iter()
        //         .map(|x| serde_json::from_str(&x).unwrap())
        //         .collect::<Vec<UpdateLog>>();

        //     // sort the deserialized log as per timestamp
        //     // keep the fields in the required order so that they are sorted in that same order.
        //     deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        //     // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        //     // Based on the output of the set operations, generate result for the requested get op and return
        //     // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        //     // just need to keep track of seq_num to avoid considering duplicate ops in log
        //     let mut max_seen_seq_num = 0u64;

        //     for each_log in deserialized_log {
        //         if matches!(each_log.update_operation, UpdateOperation::Set) {
        //             if each_log.kv_params.key.eq(key) {
        //                 if each_log.seq_num > max_seen_seq_num {
        //                     return_value = each_log.kv_params.value.clone();
        //                     max_seen_seq_num = each_log.seq_num;
        //                 }
        //             }
        //         }
        //     }
        // }

        if return_value.eq("") {
            Ok(None)
        } else {
            Ok(Some(return_value))
        }
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        // log::info!("In set");

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        };

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        match cached_bin_store_client.clock(0).await {
            Ok(seq_num) => {
                // log::info!("1st success");

                // note the new seq num and continue to list append here itself
                let new_update_log = UpdateLog {
                    seq_num: seq_num, // DONE-TODO: no plus one here
                    update_operation: UpdateOperation::Set,
                    kv_params: KeyValue {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                    },
                };

                // serialize new_update_log
                let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

                let log_append_kv = tribbler::storage::KeyValue {
                    key: KEY_UPDATE_LOG.to_string().clone(),
                    value: new_update_log_serialized,
                };

                // list-append log
                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                let locked_bin_store_client = clone_bin_store_client.lock().await;
                let cached_bin_store_client = (*locked_bin_store_client).clone();
                std::mem::drop(locked_bin_store_client);

                let _ = match cached_bin_store_client.list_append(&log_append_kv).await {
                    Ok(_) => {
                        // clock and primary append is successful
                        // log::info!("2 success");

                        // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                        let primary_list_append_kv = tribbler::storage::KeyValue {
                            key: KEY_PRIMARY_LIST.to_string().clone(),
                            value: self.name.clone(),
                        };

                        let clone_bin_client = Arc::clone(&self.bin_client);
                        let locked_bin_client = clone_bin_client.lock().await;
                        let cached_bin_client = (*locked_bin_client).clone();
                        std::mem::drop(locked_bin_client);

                        let _ = match cached_bin_client.list_append(&primary_list_append_kv).await {
                            Ok(_) => {
                                // clock, primary append and primary list username append all success
                                // append to secondary
                                let secondary_bin_store_client =
                                    match self.find_next_from_live_list().await {
                                        Ok(val) => val,
                                        Err(_) => {
                                            // iterate all if live backends list not able to give secondary

                                            let clone_bin_client_index =
                                                Arc::clone(&self.bin_client_index);
                                            let locked_bin_client_index =
                                                clone_bin_client_index.lock().await;
                                            let curr_bin_client_index = *locked_bin_client_index;
                                            std::mem::drop(locked_bin_client_index);

                                            match self
                                                .find_next_iter_secondary(curr_bin_client_index + 1)
                                                .await
                                            {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // no live backend found, return None since already written to primary
                                                    return Ok(true);
                                                }
                                            }
                                        }
                                    };

                                // list-append log
                                let _ = match secondary_bin_store_client
                                    .list_append(&log_append_kv)
                                    .await
                                {
                                    Ok(_) => {
                                        // secondary append also success
                                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                        let secondary_list_append_kv =
                                            tribbler::storage::KeyValue {
                                                key: KEY_SECONDARY_LIST.to_string().clone(),
                                                value: self.name.clone(),
                                            };

                                        let _ = match secondary_bin_store_client
                                            .bin_client
                                            .list_append(&secondary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {
                                                // all success
                                                return Ok(true);
                                            }
                                            Err(_) => {
                                                //first failure
                                                let secondary_bin_store_client =
                                                    match self.find_next_from_live_list().await {
                                                        Ok(val) => val,
                                                        Err(_) => {
                                                            // iterate all if live backends list not able to give secondary

                                                            let clone_bin_client_index =
                                                                Arc::clone(&self.bin_client_index);
                                                            let locked_bin_client_index =
                                                                clone_bin_client_index.lock().await;
                                                            let curr_bin_client_index =
                                                                *locked_bin_client_index;
                                                            std::mem::drop(locked_bin_client_index);

                                                            match self
                                                                .find_next_iter_secondary(
                                                                    curr_bin_client_index + 1,
                                                                )
                                                                .await
                                                            {
                                                                Ok(val) => val,
                                                                Err(_) => {
                                                                    // no live backend found, return None since already written to primary
                                                                    return Ok(true);
                                                                }
                                                            }
                                                        }
                                                    };

                                                // list-append log
                                                let _ = match secondary_bin_store_client
                                                    .list_append(&log_append_kv)
                                                    .await
                                                {
                                                    Ok(_) => {}
                                                    Err(_) => {}
                                                }; // what if this fails? suppress error

                                                // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                                let secondary_list_append_kv =
                                                    tribbler::storage::KeyValue {
                                                        key: KEY_SECONDARY_LIST.to_string().clone(),
                                                        value: self.name.clone(),
                                                    };

                                                let _ = match secondary_bin_store_client
                                                    .bin_client
                                                    .list_append(&secondary_list_append_kv)
                                                    .await
                                                {
                                                    Ok(_) => {}
                                                    Err(_) => {}
                                                }; // what if this fails? suppress error

                                                return Ok(true);
                                            }
                                        }; // what if this fails? suppress error
                                    }
                                    Err(_) => {
                                        // first failure

                                        let secondary_bin_store_client =
                                            match self.find_next_from_live_list().await {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // iterate all if live backends list not able to give secondary

                                                    let clone_bin_client_index =
                                                        Arc::clone(&self.bin_client_index);
                                                    let locked_bin_client_index =
                                                        clone_bin_client_index.lock().await;
                                                    let curr_bin_client_index =
                                                        *locked_bin_client_index;
                                                    std::mem::drop(locked_bin_client_index);

                                                    match self
                                                        .find_next_iter_secondary(
                                                            curr_bin_client_index + 1,
                                                        )
                                                        .await
                                                    {
                                                        Ok(val) => val,
                                                        Err(_) => {
                                                            // no live backend found, return None since already written to primary
                                                            return Ok(true);
                                                        }
                                                    }
                                                }
                                            };

                                        // list-append log
                                        let _ = match secondary_bin_store_client
                                            .list_append(&log_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                        let secondary_list_append_kv =
                                            tribbler::storage::KeyValue {
                                                key: KEY_SECONDARY_LIST.to_string().clone(),
                                                value: self.name.clone(),
                                            };

                                        let _ = match secondary_bin_store_client
                                            .bin_client
                                            .list_append(&secondary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        return Ok(true);
                                    }
                                }; // what if this fails? suppress error
                            }
                            Err(_) => {
                                // what if this fails? 1st failure in this branch
                                // iterate to get another primary and do the whole process. works because 1 failure already

                                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                let locked_bin_client_index = clone_bin_client_index.lock().await;
                                let curr_bin_client_index = *locked_bin_client_index;
                                std::mem::drop(locked_bin_client_index);

                                let _ = match self.find_next_iter(curr_bin_client_index).await {
                                    Ok(val) => val,
                                    Err(_) => {
                                        // no live backend found, return error
                                        return Err(Box::new(TribblerError::Unknown(
                                            "No live backend found".to_string(),
                                        )));
                                    }
                                };

                                // get the new seq num.
                                // generate corresponding BinStoreClient
                                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                                let locked_bin_store_client = clone_bin_store_client.lock().await;
                                let cached_bin_store_client = (*locked_bin_store_client).clone();
                                std::mem::drop(locked_bin_store_client);

                                match cached_bin_store_client.clock(0).await {
                                    Ok(seq_num) => {
                                        let new_update_log = UpdateLog {
                                            seq_num: seq_num, // DONE-TODO: no plus one here
                                            update_operation: UpdateOperation::Set,
                                            kv_params: KeyValue {
                                                key: kv.key.clone(),
                                                value: kv.value.clone(),
                                            },
                                        };

                                        // serialize new_update_log
                                        let new_update_log_serialized =
                                            serde_json::to_string(&new_update_log)?;

                                        let log_append_kv = tribbler::storage::KeyValue {
                                            key: KEY_UPDATE_LOG.to_string().clone(),
                                            value: new_update_log_serialized,
                                        };

                                        // list-append log
                                        let clone_bin_store_client =
                                            Arc::clone(&self.bin_store_client);
                                        let locked_bin_store_client =
                                            clone_bin_store_client.lock().await;
                                        let cached_bin_store_client =
                                            (*locked_bin_store_client).clone();
                                        std::mem::drop(locked_bin_store_client);

                                        cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                                        // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                                        let primary_list_append_kv = tribbler::storage::KeyValue {
                                            key: KEY_PRIMARY_LIST.to_string().clone(),
                                            value: self.name.clone(),
                                        };

                                        let clone_bin_client = Arc::clone(&self.bin_client);
                                        let locked_bin_client = clone_bin_client.lock().await;
                                        let cached_bin_client = (*locked_bin_client).clone();
                                        std::mem::drop(locked_bin_client);

                                        let _ = match cached_bin_client
                                            .list_append(&primary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? Won't fail bcz 1 failure in this branch

                                        // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.
                                        // won't fail in this branch coz 1 failure

                                        // also append log to secondary - the next in the live backends list
                                        // get live backends list
                                        let secondary_bin_store_client =
                                            match self.find_next_from_live_list().await {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // iterate all if live backends list not able to give secondary

                                                    let clone_bin_client_index =
                                                        Arc::clone(&self.bin_client_index);
                                                    let locked_bin_client_index =
                                                        clone_bin_client_index.lock().await;
                                                    let curr_bin_client_index =
                                                        *locked_bin_client_index;
                                                    std::mem::drop(locked_bin_client_index);

                                                    match self
                                                        .find_next_iter_secondary(
                                                            curr_bin_client_index + 1,
                                                        )
                                                        .await
                                                    {
                                                        Ok(val) => val,
                                                        Err(_) => {
                                                            // no live backend found, return None since already written to primary
                                                            return Ok(true);
                                                        }
                                                    }
                                                }
                                            };

                                        // list-append log
                                        let _ = match secondary_bin_store_client
                                            .list_append(&log_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                        let secondary_list_append_kv =
                                            tribbler::storage::KeyValue {
                                                key: KEY_SECONDARY_LIST.to_string().clone(),
                                                value: self.name.clone(),
                                            };

                                        let _ = match secondary_bin_store_client
                                            .bin_client
                                            .list_append(&secondary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        return Ok(true); // may keep a boolean variable to combine result of all
                                    }
                                    Err(_) => {
                                        // if not found even now, then return error
                                        return Err(Box::new(TribblerError::Unknown(
                                            "Not able to connect".to_string(),
                                        )));
                                    }
                                }
                            }
                        };
                    }
                    Err(_) => {
                        // regenerate clock and start again with one failure in this branch

                        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                        let locked_bin_client_index = clone_bin_client_index.lock().await;
                        let curr_bin_client_index = *locked_bin_client_index;
                        std::mem::drop(locked_bin_client_index);

                        let _ = match self.find_next_iter(curr_bin_client_index).await {
                            Ok(val) => val,
                            Err(_) => {
                                // no live backend found, return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "No live backend found".to_string(),
                                )));
                            }
                        };

                        // get the new seq num.
                        // generate corresponding BinStoreClient
                        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        let locked_bin_store_client = clone_bin_store_client.lock().await;
                        let cached_bin_store_client = (*locked_bin_store_client).clone();
                        std::mem::drop(locked_bin_store_client);

                        match cached_bin_store_client.clock(0).await {
                            Ok(seq_num) => {
                                let new_update_log = UpdateLog {
                                    seq_num: seq_num, // DONE-TODO: no plus one here
                                    update_operation: UpdateOperation::Set,
                                    kv_params: KeyValue {
                                        key: kv.key.clone(),
                                        value: kv.value.clone(),
                                    },
                                };

                                // serialize new_update_log
                                let new_update_log_serialized =
                                    serde_json::to_string(&new_update_log)?;

                                let log_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_UPDATE_LOG.to_string().clone(),
                                    value: new_update_log_serialized,
                                };

                                // list-append log
                                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                                let locked_bin_store_client = clone_bin_store_client.lock().await;
                                let cached_bin_store_client = (*locked_bin_store_client).clone();
                                std::mem::drop(locked_bin_store_client);

                                cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                                // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                                let primary_list_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_PRIMARY_LIST.to_string().clone(),
                                    value: self.name.clone(),
                                };

                                let clone_bin_client = Arc::clone(&self.bin_client);
                                let locked_bin_client = clone_bin_client.lock().await;
                                let cached_bin_client = (*locked_bin_client).clone();
                                std::mem::drop(locked_bin_client);

                                let _ = match cached_bin_client
                                    .list_append(&primary_list_append_kv)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }; // what if this fails? Won't fail bcz 1 failure in this branch

                                // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.
                                // won't fail in this branch coz 1 failure

                                // also append log to secondary - the next in the live backends list
                                // get live backends list
                                let secondary_bin_store_client =
                                    match self.find_next_from_live_list().await {
                                        Ok(val) => val,
                                        Err(_) => {
                                            // iterate all if live backends list not able to give secondary

                                            let clone_bin_client_index =
                                                Arc::clone(&self.bin_client_index);
                                            let locked_bin_client_index =
                                                clone_bin_client_index.lock().await;
                                            let curr_bin_client_index = *locked_bin_client_index;
                                            std::mem::drop(locked_bin_client_index);
                                            match self
                                                .find_next_iter_secondary(curr_bin_client_index + 1)
                                                .await
                                            {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // no live backend found, return None since already written to primary
                                                    return Ok(true);
                                                }
                                            }
                                        }
                                    };

                                // list-append log
                                let _ = match secondary_bin_store_client
                                    .list_append(&log_append_kv)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }; // what if this fails? suppress error

                                // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                let secondary_list_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_SECONDARY_LIST.to_string().clone(),
                                    value: self.name.clone(),
                                };

                                let _ = match secondary_bin_store_client
                                    .bin_client
                                    .list_append(&secondary_list_append_kv)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }; // what if this fails? suppress error

                                return Ok(true); // may keep a boolean variable to combine result of all
                            }
                            Err(_) => {
                                // if not found even now, then return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "Not able to connect".to_string(),
                                )));
                            }
                        }
                    }
                }; // This won't fail as already one failure there in this branch
            }
            Err(_) => {
                // error: it just crashed. then find next alive node

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index;
                std::mem::drop(locked_bin_client_index);

                let _ = match self.find_next_iter(curr_bin_client_index).await {
                    Ok(val) => val,
                    Err(_) => {
                        // no live backend found, return error
                        return Err(Box::new(TribblerError::Unknown(
                            "No live backend found".to_string(),
                        )));
                    }
                };

                // get the new seq num.
                // generate corresponding BinStoreClient
                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                let locked_bin_store_client = clone_bin_store_client.lock().await;
                let cached_bin_store_client = (*locked_bin_store_client).clone();
                std::mem::drop(locked_bin_store_client);

                match cached_bin_store_client.clock(0).await {
                    Ok(seq_num) => {
                        let new_update_log = UpdateLog {
                            seq_num: seq_num, // DONE-TODO: no plus one here
                            update_operation: UpdateOperation::Set,
                            kv_params: KeyValue {
                                key: kv.key.clone(),
                                value: kv.value.clone(),
                            },
                        };

                        // serialize new_update_log
                        let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

                        let log_append_kv = tribbler::storage::KeyValue {
                            key: KEY_UPDATE_LOG.to_string().clone(),
                            value: new_update_log_serialized,
                        };

                        // list-append log
                        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        let locked_bin_store_client = clone_bin_store_client.lock().await;
                        let cached_bin_store_client = (*locked_bin_store_client).clone();
                        std::mem::drop(locked_bin_store_client);

                        cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                        // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                        let primary_list_append_kv = tribbler::storage::KeyValue {
                            key: KEY_PRIMARY_LIST.to_string().clone(),
                            value: self.name.clone(),
                        };

                        let clone_bin_client = Arc::clone(&self.bin_client);
                        let locked_bin_client = clone_bin_client.lock().await;
                        let cached_bin_client = (*locked_bin_client).clone();
                        std::mem::drop(locked_bin_client);

                        let _ = match cached_bin_client.list_append(&primary_list_append_kv).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }; // what if this fails? Won't fail bcz 1 failure in this branch

                        // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.
                        // won't fail in this branch coz 1 failure

                        // also append log to secondary - the next in the live backends list
                        // get live backends list
                        let secondary_bin_store_client = match self.find_next_from_live_list().await
                        {
                            Ok(val) => val,
                            Err(_) => {
                                // iterate all if live backends list not able to give secondary

                                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                let locked_bin_client_index = clone_bin_client_index.lock().await;
                                let curr_bin_client_index = *locked_bin_client_index;
                                std::mem::drop(locked_bin_client_index);
                                match self
                                    .find_next_iter_secondary(curr_bin_client_index + 1)
                                    .await
                                {
                                    Ok(val) => val,
                                    Err(_) => {
                                        // no live backend found, return None since already written to primary
                                        return Ok(true);
                                    }
                                }
                            }
                        };

                        // list-append log
                        let _ = match secondary_bin_store_client.list_append(&log_append_kv).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }; // what if this fails? suppress error

                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                        let secondary_list_append_kv = tribbler::storage::KeyValue {
                            key: KEY_SECONDARY_LIST.to_string().clone(),
                            value: self.name.clone(),
                        };

                        let _ = match secondary_bin_store_client
                            .bin_client
                            .list_append(&secondary_list_append_kv)
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }; // what if this fails? suppress error

                        return Ok(true); // may keep a boolean variable to combine result of all
                    }
                    Err(_) => {
                        // if not found even now, then return error
                        return Err(Box::new(TribblerError::Unknown(
                            "Not able to connect".to_string(),
                        )));
                    }
                }
            }
        };

        // got a primary and a new seq num

        // what if the primary dies right after above check?

        // let new_update_log = UpdateLog {
        //     seq_num: new_seq_num, // DONE-TODO: no plus one here
        //     update_operation: UpdateOperation::Set,
        //     kv_params: KeyValue {
        //         key: kv.key.clone(),
        //         value: kv.value.clone(),
        //     },
        // };

        // // serialize new_update_log
        // let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

        // let log_append_kv = tribbler::storage::KeyValue {
        //     key: KEY_UPDATE_LOG.to_string().clone(),
        //     value: new_update_log_serialized,
        // };

        // // list-append log
        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;

        // locked_bin_store_client.list_append(&log_append_kv).await?; // what if this fails

        // // add this bin to primary list of the node - SHOULD this be before appending the log?
        // let primary_list_append_kv = tribbler::storage::KeyValue {
        //     key: KEY_PRIMARY_LIST.to_string().clone(),
        //     value: self.name.clone(),
        // };

        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;

        // locked_bin_store_client
        //     .list_append(&primary_list_append_kv)
        //     .await?; // what if this fails?

        // // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.

        // // also append log to secondary - the next in the live backends list
        // // get live backends list

        // let secondary_bin_store_client = match self.find_next_from_live_list().await {
        //     Ok(val) => val,
        //     Err(_) => {
        //         // iterate all if live backends list not able to give secondary
        //         match self.find_next_iter().await {
        //             Ok(val) => val,
        //             Err(_) => {
        //                 // no live backend found, return None since already written to primary
        //                 return Ok(true);
        //             }
        //         }
        //     }
        // };

        // // list-append log
        // secondary_bin_store_client
        //     .list_append(&log_append_kv)
        //     .await?; // what if this fails? suppress error

        // // add this bin to secondary list of the node - SHOULD this be before appending the log?
        // let secondary_list_append_kv = tribbler::storage::KeyValue {
        //     key: KEY_SECONDARY_LIST.to_string().clone(),
        //     value: self.name.clone(),
        // };

        // secondary_bin_store_client
        //     .list_append(&secondary_list_append_kv)
        //     .await?; // what if this fails? suppress error

        // Ok(true) // may keep a boolean variable to combine result of all
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        // let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        // let locked_bin_client_index = clone_bin_client_index.lock().await;
        // let curr_bin_client_index = *locked_bin_client_index;
        // std::mem::drop(locked_bin_client_index);

        // let _ = match self.find_next_iter(curr_bin_client_index).await {
        //     Ok(val) => val,
        //     Err(_) => {
        //         // no live backend found, return error
        //         return Err(Box::new(TribblerError::Unknown(
        //             "No live backend found".to_string(),
        //         )));
        //     }
        // };

        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;
        // let result = locked_bin_store_client.list_keys(&p).await?;
        // Ok(result)

        // fetch log by forwarding request to bin_store_client. It will handle prepending name and handling bin part

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        }; // this will work with the current bin_client_index being the hashed value
           // this will update the next alive in the cached bin store client and cached storage client.

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        let mut combined_log: Vec<UpdateLog> = Vec::new();

        //let storage::List(fetched_log) =
        let fetched_log_result: Option<storage::List> =
            match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                Ok(v) => Some(v), // 1st shot, got log
                Err(_) => {
                    // A live backend just crashed; 2nd attempt by iterating through the list to find the next alive

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    let _ = match self.find_next_iter(curr_bin_client_index).await {
                        Ok(val) => val,
                        Err(_) => {
                            // no live backend found, return error
                            return Err(Box::new(TribblerError::Unknown(
                                "No live backend found".to_string(),
                            )));
                        }
                    };

                    let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                    let locked_bin_store_client = clone_bin_store_client.lock().await;
                    let cached_bin_store_client = (*locked_bin_store_client).clone();
                    std::mem::drop(locked_bin_store_client);

                    match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        Ok(v) => Some(v), // got the log
                        Err(_) => {
                            // this should not return error
                            // continue to secondary
                            None
                        }
                    }
                }
            };

        if let Some(storage::List(fetched_log)) = fetched_log_result {
            // regenerate data from log and serve query
            let mut deserialized_log: Vec<UpdateLog> = fetched_log
                .iter()
                .map(|x| serde_json::from_str(&x).unwrap())
                .collect::<Vec<UpdateLog>>();

            // sort the deserialized log as per timestamp
            // keep the fields in the required order so that they are sorted in that same order.
            // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

            combined_log.append(&mut deserialized_log);
        }

        // get from secondary as well
        let secondary_bin_store_client_result: Option<BinStoreClient> =
            match self.find_next_from_live_list().await {
                //let secondary_bin_store_client = match self.find_next_from_live_list().await {
                Ok(val) => Some(val),
                Err(_) => {
                    // iterate all if live backends list not able to give secondary

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    match self
                        .find_next_iter_secondary(curr_bin_client_index + 1)
                        .await
                    {
                        Ok(val) => Some(val),
                        Err(_) => {
                            // no live backend found, return None since a backend exists but doesn't have data
                            None
                        } // do nothing now
                    }
                }
            };

        // secondary bin store client now contains pointer to secondary node

        if let Some(secondary_bin_store_client) = secondary_bin_store_client_result {
            // get log
            let fetched_log_result: Option<storage::List> =
                match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                    Ok(val) => Some(val),
                    Err(_) => {
                        // no log data available
                        // do nothing
                        None
                    }
                }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

            if let Some(storage::List(fetched_log)) = fetched_log_result {
                let mut deserialized_log: Vec<UpdateLog> = fetched_log
                    .iter()
                    .map(|x| serde_json::from_str(&x).unwrap())
                    .collect::<Vec<UpdateLog>>();

                combined_log.append(&mut deserialized_log);
            }
        }

        combined_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        let mut max_seen_seq_num = 0u64;

        let mut return_value_set: HashSet<String> = HashSet::new();

        for each_log in combined_log {
            if matches!(each_log.update_operation, UpdateOperation::Set) {
                if each_log.kv_params.key.starts_with(&p.prefix)
                    && each_log.kv_params.key.ends_with(&p.suffix)
                {
                    if each_log.seq_num > max_seen_seq_num {
                        return_value_set.insert(each_log.kv_params.key.clone());
                        max_seen_seq_num = each_log.seq_num;
                    }
                }
            }
        }

        let mut return_value_list: Vec<String> = Vec::from_iter(return_value_set);

        // Logs not found in this primary - a new primary where the migration might not be complete. iterate its live backends list and contact the next live
        // if return_value_list.is_empty() {
        //     // get live backends list
        //     let secondary_bin_store_client = match self.find_next_from_live_list().await {
        //         Ok(val) => val,
        //         Err(_) => {
        //             // iterate all if live backends list not able to give secondary

        //             let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        //             let locked_bin_client_index = clone_bin_client_index.lock().await;
        //             let curr_bin_client_index = *locked_bin_client_index;
        //             std::mem::drop(locked_bin_client_index);

        //             match self
        //                 .find_next_iter_secondary(curr_bin_client_index + 1)
        //                 .await
        //             {
        //                 Ok(val) => val,
        //                 Err(_) => {
        //                     // no live backend found, return None since a backend exists but doesn't have data
        //                     return Ok(storage::List(Vec::new()));
        //                 }
        //             }
        //         }
        //     };

        //     // secondary bin store client now contains pointer to secondary node

        //     // get log
        //     let storage::List(fetched_log) =
        //         match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
        //             Ok(val) => val,
        //             Err(_) => {
        //                 // no log data available
        //                 return Ok(storage::List(Vec::new()));
        //             }
        //         }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        //     // regenerate data from log and serve query
        //     let mut deserialized_log: Vec<UpdateLog> = fetched_log
        //         .iter()
        //         .map(|x| serde_json::from_str(&x).unwrap())
        //         .collect::<Vec<UpdateLog>>();

        //     // sort the deserialized log as per timestamp
        //     // keep the fields in the required order so that they are sorted in that same order.
        //     deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        //     // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        //     // Based on the output of the set operations, generate result for the requested get op and return
        //     // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        //     // just need to keep track of seq_num to avoid considering duplicate ops in log
        //     let mut max_seen_seq_num = 0u64;

        //     let mut return_value_set: HashSet<String> = HashSet::new();

        //     for each_log in deserialized_log {
        //         if matches!(each_log.update_operation, UpdateOperation::Set) {
        //             if each_log.kv_params.key.starts_with(&p.prefix)
        //                 && each_log.kv_params.key.ends_with(&p.suffix)
        //             {
        //                 if each_log.seq_num > max_seen_seq_num {
        //                     return_value_set.insert(each_log.kv_params.key.clone());
        //                     max_seen_seq_num = each_log.seq_num;
        //                 }
        //             }
        //         }
        //     }

        //     return_value_list = Vec::from_iter(return_value_set);
        // }

        return_value_list.sort(); //sorts in place
        Ok(storage::List(return_value_list))
    }
}

#[async_trait]
impl storage::KeyList for Lab3BinStoreClient {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        // let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        // let locked_bin_client_index = clone_bin_client_index.lock().await;
        // let curr_bin_client_index = *locked_bin_client_index;
        // std::mem::drop(locked_bin_client_index);

        // let _ = match self.find_next_iter(curr_bin_client_index).await {
        //     Ok(val) => val,
        //     Err(_) => {
        //         // no live backend found, return error
        //         return Err(Box::new(TribblerError::Unknown(
        //             "No live backend found".to_string(),
        //         )));
        //     }
        // };

        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;
        // let result = locked_bin_store_client.list_get(&key).await?;
        // Ok(result)

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        }; // this will work with the current bin_client_index being the hashed value
           // this will update the next alive in the cached bin store client and cached storage client.

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        let mut combined_log: Vec<UpdateLog> = Vec::new();

        //let storage::List(fetched_log) =
        let fetched_log_result: Option<storage::List> =
            match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                Ok(v) => Some(v), // 1st shot, got log
                Err(_) => {
                    // A live backend just crashed; 2nd attempt by iterating through the list to find the next alive

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    let _ = match self.find_next_iter(curr_bin_client_index).await {
                        Ok(val) => val,
                        Err(_) => {
                            // no live backend found, return error
                            return Err(Box::new(TribblerError::Unknown(
                                "No live backend found".to_string(),
                            )));
                        }
                    };

                    let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                    let locked_bin_store_client = clone_bin_store_client.lock().await;
                    let cached_bin_store_client = (*locked_bin_store_client).clone();
                    std::mem::drop(locked_bin_store_client);

                    match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        Ok(v) => Some(v), // got the log
                        Err(_) => {
                            // this should not return error
                            // continue to secondary
                            None
                        }
                    }
                }
            };

        if let Some(storage::List(fetched_log)) = fetched_log_result {
            // regenerate data from log and serve query
            let mut deserialized_log: Vec<UpdateLog> = fetched_log
                .iter()
                .map(|x| serde_json::from_str(&x).unwrap())
                .collect::<Vec<UpdateLog>>();

            // sort the deserialized log as per timestamp
            // keep the fields in the required order so that they are sorted in that same order.
            // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

            combined_log.append(&mut deserialized_log);
        }

        // get from secondary as well
        let secondary_bin_store_client_result: Option<BinStoreClient> =
            match self.find_next_from_live_list().await {
                //let secondary_bin_store_client = match self.find_next_from_live_list().await {
                Ok(val) => Some(val),
                Err(_) => {
                    // iterate all if live backends list not able to give secondary

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    match self
                        .find_next_iter_secondary(curr_bin_client_index + 1)
                        .await
                    {
                        Ok(val) => Some(val),
                        Err(_) => {
                            // no live backend found, return None since a backend exists but doesn't have data
                            None
                        } // do nothing now
                    }
                }
            };

        // secondary bin store client now contains pointer to secondary node

        if let Some(secondary_bin_store_client) = secondary_bin_store_client_result {
            // get log
            let fetched_log_result: Option<storage::List> =
                match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                    Ok(val) => Some(val),
                    Err(_) => {
                        // no log data available
                        // do nothing
                        None
                    }
                }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

            if let Some(storage::List(fetched_log)) = fetched_log_result {
                let mut deserialized_log: Vec<UpdateLog> = fetched_log
                    .iter()
                    .map(|x| serde_json::from_str(&x).unwrap())
                    .collect::<Vec<UpdateLog>>();

                combined_log.append(&mut deserialized_log);
            }
        }

        combined_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        let mut max_seen_seq_num = 0u64;

        let mut return_value_list: Vec<String> = Vec::new();

        for each_log in combined_log {
            if each_log.kv_params.key.eq(key) {
                if each_log.seq_num > max_seen_seq_num {
                    if matches!(each_log.update_operation, UpdateOperation::ListAppend) {
                        return_value_list.push(each_log.kv_params.value.clone());
                        max_seen_seq_num = each_log.seq_num;
                    } else if matches!(each_log.update_operation, UpdateOperation::ListRemove) {
                        return_value_list = return_value_list
                            .iter()
                            .filter(|val| **val != each_log.kv_params.value.clone())
                            .map(String::from)
                            .collect::<Vec<String>>();

                        max_seen_seq_num = each_log.seq_num;
                    }
                }
            }
        }

        // // Logs not found in this primary - a new primary where the migration might not be complete. iterate its live backends list and contact the next live
        // if return_value_list.is_empty() {
        //     // get live backends list
        //     let secondary_bin_store_client = match self.find_next_from_live_list().await {
        //         Ok(val) => val,
        //         Err(_) => {
        //             // iterate all if live backends list not able to give secondary

        //             let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        //             let locked_bin_client_index = clone_bin_client_index.lock().await;
        //             let curr_bin_client_index = *locked_bin_client_index;
        //             std::mem::drop(locked_bin_client_index);

        //             match self
        //                 .find_next_iter_secondary(curr_bin_client_index + 1)
        //                 .await
        //             {
        //                 Ok(val) => val,
        //                 Err(_) => {
        //                     // no live backend found, return None since a backend exists but doesn't have data
        //                     return Ok(storage::List(Vec::new()));
        //                 }
        //             }
        //         }
        //     };

        //     // secondary bin store client now contains pointer to secondary node

        //     // get log
        //     let storage::List(fetched_log) =
        //         match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
        //             Ok(val) => val,
        //             Err(_) => {
        //                 // no log data available
        //                 return Ok(storage::List(Vec::new()));
        //             }
        //         }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        //     // regenerate data from log and serve query
        //     let mut deserialized_log: Vec<UpdateLog> = fetched_log
        //         .iter()
        //         .map(|x| serde_json::from_str(&x).unwrap())
        //         .collect::<Vec<UpdateLog>>();

        //     // sort the deserialized log as per timestamp
        //     // keep the fields in the required order so that they are sorted in that same order.
        //     deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        //     // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        //     // Based on the output of the set operations, generate result for the requested get op and return
        //     // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        //     // just need to keep track of seq_num to avoid considering duplicate ops in log
        //     let mut max_seen_seq_num = 0u64;

        //     for each_log in deserialized_log {
        //         if each_log.kv_params.key.eq(key) {
        //             if each_log.seq_num > max_seen_seq_num {
        //                 if matches!(each_log.update_operation, UpdateOperation::ListAppend) {
        //                     return_value_list.push(each_log.kv_params.value.clone());
        //                     max_seen_seq_num = each_log.seq_num;
        //                 } else if matches!(each_log.update_operation, UpdateOperation::ListRemove) {
        //                     return_value_list = return_value_list
        //                         .iter()
        //                         .filter(|val| **val != each_log.kv_params.value.clone())
        //                         .map(String::from)
        //                         .collect::<Vec<String>>();

        //                     max_seen_seq_num = each_log.seq_num;
        //                 }
        //             }
        //         }
        //     }
        // }

        Ok(storage::List(return_value_list))
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        // let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        // let locked_bin_client_index = clone_bin_client_index.lock().await;
        // let curr_bin_client_index = *locked_bin_client_index;
        // std::mem::drop(locked_bin_client_index);

        // let _ = match self.find_next_iter(curr_bin_client_index).await {
        //     Ok(val) => val,
        //     Err(_) => {
        //         // no live backend found, return error
        //         return Err(Box::new(TribblerError::Unknown(
        //             "No live backend found".to_string(),
        //         )));
        //     }
        // };

        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;
        // let result = locked_bin_store_client.list_append(&kv).await?;
        // Ok(result)

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        };

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        match cached_bin_store_client.clock(0).await {
            Ok(seq_num) => {
                // log::info!("1st success");

                // note the new seq num and continue to list append here itself
                let new_update_log = UpdateLog {
                    seq_num: seq_num, // DONE-TODO: no plus one here
                    update_operation: UpdateOperation::ListAppend,
                    kv_params: KeyValue {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                    },
                };

                // serialize new_update_log
                let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

                let log_append_kv = tribbler::storage::KeyValue {
                    key: KEY_UPDATE_LOG.to_string().clone(),
                    value: new_update_log_serialized,
                };

                // list-append log
                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                let locked_bin_store_client = clone_bin_store_client.lock().await;
                let cached_bin_store_client = (*locked_bin_store_client).clone();
                std::mem::drop(locked_bin_store_client);

                let _ = match cached_bin_store_client.list_append(&log_append_kv).await {
                    Ok(_) => {
                        // clock and primary append is successful
                        // log::info!("2 success");

                        // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                        let primary_list_append_kv = tribbler::storage::KeyValue {
                            key: KEY_PRIMARY_LIST.to_string().clone(),
                            value: self.name.clone(),
                        };

                        let clone_bin_client = Arc::clone(&self.bin_client);
                        let locked_bin_client = clone_bin_client.lock().await;
                        let cached_bin_client = (*locked_bin_client).clone();
                        std::mem::drop(locked_bin_client);

                        let _ = match cached_bin_client.list_append(&primary_list_append_kv).await {
                            Ok(_) => {
                                // clock, primary append and primary list username append all success
                                // append to secondary
                                let secondary_bin_store_client =
                                    match self.find_next_from_live_list().await {
                                        Ok(val) => val,
                                        Err(_) => {
                                            // iterate all if live backends list not able to give secondary

                                            let clone_bin_client_index =
                                                Arc::clone(&self.bin_client_index);
                                            let locked_bin_client_index =
                                                clone_bin_client_index.lock().await;
                                            let curr_bin_client_index = *locked_bin_client_index;
                                            std::mem::drop(locked_bin_client_index);

                                            match self
                                                .find_next_iter_secondary(curr_bin_client_index + 1)
                                                .await
                                            {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // no live backend found, return None since already written to primary
                                                    return Ok(true);
                                                }
                                            }
                                        }
                                    };

                                // list-append log
                                let _ = match secondary_bin_store_client
                                    .list_append(&log_append_kv)
                                    .await
                                {
                                    Ok(_) => {
                                        // secondary append also success
                                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                        let secondary_list_append_kv =
                                            tribbler::storage::KeyValue {
                                                key: KEY_SECONDARY_LIST.to_string().clone(),
                                                value: self.name.clone(),
                                            };

                                        let _ = match secondary_bin_store_client
                                            .bin_client
                                            .list_append(&secondary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {
                                                // all success
                                                return Ok(true);
                                            }
                                            Err(_) => {
                                                //first failure
                                                let secondary_bin_store_client =
                                                    match self.find_next_from_live_list().await {
                                                        Ok(val) => val,
                                                        Err(_) => {
                                                            // iterate all if live backends list not able to give secondary

                                                            let clone_bin_client_index =
                                                                Arc::clone(&self.bin_client_index);
                                                            let locked_bin_client_index =
                                                                clone_bin_client_index.lock().await;
                                                            let curr_bin_client_index =
                                                                *locked_bin_client_index;
                                                            std::mem::drop(locked_bin_client_index);

                                                            match self
                                                                .find_next_iter_secondary(
                                                                    curr_bin_client_index + 1,
                                                                )
                                                                .await
                                                            {
                                                                Ok(val) => val,
                                                                Err(_) => {
                                                                    // no live backend found, return None since already written to primary
                                                                    return Ok(true);
                                                                }
                                                            }
                                                        }
                                                    };

                                                // list-append log
                                                let _ = match secondary_bin_store_client
                                                    .list_append(&log_append_kv)
                                                    .await
                                                {
                                                    Ok(_) => {}
                                                    Err(_) => {}
                                                }; // what if this fails? suppress error

                                                // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                                let secondary_list_append_kv =
                                                    tribbler::storage::KeyValue {
                                                        key: KEY_SECONDARY_LIST.to_string().clone(),
                                                        value: self.name.clone(),
                                                    };

                                                let _ = match secondary_bin_store_client
                                                    .bin_client
                                                    .list_append(&secondary_list_append_kv)
                                                    .await
                                                {
                                                    Ok(_) => {}
                                                    Err(_) => {}
                                                }; // what if this fails? suppress error

                                                return Ok(true);
                                            }
                                        }; // what if this fails? suppress error
                                    }
                                    Err(_) => {
                                        // first failure

                                        let secondary_bin_store_client =
                                            match self.find_next_from_live_list().await {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // iterate all if live backends list not able to give secondary

                                                    let clone_bin_client_index =
                                                        Arc::clone(&self.bin_client_index);
                                                    let locked_bin_client_index =
                                                        clone_bin_client_index.lock().await;
                                                    let curr_bin_client_index =
                                                        *locked_bin_client_index;
                                                    std::mem::drop(locked_bin_client_index);

                                                    match self
                                                        .find_next_iter_secondary(
                                                            curr_bin_client_index + 1,
                                                        )
                                                        .await
                                                    {
                                                        Ok(val) => val,
                                                        Err(_) => {
                                                            // no live backend found, return None since already written to primary
                                                            return Ok(true);
                                                        }
                                                    }
                                                }
                                            };

                                        // list-append log
                                        let _ = match secondary_bin_store_client
                                            .list_append(&log_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                        let secondary_list_append_kv =
                                            tribbler::storage::KeyValue {
                                                key: KEY_SECONDARY_LIST.to_string().clone(),
                                                value: self.name.clone(),
                                            };

                                        let _ = match secondary_bin_store_client
                                            .bin_client
                                            .list_append(&secondary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        return Ok(true);
                                    }
                                }; // what if this fails? suppress error
                            }
                            Err(_) => {
                                // what if this fails? 1st failure in this branch
                                // iterate to get another primary and do the whole process. works because 1 failure already

                                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                let locked_bin_client_index = clone_bin_client_index.lock().await;
                                let curr_bin_client_index = *locked_bin_client_index;
                                std::mem::drop(locked_bin_client_index);

                                let _ = match self.find_next_iter(curr_bin_client_index).await {
                                    Ok(val) => val,
                                    Err(_) => {
                                        // no live backend found, return error
                                        return Err(Box::new(TribblerError::Unknown(
                                            "No live backend found".to_string(),
                                        )));
                                    }
                                };

                                // get the new seq num.
                                // generate corresponding BinStoreClient
                                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                                let locked_bin_store_client = clone_bin_store_client.lock().await;
                                let cached_bin_store_client = (*locked_bin_store_client).clone();
                                std::mem::drop(locked_bin_store_client);

                                match cached_bin_store_client.clock(0).await {
                                    Ok(seq_num) => {
                                        let new_update_log = UpdateLog {
                                            seq_num: seq_num, // DONE-TODO: no plus one here
                                            update_operation: UpdateOperation::ListAppend,
                                            kv_params: KeyValue {
                                                key: kv.key.clone(),
                                                value: kv.value.clone(),
                                            },
                                        };

                                        // serialize new_update_log
                                        let new_update_log_serialized =
                                            serde_json::to_string(&new_update_log)?;

                                        let log_append_kv = tribbler::storage::KeyValue {
                                            key: KEY_UPDATE_LOG.to_string().clone(),
                                            value: new_update_log_serialized,
                                        };

                                        // list-append log
                                        let clone_bin_store_client =
                                            Arc::clone(&self.bin_store_client);
                                        let locked_bin_store_client =
                                            clone_bin_store_client.lock().await;
                                        let cached_bin_store_client =
                                            (*locked_bin_store_client).clone();
                                        std::mem::drop(locked_bin_store_client);

                                        cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                                        // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                                        let primary_list_append_kv = tribbler::storage::KeyValue {
                                            key: KEY_PRIMARY_LIST.to_string().clone(),
                                            value: self.name.clone(),
                                        };

                                        let clone_bin_client = Arc::clone(&self.bin_client);
                                        let locked_bin_client = clone_bin_client.lock().await;
                                        let cached_bin_client = (*locked_bin_client).clone();
                                        std::mem::drop(locked_bin_client);

                                        let _ = match cached_bin_client
                                            .list_append(&primary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? Won't fail bcz 1 failure in this branch

                                        // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.
                                        // won't fail in this branch coz 1 failure

                                        // also append log to secondary - the next in the live backends list
                                        // get live backends list
                                        let secondary_bin_store_client =
                                            match self.find_next_from_live_list().await {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // iterate all if live backends list not able to give secondary

                                                    let clone_bin_client_index =
                                                        Arc::clone(&self.bin_client_index);
                                                    let locked_bin_client_index =
                                                        clone_bin_client_index.lock().await;
                                                    let curr_bin_client_index =
                                                        *locked_bin_client_index;
                                                    std::mem::drop(locked_bin_client_index);

                                                    match self
                                                        .find_next_iter_secondary(
                                                            curr_bin_client_index + 1,
                                                        )
                                                        .await
                                                    {
                                                        Ok(val) => val,
                                                        Err(_) => {
                                                            // no live backend found, return None since already written to primary
                                                            return Ok(true);
                                                        }
                                                    }
                                                }
                                            };

                                        // list-append log
                                        let _ = match secondary_bin_store_client
                                            .list_append(&log_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                        let secondary_list_append_kv =
                                            tribbler::storage::KeyValue {
                                                key: KEY_SECONDARY_LIST.to_string().clone(),
                                                value: self.name.clone(),
                                            };

                                        let _ = match secondary_bin_store_client
                                            .bin_client
                                            .list_append(&secondary_list_append_kv)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }; // what if this fails? suppress error

                                        return Ok(true); // may keep a boolean variable to combine result of all
                                    }
                                    Err(_) => {
                                        // if not found even now, then return error
                                        return Err(Box::new(TribblerError::Unknown(
                                            "Not able to connect".to_string(),
                                        )));
                                    }
                                }
                            }
                        };
                    }
                    Err(_) => {
                        // regenerate clock and start again with one failure in this branch

                        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                        let locked_bin_client_index = clone_bin_client_index.lock().await;
                        let curr_bin_client_index = *locked_bin_client_index;
                        std::mem::drop(locked_bin_client_index);

                        let _ = match self.find_next_iter(curr_bin_client_index).await {
                            Ok(val) => val,
                            Err(_) => {
                                // no live backend found, return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "No live backend found".to_string(),
                                )));
                            }
                        };

                        // get the new seq num.
                        // generate corresponding BinStoreClient
                        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        let locked_bin_store_client = clone_bin_store_client.lock().await;
                        let cached_bin_store_client = (*locked_bin_store_client).clone();
                        std::mem::drop(locked_bin_store_client);

                        match cached_bin_store_client.clock(0).await {
                            Ok(seq_num) => {
                                let new_update_log = UpdateLog {
                                    seq_num: seq_num, // DONE-TODO: no plus one here
                                    update_operation: UpdateOperation::ListAppend,
                                    kv_params: KeyValue {
                                        key: kv.key.clone(),
                                        value: kv.value.clone(),
                                    },
                                };

                                // serialize new_update_log
                                let new_update_log_serialized =
                                    serde_json::to_string(&new_update_log)?;

                                let log_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_UPDATE_LOG.to_string().clone(),
                                    value: new_update_log_serialized,
                                };

                                // list-append log
                                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                                let locked_bin_store_client = clone_bin_store_client.lock().await;
                                let cached_bin_store_client = (*locked_bin_store_client).clone();
                                std::mem::drop(locked_bin_store_client);

                                cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                                // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                                let primary_list_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_PRIMARY_LIST.to_string().clone(),
                                    value: self.name.clone(),
                                };

                                let clone_bin_client = Arc::clone(&self.bin_client);
                                let locked_bin_client = clone_bin_client.lock().await;
                                let cached_bin_client = (*locked_bin_client).clone();
                                std::mem::drop(locked_bin_client);

                                let _ = match cached_bin_client
                                    .list_append(&primary_list_append_kv)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }; // what if this fails? Won't fail bcz 1 failure in this branch

                                // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.
                                // won't fail in this branch coz 1 failure

                                // also append log to secondary - the next in the live backends list
                                // get live backends list
                                let secondary_bin_store_client =
                                    match self.find_next_from_live_list().await {
                                        Ok(val) => val,
                                        Err(_) => {
                                            // iterate all if live backends list not able to give secondary

                                            let clone_bin_client_index =
                                                Arc::clone(&self.bin_client_index);
                                            let locked_bin_client_index =
                                                clone_bin_client_index.lock().await;
                                            let curr_bin_client_index = *locked_bin_client_index;
                                            std::mem::drop(locked_bin_client_index);
                                            match self
                                                .find_next_iter_secondary(curr_bin_client_index + 1)
                                                .await
                                            {
                                                Ok(val) => val,
                                                Err(_) => {
                                                    // no live backend found, return None since already written to primary
                                                    return Ok(true);
                                                }
                                            }
                                        }
                                    };

                                // list-append log
                                let _ = match secondary_bin_store_client
                                    .list_append(&log_append_kv)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }; // what if this fails? suppress error

                                // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                                let secondary_list_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_SECONDARY_LIST.to_string().clone(),
                                    value: self.name.clone(),
                                };

                                let _ = match secondary_bin_store_client
                                    .bin_client
                                    .list_append(&secondary_list_append_kv)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }; // what if this fails? suppress error

                                return Ok(true); // may keep a boolean variable to combine result of all
                            }
                            Err(_) => {
                                // if not found even now, then return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "Not able to connect".to_string(),
                                )));
                            }
                        }
                    }
                }; // This won't fail as already one failure there in this branch
            }
            Err(_) => {
                // error: it just crashed. then find next alive node

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index;
                std::mem::drop(locked_bin_client_index);

                let _ = match self.find_next_iter(curr_bin_client_index).await {
                    Ok(val) => val,
                    Err(_) => {
                        // no live backend found, return error
                        return Err(Box::new(TribblerError::Unknown(
                            "No live backend found".to_string(),
                        )));
                    }
                };

                // get the new seq num.
                // generate corresponding BinStoreClient
                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                let locked_bin_store_client = clone_bin_store_client.lock().await;
                let cached_bin_store_client = (*locked_bin_store_client).clone();
                std::mem::drop(locked_bin_store_client);

                match cached_bin_store_client.clock(0).await {
                    Ok(seq_num) => {
                        let new_update_log = UpdateLog {
                            seq_num: seq_num, // DONE-TODO: no plus one here
                            update_operation: UpdateOperation::ListAppend,
                            kv_params: KeyValue {
                                key: kv.key.clone(),
                                value: kv.value.clone(),
                            },
                        };

                        // serialize new_update_log
                        let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

                        let log_append_kv = tribbler::storage::KeyValue {
                            key: KEY_UPDATE_LOG.to_string().clone(),
                            value: new_update_log_serialized,
                        };

                        // list-append log
                        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        let locked_bin_store_client = clone_bin_store_client.lock().await;
                        let cached_bin_store_client = (*locked_bin_store_client).clone();
                        std::mem::drop(locked_bin_store_client);

                        cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                        // add this bin to primary list of the node - SHOULD this be before appending the log? NO
                        let primary_list_append_kv = tribbler::storage::KeyValue {
                            key: KEY_PRIMARY_LIST.to_string().clone(),
                            value: self.name.clone(),
                        };

                        let clone_bin_client = Arc::clone(&self.bin_client);
                        let locked_bin_client = clone_bin_client.lock().await;
                        let cached_bin_client = (*locked_bin_client).clone();
                        std::mem::drop(locked_bin_client);

                        let _ = match cached_bin_client.list_append(&primary_list_append_kv).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }; // what if this fails? Won't fail bcz 1 failure in this branch

                        // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.
                        // won't fail in this branch coz 1 failure

                        // also append log to secondary - the next in the live backends list
                        // get live backends list
                        let secondary_bin_store_client = match self.find_next_from_live_list().await
                        {
                            Ok(val) => val,
                            Err(_) => {
                                // iterate all if live backends list not able to give secondary

                                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                let locked_bin_client_index = clone_bin_client_index.lock().await;
                                let curr_bin_client_index = *locked_bin_client_index;
                                std::mem::drop(locked_bin_client_index);
                                match self
                                    .find_next_iter_secondary(curr_bin_client_index + 1)
                                    .await
                                {
                                    Ok(val) => val,
                                    Err(_) => {
                                        // no live backend found, return None since already written to primary
                                        return Ok(true);
                                    }
                                }
                            }
                        };

                        // list-append log
                        let _ = match secondary_bin_store_client.list_append(&log_append_kv).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }; // what if this fails? suppress error

                        // add this bin to secondary list of the node - SHOULD this be before appending the log? NO
                        let secondary_list_append_kv = tribbler::storage::KeyValue {
                            key: KEY_SECONDARY_LIST.to_string().clone(),
                            value: self.name.clone(),
                        };

                        let _ = match secondary_bin_store_client
                            .bin_client
                            .list_append(&secondary_list_append_kv)
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }; // what if this fails? suppress error

                        return Ok(true); // may keep a boolean variable to combine result of all
                    }
                    Err(_) => {
                        // if not found even now, then return error
                        return Err(Box::new(TribblerError::Unknown(
                            "Not able to connect".to_string(),
                        )));
                    }
                }
            }
        };
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        // let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        // let locked_bin_client_index = clone_bin_client_index.lock().await;
        // let curr_bin_client_index = *locked_bin_client_index;
        // std::mem::drop(locked_bin_client_index);

        // let _ = match self.find_next_iter(curr_bin_client_index).await {
        //     Ok(val) => val,
        //     Err(_) => {
        //         // no live backend found, return error
        //         return Err(Box::new(TribblerError::Unknown(
        //             "No live backend found".to_string(),
        //         )));
        //     }
        // };

        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;
        // let result = locked_bin_store_client.list_remove(&kv).await?;
        // Ok(result)

        let mut global_seq_num = 0u64;
        let mut combined_log: Vec<UpdateLog> = Vec::new();

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        };

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        let remove_result: TribResult<u32> = match cached_bin_store_client.clock(0).await {
            Ok(seq_num) => {
                // log::info!("1st success");

                // note the new seq num and continue to list append here itself
                let new_update_log = UpdateLog {
                    seq_num: seq_num, // DONE-TODO: no plus one here
                    update_operation: UpdateOperation::ListRemove,
                    kv_params: KeyValue {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                    },
                };

                global_seq_num = seq_num;

                // serialize new_update_log
                let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

                let log_append_kv = tribbler::storage::KeyValue {
                    key: KEY_UPDATE_LOG.to_string().clone(),
                    value: new_update_log_serialized,
                };

                // list-append log
                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                let locked_bin_store_client = clone_bin_store_client.lock().await;
                let cached_bin_store_client = (*locked_bin_store_client).clone();
                std::mem::drop(locked_bin_store_client);

                let _ = match cached_bin_store_client.list_append(&log_append_kv).await {
                    Ok(_) => {
                        // clock and primary append is successful
                        // log::info!("2 success");

                        // let fetched_log_result: Option<storage::List> =
                        //     match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        //         Ok(v) => Some(v), // 1st shot, got log
                        //         Err(_) => {
                        //             // A live backend just crashed; 2nd attempt by iterating through the list to find the next alive

                        //             let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                        //             let locked_bin_client_index =
                        //                 clone_bin_client_index.lock().await;
                        //             let curr_bin_client_index = *locked_bin_client_index;
                        //             std::mem::drop(locked_bin_client_index);

                        //             let _ = match self.find_next_iter(curr_bin_client_index).await {
                        //                 Ok(val) => val,
                        //                 Err(_) => {
                        //                     // no live backend found, return error
                        //                     return Err(Box::new(TribblerError::Unknown(
                        //                         "No live backend found".to_string(),
                        //                     )));
                        //                 }
                        //             };

                        //             let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        //             let locked_bin_store_client =
                        //                 clone_bin_store_client.lock().await;
                        //             let cached_bin_store_client =
                        //                 (*locked_bin_store_client).clone();
                        //             std::mem::drop(locked_bin_store_client);

                        //             match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        //                 Ok(v) => Some(v), // got the log
                        //                 Err(_) => {
                        //                     // this should not return error
                        //                     // continue to secondary
                        //                     None
                        //                 }
                        //             }
                        //         }
                        //     };

                        // if let Some(storage::List(fetched_log)) = fetched_log_result {
                        //     // regenerate data from log and serve query
                        //     let mut deserialized_log: Vec<UpdateLog> = fetched_log
                        //         .iter()
                        //         .map(|x| serde_json::from_str(&x).unwrap())
                        //         .collect::<Vec<UpdateLog>>();

                        //     // sort the deserialized log as per timestamp
                        //     // keep the fields in the required order so that they are sorted in that same order.
                        //     // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                        //     combined_log.append(&mut deserialized_log);
                        // }

                        // // get from secondary as well
                        // let secondary_bin_store_client_result: Option<BinStoreClient> =
                        //     match self.find_next_from_live_list().await {
                        //         //let secondary_bin_store_client = match self.find_next_from_live_list().await {
                        //         Ok(val) => Some(val),
                        //         Err(_) => {
                        //             // iterate all if live backends list not able to give secondary

                        //             let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                        //             let locked_bin_client_index =
                        //                 clone_bin_client_index.lock().await;
                        //             let curr_bin_client_index = *locked_bin_client_index;
                        //             std::mem::drop(locked_bin_client_index);

                        //             match self
                        //                 .find_next_iter_secondary(curr_bin_client_index + 1)
                        //                 .await
                        //             {
                        //                 Ok(val) => Some(val),
                        //                 Err(_) => {
                        //                     // no live backend found, return None since a backend exists but doesn't have data
                        //                     None
                        //                 } // do nothing now
                        //             }
                        //         }
                        //     };

                        // // secondary bin store client now contains pointer to secondary node

                        // if let Some(secondary_bin_store_client) = secondary_bin_store_client_result
                        // {
                        //     // get log
                        //     let fetched_log_result: Option<storage::List> =
                        //         match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        //             Ok(val) => Some(val),
                        //             Err(_) => {
                        //                 // no log data available
                        //                 // do nothing
                        //                 None
                        //             }
                        //         }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

                        //     if let Some(storage::List(fetched_log)) = fetched_log_result {
                        //         let mut deserialized_log: Vec<UpdateLog> = fetched_log
                        //             .iter()
                        //             .map(|x| serde_json::from_str(&x).unwrap())
                        //             .collect::<Vec<UpdateLog>>();

                        //         combined_log.append(&mut deserialized_log);
                        //     }
                        // }

                        // ***********************************************
                        // fetch log instead of primary list append
                        let _ = match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                            Ok(storage::List(fetched_log)) => {
                                // if successful, store the log and append operation to secondary
                                let mut deserialized_log: Vec<UpdateLog> = fetched_log
                                    .iter()
                                    .map(|x| serde_json::from_str(&x).unwrap())
                                    .collect::<Vec<UpdateLog>>();

                                // sort the deserialized log as per timestamp
                                // keep the fields in the required order so that they are sorted in that same order.
                                // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                combined_log.append(&mut deserialized_log);

                                // append op to secondary
                                let secondary_bin_store_client_result =
                                    match self.find_next_from_live_list().await {
                                        Ok(val) => Some(val),
                                        Err(_) => {
                                            // iterate all if live backends list not able to give secondary

                                            let clone_bin_client_index =
                                                Arc::clone(&self.bin_client_index);
                                            let locked_bin_client_index =
                                                clone_bin_client_index.lock().await;
                                            let curr_bin_client_index = *locked_bin_client_index;
                                            std::mem::drop(locked_bin_client_index);

                                            match self
                                                .find_next_iter_secondary(curr_bin_client_index + 1)
                                                .await
                                            {
                                                Ok(val) => Some(val),
                                                Err(_) => {
                                                    // no live backend found, return None since already written to primary
                                                    None
                                                }
                                            }
                                        }
                                    };

                                if let Some(secondary_bin_store_client) =
                                    secondary_bin_store_client_result
                                {
                                    // list-append log
                                    let _ = match secondary_bin_store_client
                                        .list_append(&log_append_kv)
                                        .await
                                    {
                                        Ok(_) => {
                                            // secondary append also success

                                            // get log
                                            let _ = match secondary_bin_store_client
                                                .list_get(KEY_UPDATE_LOG)
                                                .await
                                            {
                                                Ok(storage::List(fetched_log)) => {
                                                    // all success
                                                    let mut deserialized_log: Vec<UpdateLog> =
                                                        fetched_log
                                                            .iter()
                                                            .map(|x| {
                                                                serde_json::from_str(&x).unwrap()
                                                            })
                                                            .collect::<Vec<UpdateLog>>();

                                                    // sort the deserialized log as per timestamp
                                                    // keep the fields in the required order so that they are sorted in that same order.
                                                    // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                                    combined_log.append(&mut deserialized_log);

                                                    //Ok(0)
                                                }
                                                Err(_) => {
                                                    //first failure
                                                    // try to find another secondary and write log and get log
                                                    let secondary_bin_store_client_result =
                                                        match self.find_next_from_live_list().await
                                                        {
                                                            Ok(val) => Some(val),
                                                            Err(_) => {
                                                                // iterate all if live backends list not able to give secondary

                                                                let clone_bin_client_index =
                                                                    Arc::clone(
                                                                        &self.bin_client_index,
                                                                    );
                                                                let locked_bin_client_index =
                                                                    clone_bin_client_index
                                                                        .lock()
                                                                        .await;
                                                                let curr_bin_client_index =
                                                                    *locked_bin_client_index;
                                                                std::mem::drop(
                                                                    locked_bin_client_index,
                                                                );

                                                                match self
                                                                    .find_next_iter_secondary(
                                                                        curr_bin_client_index + 1,
                                                                    )
                                                                    .await
                                                                {
                                                                    Ok(val) => Some(val),
                                                                    Err(_) => {
                                                                        // no live backend found, return None since already written to primary
                                                                        None
                                                                    }
                                                                }
                                                            }
                                                        };

                                                    if let Some(secondary_bin_store_client) =
                                                        secondary_bin_store_client_result
                                                    {
                                                        // list-append log
                                                        let _ = match secondary_bin_store_client
                                                            .list_append(&log_append_kv)
                                                            .await
                                                        {
                                                            Ok(_) => {}
                                                            Err(_) => {}
                                                        };

                                                        // get log
                                                        let _ = match secondary_bin_store_client
                                                            .list_get(KEY_UPDATE_LOG)
                                                            .await
                                                        {
                                                            Ok(storage::List(fetched_log)) => {
                                                                // all success
                                                                let mut deserialized_log: Vec<
                                                                    UpdateLog,
                                                                > = fetched_log
                                                                    .iter()
                                                                    .map(|x| {
                                                                        serde_json::from_str(&x)
                                                                            .unwrap()
                                                                    })
                                                                    .collect::<Vec<UpdateLog>>();

                                                                // sort the deserialized log as per timestamp
                                                                // keep the fields in the required order so that they are sorted in that same order.
                                                                // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                                                combined_log
                                                                    .append(&mut deserialized_log);

                                                                //Ok(0)
                                                            }
                                                            Err(_) => {}
                                                        };
                                                    };
                                                    //Ok(0)
                                                }
                                            }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive
                                               // what if this fails? suppress error
                                        }
                                        Err(_) => {
                                            // first failure

                                            let secondary_bin_store_client_result =
                                                match self.find_next_from_live_list().await {
                                                    Ok(val) => Some(val),
                                                    Err(_) => {
                                                        // iterate all if live backends list not able to give secondary

                                                        let clone_bin_client_index =
                                                            Arc::clone(&self.bin_client_index);
                                                        let locked_bin_client_index =
                                                            clone_bin_client_index.lock().await;
                                                        let curr_bin_client_index =
                                                            *locked_bin_client_index;
                                                        std::mem::drop(locked_bin_client_index);

                                                        match self
                                                            .find_next_iter_secondary(
                                                                curr_bin_client_index + 1,
                                                            )
                                                            .await
                                                        {
                                                            Ok(val) => Some(val),
                                                            Err(_) => {
                                                                // no live backend found, return None since already written to primary
                                                                None
                                                            }
                                                        }
                                                    }
                                                };

                                            if let Some(secondary_bin_store_client) =
                                                secondary_bin_store_client_result
                                            {
                                                let _ = match secondary_bin_store_client
                                                    .list_append(&log_append_kv)
                                                    .await
                                                {
                                                    Ok(_) => {}
                                                    Err(_) => {}
                                                }; // what if this fails? suppress error
                                            }

                                            // get log
                                            let _ = match secondary_bin_store_client
                                                .list_get(KEY_UPDATE_LOG)
                                                .await
                                            {
                                                Ok(storage::List(fetched_log)) => {
                                                    // all success
                                                    let mut deserialized_log: Vec<UpdateLog> =
                                                        fetched_log
                                                            .iter()
                                                            .map(|x| {
                                                                serde_json::from_str(&x).unwrap()
                                                            })
                                                            .collect::<Vec<UpdateLog>>();

                                                    // sort the deserialized log as per timestamp
                                                    // keep the fields in the required order so that they are sorted in that same order.
                                                    // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                                    combined_log.append(&mut deserialized_log);

                                                    //Ok(0)
                                                }
                                                Err(_) => {}
                                            };

                                            //return Ok(true);
                                        }
                                    };
                                }
                            }
                            Err(_) => {
                                // what if this fails? 1st failure in this branch
                                // iterate to get another primary and do the whole process. works because 1 failure already

                                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                let locked_bin_client_index = clone_bin_client_index.lock().await;
                                let curr_bin_client_index = *locked_bin_client_index;
                                std::mem::drop(locked_bin_client_index);

                                let _ = match self.find_next_iter(curr_bin_client_index).await {
                                    Ok(val) => val,
                                    Err(_) => {
                                        // no live backend found, return error
                                        return Err(Box::new(TribblerError::Unknown(
                                            "No live backend found".to_string(),
                                        )));
                                    }
                                };

                                // get the new seq num.
                                // generate corresponding BinStoreClient
                                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                                let locked_bin_store_client = clone_bin_store_client.lock().await;
                                let cached_bin_store_client = (*locked_bin_store_client).clone();
                                std::mem::drop(locked_bin_store_client);

                                match cached_bin_store_client.clock(0).await {
                                    Ok(seq_num) => {
                                        let new_update_log = UpdateLog {
                                            seq_num: seq_num, // DONE-TODO: no plus one here
                                            update_operation: UpdateOperation::ListRemove,
                                            kv_params: KeyValue {
                                                key: kv.key.clone(),
                                                value: kv.value.clone(),
                                            },
                                        };

                                        global_seq_num = seq_num;

                                        // serialize new_update_log
                                        let new_update_log_serialized =
                                            serde_json::to_string(&new_update_log)?;

                                        let log_append_kv = tribbler::storage::KeyValue {
                                            key: KEY_UPDATE_LOG.to_string().clone(),
                                            value: new_update_log_serialized,
                                        };

                                        // list-append log
                                        let clone_bin_store_client =
                                            Arc::clone(&self.bin_store_client);
                                        let locked_bin_store_client =
                                            clone_bin_store_client.lock().await;
                                        let cached_bin_store_client =
                                            (*locked_bin_store_client).clone();
                                        std::mem::drop(locked_bin_store_client);

                                        cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                                        let _ = match cached_bin_store_client
                                            .list_get(KEY_UPDATE_LOG)
                                            .await
                                        {
                                            Ok(storage::List(fetched_log)) => {
                                                // if successful, store the log and append operation to secondary
                                                let mut deserialized_log: Vec<UpdateLog> =
                                                    fetched_log
                                                        .iter()
                                                        .map(|x| serde_json::from_str(&x).unwrap())
                                                        .collect::<Vec<UpdateLog>>();

                                                // sort the deserialized log as per timestamp
                                                // keep the fields in the required order so that they are sorted in that same order.
                                                // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                                combined_log.append(&mut deserialized_log);
                                            }
                                            Err(_) => {} //won't fail coz 1 failure
                                        };

                                        let secondary_bin_store_client_result =
                                            match self.find_next_from_live_list().await {
                                                Ok(val) => Some(val),
                                                Err(_) => {
                                                    // iterate all if live backends list not able to give secondary

                                                    let clone_bin_client_index =
                                                        Arc::clone(&self.bin_client_index);
                                                    let locked_bin_client_index =
                                                        clone_bin_client_index.lock().await;
                                                    let curr_bin_client_index =
                                                        *locked_bin_client_index;
                                                    std::mem::drop(locked_bin_client_index);

                                                    match self
                                                        .find_next_iter_secondary(
                                                            curr_bin_client_index + 1,
                                                        )
                                                        .await
                                                    {
                                                        Ok(val) => Some(val),
                                                        Err(_) => {
                                                            // no live backend found, return None since already written to primary
                                                            None
                                                        }
                                                    }
                                                }
                                            };

                                        if let Some(secondary_bin_store_client) =
                                            secondary_bin_store_client_result
                                        {
                                            let _ = match secondary_bin_store_client
                                                .list_append(&log_append_kv)
                                                .await
                                            {
                                                Ok(_) => {}
                                                Err(_) => {}
                                            }; // what if this fails? suppress error

                                            let _ = match secondary_bin_store_client
                                                .list_get(KEY_UPDATE_LOG)
                                                .await
                                            {
                                                Ok(storage::List(fetched_log)) => {
                                                    // all success
                                                    let mut deserialized_log: Vec<UpdateLog> =
                                                        fetched_log
                                                            .iter()
                                                            .map(|x| {
                                                                serde_json::from_str(&x).unwrap()
                                                            })
                                                            .collect::<Vec<UpdateLog>>();

                                                    // sort the deserialized log as per timestamp
                                                    // keep the fields in the required order so that they are sorted in that same order.
                                                    // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                                    combined_log.append(&mut deserialized_log);

                                                    //Ok(0)
                                                }
                                                Err(_) => {}
                                            };
                                        }

                                        //return Ok(true); // may keep a boolean variable to combine result of all
                                    }
                                    Err(_) => {
                                        // if not found even now, then return error
                                        // return Err(Box::new(TribblerError::Unknown(
                                        //     "Not able to connect".to_string(),
                                        // )));
                                    }
                                }
                            }
                        };
                    }
                    Err(_) => {
                        // regenerate clock and start again with one failure in this branch

                        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                        let locked_bin_client_index = clone_bin_client_index.lock().await;
                        let curr_bin_client_index = *locked_bin_client_index;
                        std::mem::drop(locked_bin_client_index);

                        let _ = match self.find_next_iter(curr_bin_client_index).await {
                            Ok(val) => val,
                            Err(_) => {
                                // no live backend found, return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "No live backend found".to_string(),
                                )));
                            }
                        };

                        // get the new seq num.
                        // generate corresponding BinStoreClient
                        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        let locked_bin_store_client = clone_bin_store_client.lock().await;
                        let cached_bin_store_client = (*locked_bin_store_client).clone();
                        std::mem::drop(locked_bin_store_client);

                        match cached_bin_store_client.clock(0).await {
                            Ok(seq_num) => {
                                let new_update_log = UpdateLog {
                                    seq_num: seq_num, // DONE-TODO: no plus one here
                                    update_operation: UpdateOperation::ListRemove,
                                    kv_params: KeyValue {
                                        key: kv.key.clone(),
                                        value: kv.value.clone(),
                                    },
                                };

                                global_seq_num = seq_num;

                                // serialize new_update_log
                                let new_update_log_serialized =
                                    serde_json::to_string(&new_update_log)?;

                                let log_append_kv = tribbler::storage::KeyValue {
                                    key: KEY_UPDATE_LOG.to_string().clone(),
                                    value: new_update_log_serialized,
                                };

                                // list-append log
                                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                                let locked_bin_store_client = clone_bin_store_client.lock().await;
                                let cached_bin_store_client = (*locked_bin_store_client).clone();
                                std::mem::drop(locked_bin_store_client);

                                cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                                let _ = match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await
                                {
                                    Ok(storage::List(fetched_log)) => {
                                        // if successful, store the log and append operation to secondary
                                        let mut deserialized_log: Vec<UpdateLog> = fetched_log
                                            .iter()
                                            .map(|x| serde_json::from_str(&x).unwrap())
                                            .collect::<Vec<UpdateLog>>();

                                        // sort the deserialized log as per timestamp
                                        // keep the fields in the required order so that they are sorted in that same order.
                                        // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                        combined_log.append(&mut deserialized_log);
                                    }
                                    Err(_) => {} //won't fail coz 1 failure
                                };

                                let secondary_bin_store_client_result =
                                    match self.find_next_from_live_list().await {
                                        Ok(val) => Some(val),
                                        Err(_) => {
                                            // iterate all if live backends list not able to give secondary

                                            let clone_bin_client_index =
                                                Arc::clone(&self.bin_client_index);
                                            let locked_bin_client_index =
                                                clone_bin_client_index.lock().await;
                                            let curr_bin_client_index = *locked_bin_client_index;
                                            std::mem::drop(locked_bin_client_index);

                                            match self
                                                .find_next_iter_secondary(curr_bin_client_index + 1)
                                                .await
                                            {
                                                Ok(val) => Some(val),
                                                Err(_) => {
                                                    // no live backend found, return None since already written to primary
                                                    None
                                                }
                                            }
                                        }
                                    };

                                if let Some(secondary_bin_store_client) =
                                    secondary_bin_store_client_result
                                {
                                    let _ = match secondary_bin_store_client
                                        .list_append(&log_append_kv)
                                        .await
                                    {
                                        Ok(_) => {}
                                        Err(_) => {}
                                    }; // what if this fails? suppress error

                                    let _ = match secondary_bin_store_client
                                        .list_get(KEY_UPDATE_LOG)
                                        .await
                                    {
                                        Ok(storage::List(fetched_log)) => {
                                            // all success
                                            let mut deserialized_log: Vec<UpdateLog> = fetched_log
                                                .iter()
                                                .map(|x| serde_json::from_str(&x).unwrap())
                                                .collect::<Vec<UpdateLog>>();

                                            // sort the deserialized log as per timestamp
                                            // keep the fields in the required order so that they are sorted in that same order.
                                            // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                            combined_log.append(&mut deserialized_log);

                                            //Ok(0)
                                        }
                                        Err(_) => {}
                                    };
                                }
                                //return Ok(true)
                            }
                            Err(_) => {
                                // if not found even now, then return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "Not able to connect".to_string(),
                                )));
                            }
                        }
                    }
                }; // This won't fail as already one failure there in this branch

                Ok(0)
            }
            Err(_) => {
                // error: it just crashed. then find next alive node

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index;
                std::mem::drop(locked_bin_client_index);

                let _ = match self.find_next_iter(curr_bin_client_index).await {
                    Ok(val) => val,
                    Err(_) => {
                        // no live backend found, return error
                        return Err(Box::new(TribblerError::Unknown(
                            "No live backend found".to_string(),
                        )));
                    }
                };

                // get the new seq num.
                // generate corresponding BinStoreClient
                let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                let locked_bin_store_client = clone_bin_store_client.lock().await;
                let cached_bin_store_client = (*locked_bin_store_client).clone();
                std::mem::drop(locked_bin_store_client);

                match cached_bin_store_client.clock(0).await {
                    Ok(seq_num) => {
                        let new_update_log = UpdateLog {
                            seq_num: seq_num, // DONE-TODO: no plus one here
                            update_operation: UpdateOperation::ListRemove,
                            kv_params: KeyValue {
                                key: kv.key.clone(),
                                value: kv.value.clone(),
                            },
                        };

                        global_seq_num = seq_num; // u64 is automatically copied

                        // serialize new_update_log
                        let new_update_log_serialized = serde_json::to_string(&new_update_log)?;

                        let log_append_kv = tribbler::storage::KeyValue {
                            key: KEY_UPDATE_LOG.to_string().clone(),
                            value: new_update_log_serialized,
                        };

                        // list-append log
                        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                        let locked_bin_store_client = clone_bin_store_client.lock().await;
                        let cached_bin_store_client = (*locked_bin_store_client).clone();
                        std::mem::drop(locked_bin_store_client);

                        cached_bin_store_client.list_append(&log_append_kv).await?; // This won't fail as already one failure there in this branch

                        let _ = match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                            Ok(storage::List(fetched_log)) => {
                                // if successful, store the log and append operation to secondary
                                let mut deserialized_log: Vec<UpdateLog> = fetched_log
                                    .iter()
                                    .map(|x| serde_json::from_str(&x).unwrap())
                                    .collect::<Vec<UpdateLog>>();

                                // sort the deserialized log as per timestamp
                                // keep the fields in the required order so that they are sorted in that same order.
                                // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                combined_log.append(&mut deserialized_log);
                            }
                            Err(_) => {}
                        };

                        // also append log list remove to secondary - the next in the live backends list
                        let secondary_bin_store_client_result =
                            match self.find_next_from_live_list().await {
                                Ok(val) => Some(val),
                                Err(_) => {
                                    // iterate all if live backends list not able to give secondary

                                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                    let locked_bin_client_index =
                                        clone_bin_client_index.lock().await;
                                    let curr_bin_client_index = *locked_bin_client_index;
                                    std::mem::drop(locked_bin_client_index);

                                    match self
                                        .find_next_iter_secondary(curr_bin_client_index + 1)
                                        .await
                                    {
                                        Ok(val) => Some(val),
                                        Err(_) => {
                                            // no live backend found, return None since already written to primary
                                            None
                                        }
                                    }
                                }
                            };

                        if let Some(secondary_bin_store_client) = secondary_bin_store_client_result
                        {
                            let _ = match secondary_bin_store_client
                                .list_append(&log_append_kv)
                                .await
                            {
                                Ok(_) => {}
                                Err(_) => {}
                            }; // what if this fails? suppress error

                            let _ = match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await
                            {
                                Ok(storage::List(fetched_log)) => {
                                    // all success
                                    let mut deserialized_log: Vec<UpdateLog> = fetched_log
                                        .iter()
                                        .map(|x| serde_json::from_str(&x).unwrap())
                                        .collect::<Vec<UpdateLog>>();

                                    // sort the deserialized log as per timestamp
                                    // keep the fields in the required order so that they are sorted in that same order.
                                    // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                                    combined_log.append(&mut deserialized_log);

                                    //Ok(0)
                                }
                                Err(_) => {}
                            };
                        }

                        //Ok(0) // may keep a boolean variable to combine result of all
                    }
                    Err(_) => {
                        // if not found even now, then return error
                        return Err(Box::new(TribblerError::Unknown(
                            "Not able to connect".to_string(),
                        )));
                    }
                }
                Ok(0)
            }
        };

        // we have the logs and the seq_num

        // sort the logs
        combined_log.sort();

        combined_log.reverse();
        // reverse iterate and count removes from remove at curr seq_num to next remove op
        let mut start_counting = false;
        let mut remove_count = 0;

        let mut min_seq_num_till_now = global_seq_num;

        for each_log in combined_log {
            if each_log.seq_num == global_seq_num {
                start_counting = true;
            }

            if start_counting {
                if each_log.kv_params.key.clone().eq(kv.key.clone().as_str())
                    && each_log
                        .kv_params
                        .value
                        .clone()
                        .eq(kv.value.clone().as_str())
                {
                    if each_log.seq_num < min_seq_num_till_now {
                        if matches!(each_log.update_operation, UpdateOperation::ListAppend) {
                            // log::info!("{}", each_log.seq_num);
                            remove_count += 1;
                            min_seq_num_till_now = each_log.seq_num;
                        } else if matches!(each_log.update_operation, UpdateOperation::ListRemove) {
                            break;
                        }
                    }
                }
            }
        }

        return Ok(remove_count);
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        // let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        // let locked_bin_client_index = clone_bin_client_index.lock().await;
        // let curr_bin_client_index = *locked_bin_client_index;
        // std::mem::drop(locked_bin_client_index);

        // let _ = match self.find_next_iter(curr_bin_client_index).await {
        //     Ok(val) => val,
        //     Err(_) => {
        //         // no live backend found, return error
        //         return Err(Box::new(TribblerError::Unknown(
        //             "No live backend found".to_string(),
        //         )));
        //     }
        // };

        // let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        // let locked_bin_store_client = clone_bin_store_client.lock().await;
        // let result = locked_bin_store_client.list_keys(&p).await?;
        // Ok(result)

        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        }; // this will work with the current bin_client_index being the hashed value
           // this will update the next alive in the cached bin store client and cached storage client.

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let cached_bin_store_client = (*locked_bin_store_client).clone();
        std::mem::drop(locked_bin_store_client);

        let mut combined_log: Vec<UpdateLog> = Vec::new();

        //let storage::List(fetched_log) =
        let fetched_log_result: Option<storage::List> =
            match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                Ok(v) => Some(v), // 1st shot, got log
                Err(_) => {
                    // A live backend just crashed; 2nd attempt by iterating through the list to find the next alive

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    let _ = match self.find_next_iter(curr_bin_client_index).await {
                        Ok(val) => val,
                        Err(_) => {
                            // no live backend found, return error
                            return Err(Box::new(TribblerError::Unknown(
                                "No live backend found".to_string(),
                            )));
                        }
                    };

                    let clone_bin_store_client = Arc::clone(&self.bin_store_client);
                    let locked_bin_store_client = clone_bin_store_client.lock().await;
                    let cached_bin_store_client = (*locked_bin_store_client).clone();
                    std::mem::drop(locked_bin_store_client);

                    match cached_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                        Ok(v) => Some(v), // got the log
                        Err(_) => {
                            // this should not return error
                            // continue to secondary
                            None
                        }
                    }
                }
            };

        if let Some(storage::List(fetched_log)) = fetched_log_result {
            // regenerate data from log and serve query
            let mut deserialized_log: Vec<UpdateLog> = fetched_log
                .iter()
                .map(|x| serde_json::from_str(&x).unwrap())
                .collect::<Vec<UpdateLog>>();

            // sort the deserialized log as per timestamp
            // keep the fields in the required order so that they are sorted in that same order.
            // deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

            combined_log.append(&mut deserialized_log);
        }

        // get from secondary as well
        let secondary_bin_store_client_result: Option<BinStoreClient> =
            match self.find_next_from_live_list().await {
                //let secondary_bin_store_client = match self.find_next_from_live_list().await {
                Ok(val) => Some(val),
                Err(_) => {
                    // iterate all if live backends list not able to give secondary

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;
                    std::mem::drop(locked_bin_client_index);

                    match self
                        .find_next_iter_secondary(curr_bin_client_index + 1)
                        .await
                    {
                        Ok(val) => Some(val),
                        Err(_) => {
                            // no live backend found, return None since a backend exists but doesn't have data
                            None
                        } // do nothing now
                    }
                }
            };

        // secondary bin store client now contains pointer to secondary node

        if let Some(secondary_bin_store_client) = secondary_bin_store_client_result {
            // get log
            let fetched_log_result: Option<storage::List> =
                match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
                    Ok(val) => Some(val),
                    Err(_) => {
                        // no log data available
                        // do nothing
                        None
                    }
                }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

            if let Some(storage::List(fetched_log)) = fetched_log_result {
                let mut deserialized_log: Vec<UpdateLog> = fetched_log
                    .iter()
                    .map(|x| serde_json::from_str(&x).unwrap())
                    .collect::<Vec<UpdateLog>>();

                combined_log.append(&mut deserialized_log);
            }
        }

        combined_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        let mut max_seen_seq_num = 0u64;

        // need a HashMap of (list_name, list_items: Vec<String>)
        let mut list_map: HashMap<String, Vec<String>> = HashMap::new();
        // entry api, list_map, insert if not exits function
        for each_log in combined_log {
            if matches!(each_log.update_operation, UpdateOperation::ListAppend) {
                if each_log.kv_params.key.starts_with(&p.prefix)
                    && each_log.kv_params.key.ends_with(&p.suffix)
                {
                    if each_log.seq_num > max_seen_seq_num {
                        //return_value_set.insert(each_log.kv_params.key.clone());
                        // get the list for given list name
                        let get_list_ref_given_name = list_map
                            .entry(each_log.kv_params.key.clone())
                            .or_insert(Vec::<String>::new());
                        get_list_ref_given_name.push(each_log.kv_params.value.clone());

                        max_seen_seq_num = each_log.seq_num;
                    }
                }
            } else if matches!(each_log.update_operation, UpdateOperation::ListRemove) {
                if each_log.kv_params.key.starts_with(&p.prefix)
                    && each_log.kv_params.key.ends_with(&p.suffix)
                {
                    if each_log.seq_num > max_seen_seq_num {
                        // return_value_set.insert(each_log.kv_params.key.clone());

                        // get list for given list name
                        let get_list_ref_given_name = list_map
                            .entry(each_log.kv_params.key.clone())
                            .or_insert(Vec::<String>::new());

                        // iterate through the list and remove all values matching given value.
                        *get_list_ref_given_name = get_list_ref_given_name
                            .iter()
                            .filter(|val| **val != each_log.kv_params.value.clone())
                            .map(String::from)
                            .collect::<Vec<String>>(); // creates a copy

                        max_seen_seq_num = each_log.seq_num;
                    }
                }
            }
        }

        // iterate through the hashmap and combine all the keys with non-empty lists
        let mut return_value_list: Vec<String> = Vec::new();

        for (list_name, list_val) in list_map.iter() {
            if list_val.len() > 0 {
                return_value_list.push(list_name.clone());
            }
        }

        // // Logs not found in this primary - a new primary where the migration might not be complete. iterate its live backends list and contact the next live
        // if return_value_list.is_empty() {
        //     // get live backends list
        //     let secondary_bin_store_client = match self.find_next_from_live_list().await {
        //         Ok(val) => val,
        //         Err(_) => {
        //             // iterate all if live backends list not able to give secondary

        //             let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        //             let locked_bin_client_index = clone_bin_client_index.lock().await;
        //             let curr_bin_client_index = *locked_bin_client_index;
        //             std::mem::drop(locked_bin_client_index);

        //             match self
        //                 .find_next_iter_secondary(curr_bin_client_index + 1)
        //                 .await
        //             {
        //                 Ok(val) => val,
        //                 Err(_) => {
        //                     // no live backend found, return None since a backend exists but doesn't have data
        //                     return Ok(storage::List(Vec::new()));
        //                 }
        //             }
        //         }
        //     };

        //     // secondary bin store client now contains pointer to secondary node

        //     // get log
        //     let storage::List(fetched_log) =
        //         match secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await {
        //             Ok(val) => val,
        //             Err(_) => {
        //                 // no log data available
        //                 return Ok(storage::List(Vec::new()));
        //             }
        //         }; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

        //     // regenerate data from log and serve query
        //     let mut deserialized_log: Vec<UpdateLog> = fetched_log
        //         .iter()
        //         .map(|x| serde_json::from_str(&x).unwrap())
        //         .collect::<Vec<UpdateLog>>();

        //     // sort the deserialized log as per timestamp
        //     // keep the fields in the required order so that they are sorted in that same order.
        //     deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        //     // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        //     // Based on the output of the set operations, generate result for the requested get op and return
        //     // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        //     // just need to keep track of seq_num to avoid considering duplicate ops in log
        //     let mut max_seen_seq_num = 0u64;

        //     // need a HashMap of (list_name, list_items: Vec<String>)
        //     let mut list_map: HashMap<String, Vec<String>> = HashMap::new();
        //     // entry api, list_map, insert if not exits function
        //     for each_log in deserialized_log {
        //         if matches!(each_log.update_operation, UpdateOperation::ListAppend) {
        //             if each_log.kv_params.key.starts_with(&p.prefix)
        //                 && each_log.kv_params.key.ends_with(&p.suffix)
        //             {
        //                 if each_log.seq_num > max_seen_seq_num {
        //                     //return_value_set.insert(each_log.kv_params.key.clone());
        //                     // get the list for given list name
        //                     let mut get_list_ref_given_name = list_map
        //                         .entry(each_log.kv_params.key.clone())
        //                         .or_insert(Vec::<String>::new());
        //                     get_list_ref_given_name.push(each_log.kv_params.value.clone());

        //                     max_seen_seq_num = each_log.seq_num;
        //                 }
        //             }
        //         } else if matches!(each_log.update_operation, UpdateOperation::ListRemove) {
        //             if each_log.kv_params.key.starts_with(&p.prefix)
        //                 && each_log.kv_params.key.ends_with(&p.suffix)
        //             {
        //                 if each_log.seq_num > max_seen_seq_num {
        //                     // return_value_set.insert(each_log.kv_params.key.clone());

        //                     // get list for given list name
        //                     let mut get_list_ref_given_name = list_map
        //                         .entry(each_log.kv_params.key.clone())
        //                         .or_insert(Vec::<String>::new());

        //                     // iterate through the list and remove all values matching given value.
        //                     *get_list_ref_given_name = get_list_ref_given_name
        //                         .iter()
        //                         .filter(|val| **val != each_log.kv_params.value.clone())
        //                         .map(String::from)
        //                         .collect::<Vec<String>>();

        //                     max_seen_seq_num = each_log.seq_num;
        //                 }
        //             }
        //         }
        //     }

        //     // iterate through the hashmap and combine all the keys with non-empty lists
        //     for (list_name, list_val) in list_map.iter() {
        //         if list_val.len() > 0 {
        //             return_value_list.push(list_name.clone());
        //         }
        //     }
        // }

        return_value_list.sort(); //sorts in place
        Ok(storage::List(return_value_list))
    }
}

#[async_trait]
impl storage::Storage for Lab3BinStoreClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let curr_bin_client_index = *locked_bin_client_index;
        std::mem::drop(locked_bin_client_index);

        let _ = match self.find_next_iter(curr_bin_client_index).await {
            Ok(val) => val,
            Err(_) => {
                // no live backend found, return error
                return Err(Box::new(TribblerError::Unknown(
                    "No live backend found".to_string(),
                )));
            }
        };

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.clock(at_least).await?;
        Ok(result)
    }
}
