// This is a wrapper over lab2_bin_store_client

use crate::lab2::storage_client::StorageClient;
use async_trait::async_trait;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};
use tribbler::err::TribblerError;
use tribbler::storage::{KeyList, Storage};
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
    ListGet,
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

/* impl Lab3BinStoreClient {
    async fn find_next_iter() {}

    async fn find_next_live_list() {}
}
 */
// use mutex

// Check the original functionality for list_get, list_set etc

#[async_trait]
impl storage::KeyString for Lab3BinStoreClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // fetch log by forwarding request to bin_store_client. It will handle prepending name and handling bin part

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;

        let storage::List(fetched_log) = match locked_bin_store_client
            .list_get(KEY_UPDATE_LOG)
            .await
        {
            Ok(v) => v, // 1st shot return
            Err(_) => {
                // 1st one didnt work; 2nd attempt by iterating through the list to find the next alive
                // fetching logs is unsuccessful, try getting it from the next live backend.
                let n = self.back_addrs.len() as u64;

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index;

                let mut primary_backend_index = curr_bin_client_index;

                let mut is_primary_found = false;

                for backend_index_iter in 0..n {
                    let backend_addr = &self.back_addrs
                        [((backend_index_iter + curr_bin_client_index as u64) % n) as usize]; // start from hashed_backend_index

                    let client = StorageClient {
                        addr: format!("http://{}", backend_addr.clone())
                            .as_str()
                            .to_owned(),
                        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                    };

                    // perform clock() rpc call to check if the backend is alive
                    match client.clock(0).await {
                        Ok(_) => {
                            primary_backend_index =
                                ((backend_index_iter + curr_bin_client_index as u64) % n) as usize;
                            is_primary_found = true;
                            break;
                        } // backend alive make it primary
                        Err(_) => {} // backend not alive, continue iteration
                    };
                }

                if is_primary_found {
                    // get a client to the primary backend
                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let mut locked_bin_client_index = clone_bin_client_index.lock().await;
                    *locked_bin_client_index = primary_backend_index.clone() as usize;

                    let backend_addr = self.back_addrs[primary_backend_index as usize].clone();

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

                    match (*locked_bin_store_client).list_get(KEY_UPDATE_LOG).await {
                        Ok(v) => v,
                        Err(_) => {
                            // DONE-TODO: 3rd iteration, a live found but gone down while fetching log; then again iterate - Rare case
                            // Another case is the node does not have data - replication might not be complete - sure that the next alive won't crash until replication is complete if setup correctly - No issues
                            // What if a node in between comes up? The secondary will still have data and can serve read requests. No issues here

                            is_primary_found = false;

                            for backend_index_iter in 0..n {
                                let backend_addr = &self.back_addrs[((backend_index_iter
                                    + primary_backend_index as u64)
                                    % n)
                                    as usize]; // start from primary backend index

                                let client = StorageClient {
                                    addr: format!("http://{}", backend_addr.clone())
                                        .as_str()
                                        .to_owned(),
                                    cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                                };

                                // perform clock() rpc call to check if the backend is alive
                                match client.clock(0).await {
                                    Ok(_) => {
                                        primary_backend_index = ((backend_index_iter
                                            + primary_backend_index as u64)
                                            % n)
                                            as usize;
                                        is_primary_found = true;
                                        break;
                                    } // backend alive make it primary
                                    Err(_) => {} // backend not alive, continue iteration
                                };
                            }

                            if is_primary_found {
                                // get a client to the primary backend
                                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                                let mut locked_bin_client_index =
                                    clone_bin_client_index.lock().await;
                                *locked_bin_client_index = primary_backend_index.clone() as usize;
                                let backend_addr =
                                    self.back_addrs[primary_backend_index as usize].clone();

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
                                let mut locked_bin_store_client =
                                    clone_bin_store_client.lock().await;
                                *locked_bin_store_client = BinStoreClient {
                                    name: self.name.clone(),
                                    colon_escaped_name: self.colon_escaped_name.clone(),
                                    clients: self.clients.clone(),
                                    bin_client: (*locked_bin_client).clone(),
                                };

                                match (*locked_bin_store_client).list_get(KEY_UPDATE_LOG).await {
                                    // this should definitely respond
                                    Ok(v) => v,
                                    Err(_) => {
                                        return Err(Box::new(TribblerError::Unknown(
                                            "No update log found".to_string(),
                                        )));
                                    }
                                }
                            } else {
                                // no live backend found, return error
                                return Err(Box::new(TribblerError::Unknown(
                                    "No live backend found".to_string(),
                                )));
                            }
                        }
                    }
                } else {
                    // no live backend found, return error
                    return Err(Box::new(TribblerError::Unknown(
                        "No live backend found".to_string(),
                    )));
                }
            }
        };

        // regenerate data from log and serve query
        let mut deserialized_log: Vec<UpdateLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<UpdateLog>>();

        // sort the deserialized log as per timestamp
        // keep the fields in the required order so that they are sorted in that same order.
        deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
        // Based on the output of the set operations, generate result for the requested get op and return
        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // just need to keep track of seq_num to avoid considering duplicate ops in log
        let mut max_seen_seq_num = 0u64;

        let mut return_value: String = String::from("");

        for each_log in deserialized_log {
            if each_log.seq_num > max_seen_seq_num {
                if matches!(each_log.update_operation, UpdateOperation::Set) {
                    if each_log.kv_params.key.eq(key) {
                        return_value = each_log.kv_params.value.clone();
                        max_seen_seq_num = each_log.seq_num;
                    }
                }
            }
        }

        // Logs not found in this primary - a new primary where the migration might not be complete. iterate its live backends list and contact the next live
        if return_value.eq("") {
            // get live backends list
            let clone_bin_client = Arc::clone(&self.bin_client);
            let locked_bin_client = clone_bin_client.lock().await;

            match locked_bin_client // DONE-TODO: change to storage_client
                .get(KEY_LIVE_BACKENDS_LIST) // live backends list is a serialized KeyValue where value is a serialized map of backends and their alive status
                .await
            {
                Ok(Some(live_backends_map)) => {
                    // Got live backends map, find the secondary and get data from there

                    // deserialize it
                    // regenerate data from log and serve query
                    let mut deserialized_live_backends_map: LiveBackends =
                        serde_json::from_str(&live_backends_map)?;

                    // iterate in live backends list, start from location of the primary and take the next alive as secondary

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let primary_backend_index = *locked_bin_client_index;

                    let mut secondary_addr = String::from(""); // initialize it to empty for now

                    //iterate over len modulo n
                    let len_live_backends_list =
                        deserialized_live_backends_map.is_alive_list.len() as usize;
                    for live_index in 1..len_live_backends_list {
                        // since primary is already found need to start from primary + 1 so start from index 1
                        if deserialized_live_backends_map.is_alive_list[((live_index
                            + primary_backend_index)
                            % len_live_backends_list)
                            as usize]
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

                    // get log
                    let storage::List(fetched_log) =
                        secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

                    // regenerate data from log and serve query
                    let mut deserialized_log: Vec<UpdateLog> = fetched_log
                        .iter()
                        .map(|x| serde_json::from_str(&x).unwrap())
                        .collect::<Vec<UpdateLog>>();

                    // sort the deserialized log as per timestamp
                    // keep the fields in the required order so that they are sorted in that same order.
                    deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                    // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
                    // Based on the output of the set operations, generate result for the requested get op and return
                    // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

                    // just need to keep track of seq_num to avoid considering duplicate ops in log
                    let mut max_seen_seq_num = 0u64;

                    for each_log in deserialized_log {
                        if each_log.seq_num > max_seen_seq_num {
                            if matches!(each_log.update_operation, UpdateOperation::Set) {
                                if each_log.kv_params.key.eq(key) {
                                    return_value = each_log.kv_params.value.clone();
                                    max_seen_seq_num = each_log.seq_num;
                                }
                            }
                        }
                    }
                }
                _ => {
                    // handle Ok(None) case as well
                    // DONE-TODO: 4th check, if getting live backends list also fails then do iterative search for secondary
                    let n = self.back_addrs.len() as u64;

                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let locked_bin_client_index = clone_bin_client_index.lock().await;
                    let curr_bin_client_index = *locked_bin_client_index;

                    let mut secondary_backend_index = curr_bin_client_index;

                    let mut is_secondary_found = false;

                    for backend_index_iter in 0..n {
                        let backend_addr = &self.back_addrs
                            [((backend_index_iter + curr_bin_client_index as u64) % n) as usize]; // start from hashed_backend_index

                        let client = StorageClient {
                            addr: format!("http://{}", backend_addr.clone())
                                .as_str()
                                .to_owned(),
                            cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                        };

                        // perform clock() rpc call to check if the backend is alive
                        match client.clock(0).await {
                            Ok(_) => {
                                secondary_backend_index =
                                    ((backend_index_iter + curr_bin_client_index as u64) % n)
                                        as usize;
                                is_secondary_found = true;
                                break;
                            } // backend alive make it primary
                            Err(_) => {} // backend not alive, continue iteration
                        };
                    }

                    if is_secondary_found {
                        // get a client to the secondary backend
                        let secondary_addr =
                            self.back_addrs[secondary_backend_index as usize].clone();

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

                        // get log
                        let storage::List(fetched_log) =
                            secondary_bin_store_client.list_get(KEY_UPDATE_LOG).await?; // would have data. crash not possible because reached here due to crash of primary or primary not having data due to just coming alive

                        // regenerate data from log and serve query
                        let mut deserialized_log: Vec<UpdateLog> = fetched_log
                            .iter()
                            .map(|x| serde_json::from_str(&x).unwrap())
                            .collect::<Vec<UpdateLog>>();

                        // sort the deserialized log as per timestamp
                        // keep the fields in the required order so that they are sorted in that same order.
                        deserialized_log.sort(); // sorts in place, since first field is seq_num, it sorts acc to seq_num

                        // iterate through the desirialized log and look only for set operations for this user. Single log is the best/easiest
                        // Based on the output of the set operations, generate result for the requested get op and return
                        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

                        // just need to keep track of seq_num to avoid considering duplicate ops in log
                        let mut max_seen_seq_num = 0u64;

                        for each_log in deserialized_log {
                            if each_log.seq_num > max_seen_seq_num {
                                if matches!(each_log.update_operation, UpdateOperation::Set) {
                                    if each_log.kv_params.key.eq(key) {
                                        return_value = each_log.kv_params.value.clone();
                                        max_seen_seq_num = each_log.seq_num;
                                    }
                                }
                            }
                        }
                    } else {
                        // no live backend found, return error
                        return Err(Box::new(TribblerError::Unknown(
                            "No live backend found".to_string(),
                        )));
                    }
                }
            };
        }

        //let return_value: String = String::from("Test return value");

        // let result = self.bin_client.get(&colon_escaped_key).await?;
        Ok(Some(return_value))
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        // check if self.bin_store_client is alive
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let new_seq_num_result = locked_bin_store_client.clock(0).await;

        match new_seq_num_result {
            Ok(_) => {} // continue
            Err(_) => {
                // error then find next alive node

                let n = self.back_addrs.len() as u64;

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index as u64;

                let mut primary_backend_index = curr_bin_client_index;

                let mut is_primary_found = false;

                for backend_index_iter in 0..n {
                    let backend_addr = &self.back_addrs
                        [((backend_index_iter + curr_bin_client_index) % n) as usize]; // start from curr bin client index

                    let client = StorageClient {
                        addr: format!("http://{}", backend_addr.clone())
                            .as_str()
                            .to_owned(),
                        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                    };

                    // perform clock() rpc call to check if the backend is alive
                    match client.clock(0).await {
                        Ok(_) => {
                            primary_backend_index =
                                (backend_index_iter + curr_bin_client_index) % n;
                            is_primary_found = true;
                            break;
                        } // backend alive make it primary
                        Err(_) => {} // backend not alive, continue iteration
                    };
                }

                if is_primary_found {
                    // get a client to the primary backend
                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let mut locked_bin_client_index = clone_bin_client_index.lock().await;
                    *locked_bin_client_index = primary_backend_index.clone() as usize;

                    let backend_addr = self.back_addrs[primary_backend_index as usize].clone();

                    let clone_bin_client = Arc::clone(&self.bin_client);
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
                } else {
                    // no live backend found, return error
                    return Err(Box::new(TribblerError::Unknown(
                        "No live backend found".to_string(),
                    )));
                }
            }
        };

        // what if a backend dies right after above check?
        // Need 2nd pass, that will be primary, guaranteed alive, store data there,
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let new_seq_num_result = locked_bin_store_client.clock(0).await;

        match new_seq_num_result {
            Ok(_) => {} // continue
            Err(_) => {
                // error then find next alive node

                let n = self.back_addrs.len() as u64;

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index as u64;

                let mut primary_backend_index = curr_bin_client_index;

                let mut is_primary_found = false;

                for backend_index_iter in 0..n {
                    let backend_addr = &self.back_addrs
                        [((backend_index_iter + curr_bin_client_index) % n) as usize]; // start from curr bin client index

                    let client = StorageClient {
                        addr: format!("http://{}", backend_addr.clone())
                            .as_str()
                            .to_owned(),
                        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                    };

                    // perform clock() rpc call to check if the backend is alive
                    match client.clock(0).await {
                        Ok(_) => {
                            primary_backend_index =
                                (backend_index_iter + curr_bin_client_index) % n;
                            is_primary_found = true;
                            break;
                        } // backend alive make it primary
                        Err(_) => {} // backend not alive, continue iteration
                    };
                }

                if is_primary_found {
                    // get a client to the primary backend
                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let mut locked_bin_client_index = clone_bin_client_index.lock().await;
                    *locked_bin_client_index = primary_backend_index.clone() as usize;

                    let backend_addr = self.back_addrs[primary_backend_index as usize].clone();

                    let clone_bin_client = Arc::clone(&self.bin_client);
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
                } else {
                    // no live backend found, return error
                    return Err(Box::new(TribblerError::Unknown(
                        "No live backend found".to_string(),
                    )));
                }
            }
        };

        // now primary is guaranteed to be alive
        // generate UpdateLog
        // get seq_num by calling clock() RPC
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;

        let new_seq_num = locked_bin_store_client.clock(0).await?;

        let new_update_log = UpdateLog {
            seq_num: new_seq_num, // DONE-TODO: no plus one here
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

        locked_bin_store_client.list_append(&log_append_kv).await?;

        // add this bin to primary list of the node - SHOULD this be before appending the log?
        let primary_list_append_kv = tribbler::storage::KeyValue {
            key: KEY_PRIMARY_LIST.to_string().clone(),
            value: self.name.clone(),
        };

        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;

        locked_bin_store_client
            .list_append(&primary_list_append_kv)
            .await?;

        // what if primary is there but secondary fails in between writing? Need to write to next alive in the list.

        // also append log to secondary - the next in the live backends list
        // get live backends list
        let clone_bin_client = Arc::clone(&self.bin_client);
        let locked_bin_client = clone_bin_client.lock().await;

        let live_backends_list_result = locked_bin_client.list_get(KEY_LIVE_BACKENDS_LIST).await; // DONE-TODO: use storage client

        // TODO: do a third iteration on error or if primary crashed in between. This iteration is to get the secondary
        let live_backends_list = match live_backends_list_result {
            Ok(v) => v,
            Err(_) => {
                let n = self.back_addrs.len() as u64;

                let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                let locked_bin_client_index = clone_bin_client_index.lock().await;
                let curr_bin_client_index = *locked_bin_client_index as u64;

                let mut primary_backend_index = curr_bin_client_index;

                let mut is_primary_found = false;

                for backend_index_iter in 0..n {
                    let backend_addr = &self.back_addrs
                        [((backend_index_iter + curr_bin_client_index) % n) as usize]; // start from curr bin client index

                    let client = StorageClient {
                        addr: format!("http://{}", backend_addr.clone())
                            .as_str()
                            .to_owned(),
                        cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
                    };

                    // perform clock() rpc call to check if the backend is alive
                    match client.clock(0).await {
                        Ok(_) => {
                            primary_backend_index =
                                (backend_index_iter + curr_bin_client_index) % n;
                            is_primary_found = true;
                            break;
                        } // backend alive make it primary
                        Err(_) => {} // backend not alive, continue iteration
                    };
                }

                if is_primary_found {
                    // get a client to the primary backend
                    let clone_bin_client_index = Arc::clone(&self.bin_client_index);
                    let mut locked_bin_client_index = clone_bin_client_index.lock().await;
                    *locked_bin_client_index = primary_backend_index.clone() as usize;

                    let backend_addr = self.back_addrs[primary_backend_index as usize].clone();

                    let clone_bin_client = Arc::clone(&self.bin_client);
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

                    match locked_bin_client.list_get(KEY_LIVE_BACKENDS_LIST).await {
                        Ok(v) => v,
                        Err(_) => {
                            // no live backend found, return error
                            return Err(Box::new(TribblerError::Unknown(
                                "Error in getting live backends list".to_string(),
                            )));
                        }
                    }
                } else {
                    // no live backend found, return error
                    return Err(Box::new(TribblerError::Unknown(
                        "No live backend found".to_string(),
                    )));
                }
            }
        };

        // iterate in live backends list, find the location of the primary and take the next as secondary
        let clone_bin_client_index = Arc::clone(&self.bin_client_index);
        let locked_bin_client_index = clone_bin_client_index.lock().await;
        let mut secondary_addr = self.back_addrs[*locked_bin_client_index].clone();

        //iterate over len modulo n
        let len_live_backends_list = live_backends_list.0.len() as usize;
        for live_index in 0..len_live_backends_list {
            if live_backends_list.0[live_index as usize]
                .eq(self.back_addrs[*locked_bin_client_index as usize]
                    .clone()
                    .as_str())
            {
                secondary_addr =
                    live_backends_list.0[(live_index + 1) % len_live_backends_list].clone();
            }
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

        // append log
        secondary_bin_store_client
            .list_append(&log_append_kv)
            .await?;

        // add this bin to secondary list of the node
        let secondary_list_append_kv = tribbler::storage::KeyValue {
            key: KEY_SECONDARY_LIST.to_string().clone(),
            value: self.name.clone(),
        };

        secondary_bin_store_client
            .list_append(&secondary_list_append_kv)
            .await?;

        Ok(true) // may keep a boolean variable to combine result of all
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.list_keys(&p).await?;
        Ok(result)
    }
}

#[async_trait]
impl storage::KeyList for Lab3BinStoreClient {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.list_get(&key).await?;
        Ok(result)
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.list_append(&kv).await?;
        Ok(result)
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.list_remove(&kv).await?;
        Ok(result)
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.list_keys(&p).await?;
        Ok(result)
    }
}

#[async_trait]
impl storage::Storage for Lab3BinStoreClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let clone_bin_store_client = Arc::clone(&self.bin_store_client);
        let locked_bin_store_client = clone_bin_store_client.lock().await;
        let result = locked_bin_store_client.clock(at_least).await?;
        Ok(result)
    }
}
