use crate::lab2::storage_client::StorageClient;
use async_trait::async_trait;

use serde::{Deserialize, Serialize};
use tribbler::colon;
use tribbler::err::TribblerError;
use tribbler::storage::{KeyList, KeyValue, Storage};
use tribbler::{err::TribResult, storage};

pub struct BinStoreClient {
    pub name: String,
    pub colon_escaped_name: String,
    pub clients: Vec<StorageClient>,
    pub bin_client: StorageClient,
    pub bin_client_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum UpdateOperation {
    Set,
    ListAppend,
    ListGet,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UpdateLog {
    seq_no: u64,
    update_operation: UpdateOperation,
    kv_params: KeyValue, // override  KeyValue and implement serialize for it
}

fn remove_prefix(s: &str, p: &str) -> String {
    let string = s.clone();
    if s.starts_with(p) {
        let result = String::from(&string[p.len()..]);
        result
    } else {
        let result = String::from(string);
        result
    }
}

impl BinStoreClient {
    pub async fn common_ops(&mut self) -> TribResult<()> {
        // try to write this code in each function

        // check if self.bin_client is alive
        match self.bin_client.clock(0).await {
            Ok(_) => return Ok(()),
            Err(_) => {} // error then find next alive
        };

        let n = self.clients.len() as u64;
        let curr_bin_client_index = self.bin_client_index.clone() as u64;

        for backend_index_iter in 0..n {
            let client =
                self.clients[((backend_index_iter + curr_bin_client_index) % n) as usize].clone();

            // perform clock() rpc call to check if the backend is alive
            match client.clock(0).await {
                Ok(_) => {
                    self.bin_client_index =
                        ((backend_index_iter + self.bin_client_index as u64) % n) as usize;
                    self.bin_client = client;
                    break;
                } // backend alive make it primary
                Err(_) => {} // backend not alive
            };
        }

        // check if obtained self.bin_client is alive
        match self.bin_client.clock(0).await {
            Ok(_) => Ok(()),
            Err(_) => Err(Box::new(TribblerError::Unknown(
                "No backend alive".to_string(),
            ))),
        }
    }
}

#[async_trait]
impl storage::KeyString for BinStoreClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut colon_escaped_key = self.colon_escaped_name.to_owned();
        colon_escaped_key.push_str(&key);

        // get log
        let mut colon_escaped_log = self.colon_escaped_name.to_owned();
        colon_escaped_log.push_str("LOG");

        // fetch log
        let storage::List(fetched_log) = match self.bin_client.list_get("LOG").await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        // regenerate data from log and serve query
        let deserialized_log: Vec<UpdateLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<UpdateLog>>();

        // sort the deserialized log as per timestamp
        // keep the fields in the required order so that they are sorted in that same order.

        // think about where can we clean the log - clean in memory only, Don't clean in storage - hashset approach

        // iterate through the desirialized log and look only for set operations. Single log is the best/easiest

        // Based on the output of the set operations, generate result for the requested get op and return

        // Error in first getting value from primary - then iterate live backends list and contact the next live

        let return_value: String = String::from("Test return value");

        // let result = self.bin_client.get(&colon_escaped_key).await?;
        Ok(Some(return_value))
    }
    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let mut colon_escaped_key = self.colon_escaped_name.to_owned();
        colon_escaped_key.push_str(&kv.key);

        let result = self
            .bin_client
            .set(&storage::KeyValue {
                key: colon_escaped_key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(result)
    }
    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let mut colon_escaped_prefix = self.colon_escaped_name.to_owned();
        colon_escaped_prefix.push_str(&p.prefix);
        //log::info!("colon_escaped_prefix: {}", colon_escaped_prefix);

        let colon_escaped_suffix = colon::escape(&p.suffix).to_owned();
        //log::info!("colon_escaped_suffix: {}", colon_escaped_suffix);

        let storage::List(result_keys) = self
            .bin_client
            .keys(&storage::Pattern {
                prefix: colon_escaped_prefix.clone(),
                suffix: colon_escaped_suffix.clone(),
            })
            .await?;

        let mut result_keys_for_bin: Vec<String> = Vec::new();
        for key in result_keys {
            let mut colon_name = self.name.to_owned();
            colon_name.push_str(&"::".to_string());
            if colon::unescape(&key).starts_with(&colon_name) {
                result_keys_for_bin.push(remove_prefix(&key, &colon_name));
            }
        }
        Ok(storage::List(result_keys_for_bin))
    }
}

#[async_trait]
impl storage::KeyList for BinStoreClient {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let mut colon_escaped_key = self.colon_escaped_name.to_owned();
        colon_escaped_key.push_str(&key);

        let result = self.bin_client.list_get(&colon_escaped_key).await?;
        Ok(result)
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let mut colon_escaped_key = self.colon_escaped_name.to_owned();
        colon_escaped_key.push_str(&kv.key);

        let result = self
            .bin_client
            .list_append(&storage::KeyValue {
                key: colon_escaped_key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(result)
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let mut colon_escaped_key = self.colon_escaped_name.to_owned();
        colon_escaped_key.push_str(&kv.key);

        let result = self
            .bin_client
            .list_remove(&storage::KeyValue {
                key: colon_escaped_key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(result)
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let mut colon_escaped_prefix = self.colon_escaped_name.to_owned();
        colon_escaped_prefix.push_str(&p.prefix);

        let colon_escaped_suffix = colon::escape(&p.suffix).to_owned();
        let storage::List(result_keys) = self
            .bin_client
            .list_keys(&storage::Pattern {
                prefix: colon_escaped_prefix.clone(),
                suffix: colon_escaped_suffix.clone(),
            })
            .await?;
        let mut result_keys_for_bin: Vec<String> = Vec::new();
        for key in result_keys {
            let mut colon_name = self.name.to_owned();
            colon_name.push_str(&"::".to_string());
            if colon::unescape(&key).starts_with(&colon_name) {
                result_keys_for_bin.push(remove_prefix(&key, &colon_name));
            }
        }
        Ok(storage::List(result_keys_for_bin))
    }
}

#[async_trait]
impl storage::Storage for BinStoreClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let result = self.bin_client.clock(at_least).await?;
        Ok(result)
    }
}
