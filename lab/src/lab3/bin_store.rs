use super::storage_client::StorageClient;
use crate::lab2::bin_store_client::BinStoreClient;
use ::tribbler::colon;
use async_trait::async_trait;
use std::sync::Arc;
use std::{collections::hash_map::DefaultHasher, hash::Hasher};
use tokio::sync::Mutex;
use tribbler::err::TribResult;
use tribbler::storage;
use tribbler::storage::Storage;

pub struct BinStore {
    pub back_addrs: Vec<String>,
}
#[async_trait]
impl storage::BinStorage for BinStore {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let n = self.back_addrs.len() as u64;
        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());

        // get the first live backend as primary
        let hash = hasher.finish();

        let hashed_backend_index = hash % n; // generate hash and get the index of backend

        let mut primary_backend_index = hashed_backend_index;

        // iterate and find the next alive starting from hashed_backend_index
        for backend_index_iter in 0..n {
            let backend_addr =
                &self.back_addrs[((backend_index_iter + hashed_backend_index) % n) as usize]; // start from hashed_backend_index
            let client = StorageClient {
                addr: format!("http://{}", backend_addr.clone())
                    .as_str()
                    .to_owned(),
                cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
            };

            // perform clock() rpc call to check if the backend is alive

            match client.clock(0).await {
                Ok(_) => {
                    primary_backend_index = (backend_index_iter + hashed_backend_index) % n;
                    break; // have a is_found
                } // backend alive make it primary
                Err(_) => {} // backend not alive
            };
        }

        // get a client to the primary backend
        let backend_addr = &self.back_addrs[primary_backend_index as usize];
        let client = StorageClient {
            addr: format!("http://{}", backend_addr.clone())
                .as_str()
                .to_owned(),
            cached_conn: Arc::new(tokio::sync::Mutex::new(None)),
        };

        // clock call to check that the primary backend is actually alive
        // Otherwise error

        let mut colon_escaped_name: String = colon::escape(name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let mut storage_clients: Vec<StorageClient> = Vec::new();
        for address in self.back_addrs.iter() {
            storage_clients.push(StorageClient {
                addr: format!("http://{}", address.clone()),
                cached_conn: Arc::new(Mutex::new(None)),
            });
        }
        Ok(Box::new(BinStoreClient {
            name: String::from(name),
            colon_escaped_name,
            clients: storage_clients,
            bin_client: client,
            bin_client_index: primary_backend_index as usize,
        }))
    }
}
