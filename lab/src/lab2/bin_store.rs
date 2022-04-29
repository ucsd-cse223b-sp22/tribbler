use super::storage_client::StorageClient;
use crate::lab2::bin_store_client::BinStoreClient;
use ::tribbler::colon;
use async_trait::async_trait;
use std::{collections::hash_map::DefaultHasher, hash::Hasher};
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

        let hash = hasher.finish();
        let backend_addr = &self.back_addrs[(hash % n) as usize];
        let client = StorageClient {
            addr: format!("http://{}", backend_addr.clone())
                .as_str()
                .to_owned(),
        };
        let mut colon_escaped_name: String = colon::escape(name.clone()).to_owned();
        colon_escaped_name.push_str(&"::".to_string());

        let mut storage_clients: Vec<StorageClient> = Vec::new();
        for address in self.back_addrs.iter() {
            storage_clients.push(StorageClient {
                addr: format!("http://{}", address.clone()),
            });
        }
        Ok(Box::new(BinStoreClient {
            name: String::from(name),
            colon_escaped_name,
            clients: storage_clients,
            bin_client: client,
        }))
    }
}
