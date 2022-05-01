use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tribbler::err::TribResult;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::{rpc, storage};

pub struct StorageClient {
    pub addr: String,
    pub cached_conn: Arc<Mutex<Option<TribStorageClient<Channel>>>>,
}

#[async_trait]
impl storage::KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // we implement Storage class to feel as if we are accessing locally but internally we are calling the server
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        match response.into_inner().value {
            value => {
                if value.chars().count() == 0 {
                    Ok(None)
                } else {
                    Ok(Some(value))
                }
            }
        }
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?);
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .set(rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match response.into_inner().value {
            value => Ok(value),
        }
    }
    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .keys(rpc::Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        match response.into_inner().list {
            list => Ok(storage::List(list)),
        }
    }
}

#[async_trait]
impl storage::KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .list_get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        match response.into_inner().list {
            list => Ok(storage::List(list)),
        }
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .list_append(rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match response.into_inner().value {
            value => Ok(value),
        }
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .list_remove(rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match response.into_inner().removed {
            removed => Ok(removed),
        }
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .list_keys(rpc::Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        match response.into_inner().list {
            list => Ok(storage::List(list)),
        }
    }
}

#[async_trait]
impl storage::Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let my_cached_conn = Arc::clone(&self.cached_conn);
        let mut mut_cached_conn = my_cached_conn.lock().await;
        if mut_cached_conn.is_none() {
            *mut_cached_conn = Some(TribStorageClient::connect(self.addr.clone()).await?).clone();
        }

        let response = mut_cached_conn
            .as_mut()
            .unwrap()
            .clock(rpc::Clock {
                timestamp: at_least,
            })
            .await?;
        match response.into_inner().timestamp {
            timestamp => Ok(timestamp),
        }
    }
}
