use crate::lab2::storage_client::StorageClient;
use async_trait::async_trait;
use tribbler::colon;
use tribbler::{err::TribResult, storage};
pub struct BinStoreClient {
    pub name: String,
    pub colon_escaped_name: String,
    pub clients: Vec<StorageClient>,
    pub bin_client: StorageClient,
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
#[async_trait]
impl storage::KeyString for BinStoreClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut colon_escaped_key = self.colon_escaped_name.to_owned();
        colon_escaped_key.push_str(&key);
        let result = self.bin_client.get(&colon_escaped_key).await?;
        Ok(result)
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
        let colon_escaped_prefix = colon::escape(&p.prefix).to_owned();
        let colon_escaped_suffix = colon::escape(&p.suffix).to_owned();
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
        let colon_escaped_prefix = colon::escape(&p.prefix).to_owned();
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
