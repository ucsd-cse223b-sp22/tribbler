use async_trait::async_trait;
use tonic;
use tribbler::{rpc, storage};
// use tribbler::err::TribResult;
// use tribbler::rpc::trib_storage_client::TribStorageClient;
pub struct StorageServer {
    storage: Box<dyn storage::Storage>,
}
impl StorageServer {
    pub fn new(s: Box<dyn storage::Storage>) -> StorageServer {
        StorageServer { storage: s }
    }
}
#[async_trait]
impl rpc::trib_storage_server::TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::Value>, tonic::Status> {
        let result = match self
            .storage
            .get(&request.into_inner().key.to_string())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking get"),
                ))
            }
        };
        let response = match result {
            Some(val) => rpc::Value { value: val },
            None => rpc::Value {
                value: String::from(""),
            },
        };
        Ok(tonic::Response::new(response))
    }
    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let key_value = request.into_inner().clone();
        let result = match self
            .storage
            .set(&storage::KeyValue {
                key: key_value.key.to_string(),
                value: key_value.value.to_string(),
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking set"),
                ))
            }
        };
        Ok(tonic::Response::new(rpc::Bool { value: result }))
    }
    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let pattern = request.into_inner().clone();
        let result = match self
            .storage
            .keys(&storage::Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking keys"),
                ))
            }
        };
        let storage::List(key_list) = result;
        let response = rpc::StringList {
            list: key_list.clone(),
        };
        Ok(tonic::Response::new(response))
    }
    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let result = match self
            .storage
            .list_get(&request.into_inner().key.to_string())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking list_get"),
                ))
            }
        };
        let storage::List(val_list) = result;
        // if val_list.is_empty() {
        //     return Err(tonic::Status::new(
        //         tonic::Code::Unknown,
        //         format!("Error while invoking list_get: Key Not Found"),
        //     ));
        // }
        let response = rpc::StringList {
            list: val_list.clone(),
        };
        Ok(tonic::Response::new(response))
    }
    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let kv = request.into_inner().clone();
        let result = match self
            .storage
            .list_append(&storage::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking list_append"),
                ))
            }
        };
        Ok(tonic::Response::new(rpc::Bool { value: result }))
    }
    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        let kv = request.into_inner().clone();
        let result = match self
            .storage
            .list_remove(&storage::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking list_remove"),
                ))
            }
        };
        Ok(tonic::Response::new(rpc::ListRemoveResponse {
            removed: result,
        }))
    }
    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let pattern = request.into_inner().clone();
        let result = match self
            .storage
            .list_keys(&storage::Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking list_keys"),
                ))
            }
        };
        let storage::List(key_list) = result;
        let response = rpc::StringList {
            list: key_list.clone(),
        };
        Ok(tonic::Response::new(response))
    }
    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        let result = match self.storage.clock(request.into_inner().timestamp).await {
            Ok(v) => v,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Error while invoking clock"),
                ))
            }
        };
        Ok(tonic::Response::new(rpc::Clock { timestamp: result }))
    }
}
