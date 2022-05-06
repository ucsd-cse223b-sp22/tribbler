use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::keeper::{
    keeper_sync_client::KeeperSyncClient, keeper_sync_server::KeeperSync, Heartbeat,
};

use super::lab3_bin_store_client::LiveBackends;
#[derive(Clone)]
pub struct Keeper {
    pub max_clock_so_far: Arc<Mutex<u64>>,
    pub live_backends_list_bc: Arc<Mutex<LiveBackends>>,
    pub live_backends_list_kp: Arc<Mutex<LiveBackends>>,
    pub first_bc_index: Arc<Mutex<u32>>,
    pub last_bc_index: Arc<Mutex<u32>>,
    pub back_addrs: Vec<String>,
    pub keeper_addrs: Vec<String>,
    pub my_id: u32,
    pub cached_conn: Arc<Mutex<Vec<Option<KeeperSyncClient<Channel>>>>>,
}

impl Keeper {}

#[async_trait]
impl KeeperSync for Keeper {
    async fn get_heartbeat(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<Heartbeat>, tonic::Status> {
        let clone_arc = self.max_clock_so_far.clone();
        let locked_val = clone_arc.lock().await;
        let max_clock_so_far = *locked_val;
        drop(locked_val);

        let clone_arc = self.live_backends_list_kp.clone();
        let locked_val = clone_arc.lock().await;
        let live_backends_list_kp = (*locked_val).clone();
        drop(locked_val);

        let my_id = self.my_id;

        let clone_arc = self.first_bc_index.clone();
        let locked_val = clone_arc.lock().await;
        let first_back_index = *locked_val;
        drop(locked_val);

        let clone_arc = self.last_bc_index.clone(); // when you want to get actual values you need to acquire lock and get values
        let locked_val = clone_arc.lock().await; // when you just want to pass the arc<Mutex> field to someone then dereference and pass
        let last_back_index = *locked_val;
        drop(locked_val);

        Ok(tonic::Response::new(Heartbeat {
            max_clock: max_clock_so_far,
            live_back_list: serde_json::to_string(&live_backends_list_kp).unwrap(),
            keeper_index: my_id,
            first_back_index: first_back_index,
            last_back_index: last_back_index,
        }))
        // return r;
    }
}
