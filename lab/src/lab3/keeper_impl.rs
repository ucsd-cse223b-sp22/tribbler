use async_trait::async_trait;
use tribbler::config::KeeperConfig;

use crate::keeper::{keeper_sync_server::KeeperSync, Heartbeat};

pub struct Keeper {
    pub max_clock_so_far: u64,
    pub live_backends_list_bc: String,
    pub live_backends_list_kp: String,
    pub first_bc_index: u32,
    pub last_bc_index: u32,
    pub backs: Vec<String>,
    pub index: u32,
}

#[async_trait]
impl KeeperSync for Keeper {
    async fn get_heartbeat(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<Heartbeat>, tonic::Status> {
        let r: Result<tonic::Response<Heartbeat>, tonic::Status> =
            Ok(tonic::Response::new(Heartbeat {
                max_clock: self.max_clock_so_far,
                live_back_list: self.live_backends_list_bc.clone(),
                keeper_index: self.index,
                first_back_index: self.first_bc_index,
                last_back_index: self.last_bc_index,
            }));
        return r;
    }
}
