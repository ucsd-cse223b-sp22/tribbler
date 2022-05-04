use async_trait::async_trait;
use tonic::transport::Server;

use crate::keeper::{
    keeper_sync_server::{KeeperSync, KeeperSyncServer},
    Heartbeat,
};

pub struct KeeperServer {
    pub max_clock_so_far: u64,
    pub live_backends_list_bc: String,
    pub live_backends_list_kp: String,
    pub first_bc_index: u32,
    pub last_bc_index: u32,
    pub backs: Vec<String>,
    pub index: u32,
}

#[async_trait]
impl KeeperSync for KeeperServer {
    async fn get_heartbeat(
        &self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Response<Heartbeat>, tonic::Status> {
        let mut r: Result<tonic::Response<Heartbeat>, tonic::Status> =
            Ok(tonic::Response::new(Heartbeat {
                max_clock: 0,
                live_back_list: String::from(""),
                keeper_index: 0,
                first_back_index: 0,
                last_back_index: 0,
            }));
        return r;
    }
}

// #[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();

    println!("KeeperServer listening on {}", addr);

    Server::builder()
        .add_service(KeeperSyncServer::new(KeeperServer {
            max_clock_so_far: 0,
            live_backends_list_bc: String::from(""),
            live_backends_list_kp: String::from(""),
            first_bc_index: 0,
            last_bc_index: 0,
            backs: vec![],
            index: 0,
        }))
        .serve(addr)
        .await?;

    Ok(())
}
