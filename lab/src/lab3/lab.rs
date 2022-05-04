use std::net::ToSocketAddrs;
use std::sync::Arc; // final version

use crate::keeper::keeper_sync_client::KeeperSyncClient;
use crate::keeper::keeper_sync_server::KeeperSyncServer;
use crate::lab3::keeper_server::KeeperServer;

use tonic::transport::{Channel, Server};
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};
/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back() -> TribResult<()> {
    let addr = "127.0.0.1:3000".parse().unwrap();

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

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<KeeperSyncClient<Channel>> {
    let client = KeeperSyncClient::connect("127.0.0.1:3000").await?;

    // let request = tonic::Request::new(());

    // let response = client.get_heartbeat(request).await?;

    // println!("RESPONSE={:?}", response);

    Ok(client)
}
