use crate::keeper::{keeper_sync_client::KeeperSyncClient, Heartbeat};

pub struct KeeperClient {
    pub addr: String,
}

// impl KeeperClient {
//     async fn conn(&self) -> Result<(), ()> {
//         let mut client = KeeperSyncClient::connect(self.addr.clone()).await.unwrap();
//         let x = client.get_heartbeat(tonic::Request::new(())).await.unwrap();
//         println!("Response from server: {:?}", x);
//         return Ok(());
//     }
// }

async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeeperSyncClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(());

    let response = client.get_heartbeat(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
