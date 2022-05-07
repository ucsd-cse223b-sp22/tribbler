use std::{
    process,
    sync::{
        mpsc::{self, Sender},
        Arc,
    },
    time::Duration,
};

use lab::{lab1, lab2};
use log::{error, info, warn, LevelFilter};
use tokio::{join, time, task::JoinHandle};
use tribbler::{addr, config::Config, err::TribResult, storage::MemStorage};

#[derive(Debug, Clone)]
pub enum ProcessType {
    Back,
    Keep,
}

pub async fn main(
    t: ProcessType,
    log_level: LevelFilter,
    cfg: String,
    _ready_addrs: Vec<String>,
    recv_timeout: u64,
) -> TribResult<()> {
    env_logger::builder()
        .default_format()
        .filter_level(log_level)
        .init();
    let config = Arc::new(Config::read(Some(&cfg))?);

    println!("{:?}", config);
    let (tx, rdy) = mpsc::channel();

    let mut shutdown_sender:Option<tokio::sync::mpsc::Sender<()>> = None;

    let mut handles = vec![];
    let it = match t {
        ProcessType::Back => &config.backs,
        ProcessType::Keep => &config.keepers,
    };
    for (i, srv) in it.iter().enumerate() {
        if addr::check(srv)? {
            let handle = tokio::spawn(run_srv(
                t.clone(),
                i,
                config.clone(),
                Some(tx.clone()),
            ));

            let is_keeper_process = match t {
                ProcessType::Back => false,
                ProcessType::Keep => true,
            };
            let mut process_type = "backend";
            if is_keeper_process {
                process_type = "keeper";
            }

            handles.push((t.clone(), i, config.clone(), Some(tx.clone()), process_type, i, handle));
        }
    }
    let proc_name = match t {
        ProcessType::Back => "backend",
        ProcessType::Keep => "keeper",
    };
    if handles.is_empty() {
        warn!("no {}s found for this host", proc_name);
        return Ok(());
    }
    info!("Waiting for ready signal from {}...", proc_name);
    match rdy.recv_timeout(Duration::from_secs(recv_timeout)) {
        Ok(msg) => match msg {
            true => info!("{}s should be ready to serve.", proc_name),
            false => {
                error!("{}s failed to start successfully", proc_name);
                process::exit(1);
            }
        },
        Err(_) => {
            error!("timed out waiting for {}s to start", proc_name);
            process::exit(1);
        }
    }

    let mut interval = time::interval(time::Duration::from_secs(120)); // 30 seconds
    interval.tick().await;
    interval.tick().await;

    let mut death_interval = time::interval(time::Duration::from_secs(45)); // 30 seconds
    println!("==================outside for loop==============");
    handles.swap(0, 1);
    for (arg1, arg2, arg3, arg4, proc_type, idx, h) in handles {
        // println!("{}, {}, {:?}", proc_type, idx, h);
        if proc_type == "backend" && idx == 1 {
            println!("aborting backend 1");
            h.abort();
            death_interval.tick().await;
            death_interval.tick().await;
            println!("reviving backend 1");
            tokio::spawn(run_srv(arg1, arg2, arg3, arg4));
            continue;
        }

        match join!(h) {
            (Ok(_),) => (),
            (Err(e),) => {
                warn!("A {} failed to join: {}", proc_name, e);
            }
        };
    }
    Ok(())
}

#[allow(unused_must_use)]
async fn run_srv(t: ProcessType, idx: usize, config: Arc<Config>, tx: Option<Sender<bool>>) -> Option<tokio::sync::mpsc::Sender<()>> {
    match t {
        ProcessType::Back => {
            let (shut_tx, shut_rx) = tokio::sync::mpsc::channel::<()>(1);
            let cfg = config.back_config(idx, Box::new(MemStorage::default()), tx, Some(shut_rx));
            info!("starting backend on {}", cfg.addr);
            lab1::serve_back(cfg).await;
            return Some(shut_tx.clone());
        }
        ProcessType::Keep => {
            let (shut_tx, shut_rx) = tokio::sync::mpsc::channel::<()>(1);
            let cfg = config.keeper_config(idx, tx, Some(shut_rx)).unwrap();
            info!("starting keeper on {}", cfg.addr());
            lab2::serve_keeper(cfg).await;
            return Some(shut_tx.clone());
        }
    };
}
