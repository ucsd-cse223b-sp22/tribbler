use std::{
    process,
    sync::{
        mpsc::{self, Sender, Receiver},
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

    let mut shutdown_sender_vec:Vec<tokio::sync::mpsc::Sender<()>> = vec![];

    // let (shut_tx, shut_rx) = tokio::sync::mpsc::channel::<()>(1);
    

    let mut handles = vec![];
    let it = match t {
        ProcessType::Back => &config.backs,
        ProcessType::Keep => &config.keepers,
    };

    let mut xinterval = time::interval(time::Duration::from_secs(10)); // 30 seconds
    // interval.tick().await;
    // interval.tick().await;
    for (i, srv) in it.iter().enumerate() {
        // if i == it.len() - 1 {
        //     xinterval.tick().await;
        //     xinterval.tick().await;
        // }
        let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
        shutdown_sender_vec.push(shut_tx);
        if addr::check(srv)? {
            let handle = tokio::spawn(run_srv(
                t.clone(),
                i,
                config.clone(),
                Some(tx.clone()),
                shut_rx
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

    let mut interval = time::interval(time::Duration::from_secs(10)); // 30 seconds
    interval.tick().await;
    interval.tick().await;

    let mut death_interval = time::interval(time::Duration::from_secs(1)); // 30 seconds
    // handles.swap(0, 1);
    for (arg1, arg2, arg3, arg4, proc_type, idx, h) in handles {
        // println!("{}, {}, {:?}", proc_type, idx, h);
        if proc_type == "keeper" && idx == 0 {
            println!("==========================aborting keeper {}==================================", idx);
            shutdown_sender_vec[idx].send(()).await?;
            // death_interval.tick().await;
            // death_interval.tick().await;
            // println!("=================================reviving keeper {}==========================", idx);
            // tokio::spawn(run_srv(arg1, arg2, arg3, arg4));
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
async fn run_srv(t: ProcessType, idx: usize, config: Arc<Config>, tx: Option<Sender<bool>>, rx: tokio::sync::mpsc::Receiver<()>) {
    match t {
        ProcessType::Back => {
            let cfg = config.back_config(idx, Box::new(MemStorage::default()), tx, Some(rx));
            info!("starting backend on {}", cfg.addr);
            lab1::serve_back(cfg).await;
        }
        ProcessType::Keep => {
            // if idx != 0 {
            let cfg = config.keeper_config(idx, tx, Some(rx)).unwrap();
            info!("starting keeper on {}", cfg.addr());
            lab2::serve_keeper(cfg).await;
            // }
        }
    };
}
