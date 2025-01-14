pub fn hle() -> &'static str {
    if cfg!(feature = "hardware-lock-elision") {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            "ON    "
        } else {
            "NO HTL"
        }
    } else {
        "OFF   "
    }
}

pub type SnapShot = Version;
pub type INDEX = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

use crossbeam_channel::{bounded, Sender, TryRecvError};
use itertools::{Either, Itertools};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::ops::Div;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::Arc;
use std::{mem, thread};
use std::os::unix::thread::JoinHandleExt;
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, SystemTime};
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::CRUDProtocol;
use crate::record_model::Version;
use crate::tree::bplus_tree;
use crate::tree::bplus_tree::{new_INDEX, BPlusTree};

pub fn olap(index: IndexHandler, snapshot: SnapShot, number_olaps: u64) -> Arc<AtomicU64> {
    assert!(index.is_left(),
            "OLAP init failed! Provide an initialized TxManager!");

    let olaps = Arc::new(AtomicU64::new(0));
    let olaps_clone_1 = olaps.clone();
    let olaps_clone_2 = olaps.clone();

    let _join_handle = spawn(move || if let Either::Left(m_manager) = index {
        let manager = m_manager.clone();
        let olap_runner = spawn(move || {
            let olap_tx
                = CRUDOperation::Range((u64::MIN..=u64::MAX).into(), snapshot);

            // let _tracked = manager.enq_bookkeeping(&olap_tx);
            loop {
                let _tx_res = manager.dispatch(olap_tx.clone());
                olaps_clone_1.fetch_add(1, Release);
            }
        });

        loop {
            if olaps_clone_2.load(Acquire) < number_olaps {
                thread::sleep(Duration::from_millis(1))
            } else {
                unsafe { let _e = libc::pthread_cancel(olap_runner.as_pthread_t()); }
                break
            }
        }
        // let _untracked = m_manager.deq_book_keeping(snapshot);
    });

    olaps
}

const CONFIG_PARAMETERS: &'static str = "config.json";
#[derive(Clone, Serialize, Deserialize)]
pub enum ClockType {
    FREE,
    OPT,
    SYNC,
}

impl Display for ClockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClockType::FREE => write!(f, "FREE"),
            ClockType::OPT => write!(f, "OPT"),
            ClockType::SYNC => write!(f, "SYNC"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GroupConfig {
    olap: Option<(SnapShot, u64)>,
    protocol: CRUDProtocol,
    clock: ClockType,
    range_start: u64,
    range_end: u64,
    lambda: f64,
    gc_enable: bool,
    threads: usize,
    total_tx: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    chain_groups: Vec<SubGroupConfig>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SubGroupConfig {
    olap: Option<(SnapShot, u64)>,
    range_start: u64,
    range_end: u64,
    lambda: f64,
    gc_enable: bool,
    threads: usize,
    total_tx: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
}

impl GroupConfig {
    fn is_valid(&self) -> bool {
        100 == self.insert_ratio
            + self.update_ratio
            + self.delete_ratio
            + self.point_reads_ratio
            + self.range_reads_ratio
            && self.threads > 1
            && self.protocol.is_mono_writer()
            && self.is_read_only()
            || self.threads == 1 && self.protocol.is_mono_writer()
            || !self.protocol.is_mono_writer()
    }

    fn index_handler(&self) -> IndexHandler {
        Either::Right(self.protocol.clone())
    }

    fn is_read_only(&self) -> bool {
        self.insert_ratio == 0 && self.update_ratio == 0 && self.delete_ratio == 0
    }

    fn is_write_only(&self) -> bool {
        self.point_reads_ratio == 0 && self.range_reads_ratio == 0
    }

    fn is_mix_read_write(&self) -> bool {
        !self.is_read_only() && !self.is_write_only()
    }

    fn num_chains(&self) -> usize {
        self.chain_groups.len()
    }
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            olap: None,
            chain_groups: vec![],
            protocol: Default::default(),
            clock: ClockType::FREE,
            range_start: 0,
            range_end: u64::MAX,
            lambda: 0.1,
            gc_enable: false,
            threads: 1,
            total_tx: 10_000_000,
            insert_ratio: 100,
            update_ratio: 0,
            delete_ratio: 0,
            point_reads_ratio: 0,
            range_reads_ratio: 0,
            range_size: 0,
        }
    }
}

impl Display for GroupConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.protocol,
            "_",
            self.range_start,
            self.range_end,
            self.lambda,
            "_",
            self.threads,
            self.insert_ratio,
            self.update_ratio,
            self.delete_ratio,
            self.point_reads_ratio,
            self.range_reads_ratio,
            self.range_size,
        )
    }
}

impl Display for SubGroupConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            "_",
            self.range_start,
            self.range_end,
            self.lambda,
            "_",
            self.threads,
            self.insert_ratio,
            self.update_ratio,
            self.delete_ratio,
            self.point_reads_ratio,
            self.range_reads_ratio,
            self.range_size,
        )
    }
}

type IndexHandler = Either<Arc<INDEX>, CRUDProtocol>;

fn load_config_experiments() -> Vec<GroupConfig> {
    match OpenOptions::new().read(true).open(CONFIG_PARAMETERS) {
        Ok(file) => serde_json::from_reader(file).unwrap_or_else(|error| {
            println!("JSON Error: {}", error);
            println!("Using default ConfigParameters");
            vec![GroupConfig::default()]
        }),
        Err(error) => {
            println!("File Error: {}", error);
            println!("Using default ConfigParameters");
            vec![GroupConfig::default()]
        }
    }
}

pub fn execute_experiments() {
    let groups
        = load_config_experiments();

    let total_exps = groups
        .iter()
        .fold(groups.len(), |acc, group| acc + group.num_chains());

    println!("[Loaded] - Experiments loaded #{total_exps}");
    println!("experiment_id,chain_id,tx_target,tx_executed,tx_success,tx_fail,time,protocol,clock,range_start,range_end,lambda,gc_enable,threads,insert_ratio,update_ratio,delete_ratio,point_reads_ratio,range_reads_ratio,range_size");
    groups
        .into_iter()
        .enumerate()
        .for_each(|(experiment_id, experiment)| {
            let mut olap_handle = None;
            let target_tx = experiment.total_tx;
            if let Some((snapshot, halt)) = experiment.olap {
                if let Either::Right(protocol) = experiment.index_handler() {
                    print!("{experiment_id},INIT_OLAP_s{snapshot}_n{halt},{target_tx}");
                    olap_handle = Some(olap(Either::Left(Arc::new(new_INDEX(protocol))), snapshot, halt));
                }
            }
            else {
                print!("{experiment_id},INIT,{target_tx}");
            }

            let mut index_handler
                = start_experiment_by_config(&experiment);

            while let (Some(olap_handle), Some((.., n_olaps)))
                = (&olap_handle, &experiment.olap)
            {
                if olap_handle.load(Acquire) < *n_olaps {
                    thread::yield_now();
                }
            }
            // drop(olap_handle.take());
            println!(",{experiment}");

            experiment
                .chain_groups
                .into_iter()
                .enumerate()
                .for_each(|(num, inner_group)| {
                    let subgroup = num + 1;
                    let target_tx = inner_group.total_tx;

                    if let Some((snapshot, halt)) = inner_group.olap {
                        print!("{experiment_id},{subgroup}_OLAP_s{snapshot}_t{halt},{target_tx}");
                        olap_handle = Some(olap(index_handler.clone(), snapshot, halt));
                    }
                    else {
                        print!("{experiment_id},{subgroup},{target_tx}");
                    }

                    // if let Either::Left(ref m_manager) = index_handler {
                    //     if inner_group.gc_enable && !m_manager.is_gc_enabled() {
                    //         m_manager.enable_gc();
                    //     } else if !inner_group.gc_enable && m_manager.is_gc_enabled() {
                    //         m_manager.disable_gc();
                    //     }
                    // }

                    index_handler
                        = chain_experiment_by_config(&inner_group, index_handler.clone());

                    while let (Some(olap_handle), Some((.., n_olaps)))
                        = (&olap_handle, &experiment.olap)
                    {
                        if olap_handle.load(Acquire) < *n_olaps {
                            thread::yield_now();
                        }
                    }
                    // drop(olap_handle.take());
                    println!(",{},{}", experiment.protocol, inner_group);
                });
        })
}

fn start_experiment_by_config(config: &GroupConfig) -> IndexHandler {
    run_experiment_with_params(
        config.threads,
        config.index_handler(),
        config.gc_enable,
        config.lambda,
        config.range_start,
        config.range_end,
        config.insert_ratio,
        config.update_ratio,
        config.delete_ratio,
        config.point_reads_ratio,
        config.range_reads_ratio,
        config.range_size,
        config.total_tx,
    )
}

fn chain_experiment_by_config(config: &SubGroupConfig, index_handler: IndexHandler) -> IndexHandler {
    run_experiment_with_params(
        config.threads,
        index_handler,
        config.gc_enable,
        config.lambda,
        config.range_start,
        config.range_end,
        config.insert_ratio,
        config.update_ratio,
        config.delete_ratio,
        config.point_reads_ratio,
        config.range_reads_ratio,
        config.range_size,
        config.total_tx,
    )
}

fn run_experiment_with_params(
    threads: usize,
    index: IndexHandler,
    gc_enable: bool,
    lambda: f64,
    range_start: u64,
    range_end: u64,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    total_tx: usize,
) -> IndexHandler {
    let total_tx_counter
        = Arc::new(AtomicUsize::new(0));

    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        lambda,
        range_start,
        range_end,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
    );

    while total_tx_counter.load(SeqCst) < total_tx {
        thread::yield_now();
    }

    let bulk_killer = handles
        .into_iter()
        .map(|(handle, killer)| {
            drop(killer);
            handle
        })
        .collect_vec();

    let result = bulk_killer
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect_vec();

    let mut total_time = 0;
    let mut total_success = 0;
    let mut total_error = 0;
    for (_index, (tx_success, tx_error, time)) in result.iter().enumerate() {
        // println!("\t[tid_{index}]: tx_success = {tx_success}, tx_error = {tx_error}, time = {time}");
        total_success += tx_success;
        total_error += tx_error;
        total_time = total_time.max(*time);
    }

    let total_executed_tx = total_success + total_error;

    print!(",{total_executed_tx},{total_success},{total_error},{total_time}");
    // println!("\t---------------------------------------------------------------------------------");
    // println!("\t[Summary] - Tx Executed = {total_executed_tx}, Target Tx = {total_tx}, Total Time = {total_time}");
    // println!("\t---------------------------------------------------------------------------------");

    index_handler
}

pub const FAN_OUT: usize = 255 - 5;
pub const NUM_RECORDS: usize = 107 - 5;

pub type Key = u64;
pub type Payload = u64;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

fn experiment(
    num_threads: usize,
    index_handler: IndexHandler,
    gc_enable: bool,
    lambda: f64,
    range_start: u64,
    range_end: u64,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    points_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    total_tx: Arc<AtomicUsize>,
) -> (
    IndexHandler,
    Vec<(JoinHandle<(usize, usize, u128)>, Sender<()>)>,
) {
    debug_assert_eq!(
        insert_ratio + update_ratio + delete_ratio + points_reads_ratio + range_reads_ratio,
        100,
        "Ratios must add to 100%"
    );

    #[inline(always)]
    fn gen_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut ThreadRng) -> u64 {
        #[inline(always)]
        fn sample_next(lambda: f64, rnd: &mut ThreadRng) -> f64 {
            let num = rnd.gen_range(0_f64..1_f64);

            (1_f64 - num).ln().div(-lambda)
        }
        let range = range_end - range_start;
        (((loop {
            let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
            if key >= 0_f64 {
                break key;
            }
        }) / range as f64)
            * u64::MAX as f64) as _
    }

    let manager = match index_handler {
        Either::Left(m_index) => m_index,
        Either::Right(protocol) => Arc::new(new_INDEX(protocol)),
    };

    type WorkerSignal = ();

    let handles = (0..num_threads)
        .map(|_| {
            let manager = manager.clone();

            let (thread_killer, thread_control)
                = bounded::<WorkerSignal>(0);

            let total_tx = total_tx.clone();

            // tx_success, tx_error, time_spent
            let handle = spawn(move || {
                let mut rng = thread_rng();

                let mut generator = || gen_key(range_end, range_start, range_end, lambda, &mut rng);

                let (mut tx_success, mut tx_error, start_execution_time) =
                    (0usize, 0usize, SystemTime::now());

                let local_tx = |key: u64| -> CRUDOperation<u64, u64> {
                    let random_number = thread_rng().gen_range(0..100);

                    if random_number < insert_ratio {
                        CRUDOperation::Insert(key, u64::default())
                    } else if random_number < insert_ratio + points_reads_ratio {
                        CRUDOperation::PointSi(key)
                    } else if random_number < insert_ratio + points_reads_ratio + range_reads_ratio
                    {
                        if u64::MAX - range_size <= key {
                            CRUDOperation::RangeSi((key..=u64::MAX).into())
                        } else {
                           CRUDOperation::RangeSi((key..key + range_size).into())
                        }
                    } else if random_number
                        < insert_ratio + points_reads_ratio + range_reads_ratio + delete_ratio
                    {
                        CRUDOperation::Delete(key)
                    } else {
                        CRUDOperation::Update(key, u64::default())
                    }
                };

                loop {
                    match thread_control.try_recv() {
                        Err(TryRecvError::Disconnected) => break,
                        _ => {
                            let next
                                = local_tx(generator());

                            match manager.dispatch(next) {
                                (_nv, CRUDOperationResult::Error) => tx_error += 1,
                                (_nv, _) => tx_success += 1,
                            }

                            total_tx.fetch_add(1, Relaxed);
                        }
                    }
                }

                (
                    tx_success,
                    tx_error,
                    SystemTime::now()
                        .duration_since(start_execution_time)
                        .unwrap()
                        .as_millis(),
                )
            });

            (handle, thread_killer)
        })
        .collect_vec();

    (IndexHandler::Left(manager), handles)
}

pub fn format_insertions(i: usize) -> String {
    if i % 1_000_000_000 == 0 {
        format!("{} B", i as f64 / 1_000_000_000_f64)
    } else if i % 1_000_000 == 0 {
        format!("{} Mio", i as f64 / 1_000_000_f64)
    } else if i % 1_000 == 0 {
        format!("{} K", i as f64 / 1_000_f64)
    } else {
        i.to_string()
    }
}
