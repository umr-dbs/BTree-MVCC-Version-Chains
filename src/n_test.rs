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
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::ops::Div;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::Arc;
use std::{fs, mem, thread};
use std::io::Write;
use std::os::unix::thread::JoinHandleExt;
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, SystemTime};
use rand::distr::{Alphanumeric, Uniform};
use rand::prelude::StdRng;
use rand_distr::Zipf;
use rand_distr::Distribution;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::CRUDProtocol;
use crate::page_model::node::Node;
use crate::record_model::Version;
use crate::tree::bplus_tree::{new_INDEX, BPlusTree};

pub fn run_olaps(handler: IndexHandler, number_workers: usize, number_olaps_per_worker: usize, n: usize)
                 -> Vec<JoinHandle<Vec<(SnapShot, u64, u128)>>>
{
    (0..number_olaps_per_worker)
        .map(|_| olap(handler.clone(), number_workers, n))
        .collect()
}

pub fn olap(index_handler: IndexHandler, number_olaps: usize, n: usize) -> JoinHandle<Vec<(SnapShot, u64, u128)>> {
    let index = index_handler
        .left()
        .expect("OLAP init failed! Provide an initialized TxManager!");

    spawn(move || {
        let uni_form
            = Uniform::new_inclusive(1_usize, n).unwrap();

        (0..number_olaps).map(|_| {
            let range_max
                = uni_form.sample(&mut rand::rng()) as Key;

            thread::sleep(Duration::from_millis(
                rand::random_range(1u64..100u64)));

            let si = index.committed_version() as Key;
            let time_start = SystemTime::now();
            let _crud_res = index.dispatch(CRUDOperation::Range(
                (index.min_key..=range_max).into(),
                si));
            (si, range_max, SystemTime::now().duration_since(time_start).unwrap().as_nanos())
        }).collect_vec()
    })
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
    olap_workers: usize,
    olaps_tx_per_worker: usize,
    protocol: CRUDProtocol,
    clock: ClockType,
    skew: f64,
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
    olap_workers: usize,
    olaps_tx_per_worker: usize,
    skew: f64,
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
            olap_workers: 0,
            olaps_tx_per_worker: 0,
            chain_groups: vec![],
            protocol: Default::default(),
            clock: ClockType::FREE,
            skew: 1f64,
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
            "{},{},{},{},{},{},{},{},{},{},{}",
            self.protocol,
            "_",
            self.skew,
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
            "{},{},{},{},{},{},{},{},{},{}",
            "_",
            self.skew,
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
    println!("experiment_id,\
    chain_id,\
    tx_target,\
    tx_executed,\
    tx_success,\
    tx_fail,\
    time,\
    protocol,\
    clock,\
    skew,
    gc_enable,\
    threads,\
    insert_ratio,\
    update_ratio,\
    delete_ratio,\
    point_reads_ratio,\
    range_reads_ratio,\
    range_size,\
    log_height,\
    actual_height,\
    blocks_allocated,\
    blocks_reused,\
    olaps_total_time,\
    olaps_workers,\
    olaps_per_worker");
    groups
        .into_iter()
        .enumerate()
        .for_each(|(experiment_id, experiment)| {
            let olap_start_time = SystemTime::now();
            let mut olap_handle = None;
            let mut index_handler = None;
            let init_target_tx = experiment.total_tx;
            if experiment.olap_workers > 0 {
                if let Either::Right(protocol) = experiment.index_handler() {
                    print!("{experiment_id},INIT,{init_target_tx}");
                    index_handler = Some(Either::Left(Arc::new(new_INDEX(protocol))));
                    olap_handle = Some(run_olaps(index_handler.clone().unwrap(),
                                                 experiment.olap_workers,
                                                 experiment.olaps_tx_per_worker,
                                                 init_target_tx));
                }
            }
            else {
                print!("{experiment_id},INIT,{init_target_tx}");
            }

            let mut index_handler
                = start_experiment_by_config(&experiment, index_handler);

            let mut olap_time = 0;
            if let Some(olap_handle) = olap_handle {
                let olap_data_result = olap_handle
                    .into_iter()
                    .flat_map(|jh| jh.join().unwrap())
                    .collect_vec();

                olap_time = SystemTime::now()
                    .duration_since(olap_start_time).unwrap().as_millis();

                let _nc = fs::remove_file(format!("ll_olap_{experiment_id}_INIT.csv"));
                let mut olap_file = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .write(true)
                    .open(format!("ll_olap_{experiment_id}_INIT.csv"))
                    .unwrap();

                olap_file.write_all(b"snapshot,range_end,latency\n").unwrap();
                for (si, range_max, olap_latency) in olap_data_result {
                    olap_file.write_all(format!("\
                                      {si},\
                                      {range_max},\
                                      {olap_latency}\n").as_bytes())
                        .unwrap();
                }
            }

            let (h, r) = height_root(&index_handler);
            let (alloc, reuse) = block_alloc_reuses(&index_handler);
            let (olap_w, olaps_per_w)
                = (experiment.olap_workers, experiment.olaps_tx_per_worker);
            println!(",{experiment},{h},{r},{alloc},{reuse},{olap_time},{olap_w},{olaps_per_w}");

            experiment
                .chain_groups
                .into_iter()
                .enumerate()
                .for_each(|(num, inner_group)| {
                    let subgroup = num + 1;
                    let target_tx = inner_group.total_tx;
                    let mut olap_handle = None;
                    let olap_start_time = SystemTime::now();

                    if inner_group.olap_workers > 0 {
                        print!("{experiment_id},{subgroup},{target_tx}");
                        olap_handle = Some(run_olaps(
                            index_handler.clone(), inner_group.olap_workers,
                            inner_group.olaps_tx_per_worker, 
                            init_target_tx));
                    }
                    else {
                        print!("{experiment_id},{subgroup},{target_tx}");
                    }

                    if let Either::Left(ref m_manager) = index_handler {
                        m_manager.block_manager.reset_alloc_reuse_counts();
                    }

                    index_handler
                        = chain_experiment_by_config(&inner_group, index_handler.clone());

                    let mut olap_time = 0;
                    if let Some(olap_handle) = olap_handle {
                        let olap_data_result = olap_handle
                            .into_iter()
                            .flat_map(|jh| jh.join().unwrap())
                            .collect_vec();

                        olap_time = SystemTime::now()
                            .duration_since(olap_start_time).unwrap().as_millis();

                        let _nc = fs::remove_file(format!("ll_olap_{experiment_id}_{subgroup}.csv"));
                        let mut olap_file = fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .write(true)
                            .open(format!("ll_olap_{experiment_id}_{subgroup}.csv"))
                            .unwrap();

                        olap_file.write_all(b"snapshot,range_end,latency\n").unwrap();
                        for (si, range_max, olap_latency) in olap_data_result {
                            olap_file.write_all(format!("\
                            {si},{range_max},{olap_latency}\n").as_bytes()).unwrap();
                        }
                    }
                    // drop(olap_handle.take());
                    
                    let (h, r) = height_root(&index_handler);
                    let (alloc, reuse) = block_alloc_reuses(&index_handler);
                    let (olap_w, olaps_per_w)
                        = (inner_group.olap_workers, inner_group.olaps_tx_per_worker);
                    println!(",{},{},{h},{r},{alloc},{reuse},{olap_time},{olap_w},{olaps_per_w}", experiment.protocol, inner_group);
                });
        })
}

fn start_experiment_by_config(config: &GroupConfig, index_handler: Option<IndexHandler>) -> IndexHandler {
    run_experiment_with_params(
        config.threads,
        index_handler.unwrap_or(config.index_handler()),
        config.gc_enable,
        config.skew,
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
        config.skew,
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
    skew: f64,
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
        skew,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
        total_tx
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
pub const DEBUG: bool = true;

pub const FAN_OUT: usize = 255;
pub const NUM_RECORDS: usize = 102;

pub type Key = u64;
// pub type Payload = PayloadIndirection;
pub type Payload = u64;

pub const PAYLOAD_STR_LEN_MIN: usize = 704;
pub const PAYLOAD_STR_LEN_MAX: usize = 7078;
pub const PAYLOAD_ATTR_STR_COUNT: usize = 67;

fn rnd_str(len_min: usize, len_max: usize) -> String {
    let len = rand::rng().random_range(len_min..=len_max);
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn rnd_str_vec(items: usize, str_len_min: usize, str_len_max: usize) -> Vec<String> {
    (0..items)
        .map(|i| rnd_str(str_len_min, str_len_max))
        .collect()
}
#[derive(Clone)]
pub struct PayloadIndirection(Box<PayloadData>);

#[derive(Clone)]
pub struct PayloadData {
    attributes: Vec<String>
}

impl PayloadData {
    pub fn attr(&self, i: usize) -> &str {
        self.attributes.get(i).unwrap()
    }
}

impl Default for PayloadIndirection {
    fn default() -> Self {
        Self(Box::new(PayloadData {
            attributes: rnd_str_vec(
                PAYLOAD_ATTR_STR_COUNT,
                PAYLOAD_STR_LEN_MIN,
                PAYLOAD_STR_LEN_MAX),
        }))
    }
}

impl Display for PayloadIndirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.attributes.join(", "))
    }
}

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
    skew: f64,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    points_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    total_tx: Arc<AtomicUsize>,
    n: usize
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
    fn gen_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut StdRng) -> u64 {
        #[inline(always)]
        fn sample_next(lambda: f64, rnd: &mut StdRng) -> f64 {
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
                let mut rng = rand::rng();
                let mut zipf = Zipf::new(n as f64, skew).unwrap();
                let mut generator = || zipf.sample(&mut rng) as Key;

                let (mut tx_success, mut tx_error, start_execution_time) =
                    (0usize, 0usize, SystemTime::now());

                let local_tx = |key: Key| -> CRUDOperation<Key, Payload> {
                    let random_number = rand::rng().random_range(0..100);

                    if random_number < insert_ratio {
                        CRUDOperation::Insert(key, Payload::default())
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
                        CRUDOperation::Update(key, Payload::default())
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

fn block_alloc_reuses(index_handler: &IndexHandler) -> (usize, usize) {
    if let Either::Left(index) = index_handler {
        (index.block_manager.alloc_count.load(SeqCst), 0)
    }
    else {
        unreachable!()
    }
}

fn height_root(index_handler: &IndexHandler) -> (usize, usize) {
    if let Either::Left(index) = index_handler {
        let log_height = index.root.height() as usize;
        let mut real_height = 1usize;

        let mut curr_block = index.root.block().clone();
        let mut curr_guard = curr_block.borrow_read();
        loop {
            match curr_guard.deref().unwrap().node_data {
                Node::Index(ref page) => unsafe {
                    curr_block = page.get_child_unsafe(0).clone();
                    curr_guard = curr_block.borrow_read();
                },
                _ => return (log_height, real_height),
            }
            real_height += 1;
        }
    }
    unreachable!()
}
