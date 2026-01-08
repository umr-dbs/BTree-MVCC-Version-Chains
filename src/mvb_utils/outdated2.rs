use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::{fs, mem, thread};
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::{Add, Deref, DerefMut, Div, RangeInclusive, Sub};
use std::path::Path;
use std::ptr::slice_from_raw_parts;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, SystemTime};
use crossbeam::channel::TryRecvError;
use hashbrown::HashMap;
use itertools::{all, Itertools};
use parking_lot::RwLock;
use rand::{Rng, RngCore, SeedableRng, thread_rng};
use rand::distributions::{Standard, Uniform};
use rand::rngs::StdRng;
use crate::block::block_manager::{_4KB, bsz_alignment};
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::locking::locking_strategy::{CRUDProtocol, LHL_read, LHL_write, LHL_read_write, LockingStrategy, OLC, orwc, orwc_attempts};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy::{LockCoupling, MonoWriter};
use crate::page_model::node::Node;
use crate::record_model::Version;
use crate::tree::bplus_tree::BPlusTree;

use crate::utils::interval::Interval;
use crate::utils::smart_cell::COUNTERS;


// fn seq_create() {
//     let insert: Key = 1_000_000;
//     let delete = 900_000;
//
//     let inserts = (1..=insert)
//         // .map(|key| CRUDOperation::Insert(key, Payload::default()))
//         .collect_vec();
//
//     let updates = inserts.clone();
//
//     let mut deletes = inserts.clone();
//     deletes.shuffle(&mut rand::rng());
//     deletes.truncate(delete);
//
//     let mut query = inserts
//         .into_iter()
//         .map(|key| CRUDOperation::Insert(key, Payload::default()))
//         .chain(updates
//             .into_iter()
//             .map(|update| CRUDOperation::Update(update, Payload::default())))
//         .collect_vec();
//
//     query.shuffle(&mut rand::rng());
//
//     fs::write("query.json", serde_json::to_string(query.as_slice()).unwrap()).unwrap();
//
// }
//
// fn seq_run() {
//     println!("Reading query....");
//     let query: Vec<CRUDOperation<Key, Payload>>
//         = serde_json::from_reader(fs::File::open("query.json").unwrap()).unwrap();
//
//     println!("Loaded query....");
// }


pub enum Sampler {
    Uniform(Uniform<u64>, ThreadRng),
    Zipf(Zipf<f64>, ThreadRng),
}

impl Sampler {
    pub fn new(skew: f64, n: Key) -> Self {
        if skew == 0_f64 {
            Sampler::Uniform(Uniform::new(0, n).unwrap(), rand::rng())
        }
        else {
            Sampler::Zipf(Zipf::new(n as f64, skew).unwrap(), rand::rng())
        }
    }
    #[inline(always)]
    pub fn sample(&mut self) -> Key {
        match self {
            Sampler::Uniform(dist, rng) =>
                dist.sample(rng) as Key,
            Sampler::Zipf(dist, rng) =>
                dist.sample(rng) as Key,
        }
    }
}

impl Display for Sampler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Sampler::Uniform(..) => write!(f, "Uniform"),
            Sampler::Zipf(..) => write!(f, "Zipf"),
        }
    }
}

pub fn run_olaps(handler: IndexHandler,
                 number_workers: usize,
                 number_olaps_per_worker: usize,
                 n: usize,
                 current_committed: Version
) -> Vec<JoinHandle<Vec<(SnapShot, RangeMax, OlapTime, CurrentVersionSI, SleepTime, ResultsCount)>>>
{
    let mut handles
        = Vec::with_capacity(number_workers);

    for i in 1..=number_workers as u64 {
        handles.push(olap(i, handler.clone(), number_olaps_per_worker, n));
    }

    handles
}

type CurrentVersionSI = SnapShot;
type RangeMax = Key;
type OlapTime = u128;
type SleepTime = u64;

type ResultsCount = usize;

const FIXED_RANGE_VAR_SI: bool              = false;
const FIXED_RANGE_INTERVAL: u64             = 10_000;

pub fn olap(olap_id: u64, index_handler: IndexHandler, number_olaps: usize, n: usize)
            -> JoinHandle<Vec<(SnapShot, RangeMax, OlapTime, CurrentVersionSI, SleepTime, ResultsCount)>> {
    let index = index_handler
        .left()
        .expect("OLAP Init. failed! Provide an initialized TxManager!");

    spawn(move || {
        let uni_form
            = Uniform::new(0_usize, n).unwrap();

        let mut olap_res
            = Vec::with_capacity(number_olaps);

        let mut current_version
            = index.committed_version();

        let mut range_max = FIXED_RANGE_INTERVAL;
        let mut sleep_time = 0;

        let si_steps = current_version / number_olaps as u64;
        let limit = if FIXED_RANGE_VAR_SI {
            match current_version % number_olaps as u64 == 0 {
                true => number_olaps as u64,
                false => number_olaps as u64 + 1,
            }
        }
        else {
            number_olaps as u64 - 1
        };

        for olap_id in 0..=limit {
            let mut si;

            if FIXED_RANGE_VAR_SI {
                si = si_steps * olap_id;
            }
            else {
                si = index.committed_version();
                // sleep_time = rand::random_range(1..=150);

                // thread::sleep(Duration::from_millis(sleep_time));

                current_version
                    = index.committed_version();

                si = rand::random_range(0..=si);

                range_max
                    = uni_form.sample(&mut rand::rng()) as RangeMax;
            }

            let time_start = SystemTime::now();
            let (_nv, crud_res) = index.dispatch(CRUDOperation::Range(
                (index.min_key..=range_max).into(),
                si));

            let time_spent = SystemTime::now().duration_since(time_start).unwrap().as_nanos();
            let matched_results_count
                = if let CRUDOperationResult::MatchedRecords(records) = crud_res {
                records.len()
            }
            else {
                unreachable!()
            };

            olap_res.push(
                (si,
                 range_max,
                 time_spent,
                 current_version,
                 sleep_time,
                 matched_results_count)
            );
        }

        olap_res
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
    olap_joint_workload: bool,
    olap_workers: usize,
    olaps_tx_per_worker: usize,
    protocol: CRUDProtocol,
    v_index_type: VersionIndexType,
    clock: ClockType,
    skew: f64,
    skew_n: usize,
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
    olap_joint_workload: bool,
    olap_workers: usize,
    olaps_tx_per_worker: usize,
    skew: f64,
    skew_n: usize,
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
        Either::Right((self.protocol.clone(), self.v_index_type))
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
            olap_joint_workload: false,
            olap_workers: 0,
            olaps_tx_per_worker: 0,
            chain_groups: vec![],
            protocol: Default::default(),
            v_index_type: VersionIndexType::VANILLA,
            clock: ClockType::FREE,
            skew: 1f64,
            skew_n: 10000,
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
            self.v_index_type,
            "_",
            self.skew,
            self.skew_n,
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
            self.skew,
            self.skew_n,
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

type IndexHandler = Either<Arc<INDEX>, (CRUDProtocol, VersionIndexType)>;

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
    println!("main_index,\
    experiment_id,\
    chain_id,\
    tx_target,\
    tx_executed,\
    tx_success,\
    tx_fail,\
    time,\
    protocol,\
    mvb_version_index,\
    clock,\
    skew,\
    skew_n,\
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
    olaps_per_worker,\
    olaps_avg_sleep_time,\
    olaps_joint_workload,\
    total_running_time");
    groups
        .into_iter()
        .enumerate()
        .for_each(|(experiment_id, experiment)| {
            // unsafe {
            //     unforce_read_success()
            // }

            let mut olap_handle = None;
            let mut index_handler = None;
            let init_target_tx = experiment.total_tx;
            let mut total_running_time = 0u128;
            let v_index_prefix = match experiment.v_index_type {
                VersionIndexType::VANILLA => "btree_ll",
                VersionIndexType::SkipList => "btree_sk",
                VersionIndexType::FrugalSkipList => "btree_fg",
                VersionIndexType::BTree => "btree_btree",
                VersionIndexType::VWEAVER => "btree_vweaver"
            };
            if experiment.olap_workers > 0 {
                if let Either::Right((protocol, v_index_kind)) = experiment.index_handler() {
                    print!("{SYSTEM_STR},{experiment_id},INIT,{init_target_tx}");
                    index_handler = Some(Either::Left(Arc::new(new_INDEX(protocol, v_index_kind))));
                    olap_handle = Some(run_olaps(index_handler.clone().unwrap(),
                                                 experiment.olap_workers,
                                                 experiment.olaps_tx_per_worker,
                                                 init_target_tx, 0));
                }
            }
            else {
                print!("{SYSTEM_STR},{experiment_id},INIT,{init_target_tx}");
            }

            let terminate_workload = match olap_handle {
                Some(..) => Some(Arc::new(AtomicBool::new(false))),
                _ => None
            };
            let terminate_clone
                = terminate_workload.clone();

            let handler_clone
                = index_handler.clone();

            let exp_clone
                = experiment.clone();

            let mut start_time = SystemTime::now();
            let sp_index_handler
                = spawn(move || start_experiment_by_config(&exp_clone, handler_clone, terminate_clone));

            let mut total_olap_time = 0;
            let mut avg_olap_sleep_time = 0;
            if let Some(olap_handle) = olap_handle {
                let olap_data_result = olap_handle
                    .into_iter()
                    .flat_map(|jh| jh.join().unwrap())
                    .map(|t@(.., olap_time, olap_sleep_time, _)| {
                        total_olap_time += olap_time;
                        avg_olap_sleep_time += olap_sleep_time;
                        t
                    }).collect_vec();

                terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                index_handler = Some(sp_index_handler.join().unwrap());

                total_running_time = SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_millis();

                total_olap_time /= 1_000_000;
                avg_olap_sleep_time /= experiment.olap_workers as CurrentVersionSI;
                avg_olap_sleep_time /= experiment.olaps_tx_per_worker as CurrentVersionSI;

                let _nc = fs::remove_file(format!("{v_index_prefix}_olap_{experiment_id}_INIT.csv"));
                let mut olap_file = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .write(true)
                    .open(format!("{v_index_prefix}_olap_{experiment_id}_INIT.csv"))
                    .unwrap();

                olap_file.write_all(b"target_snapshot,current_snapshot,sleep_time,range_end,count_results,latency\n").unwrap();
                for (si, range_max, olap_latency, curr_si, sleep_time, count) in olap_data_result {
                    olap_file.write_all(format!("\
                                      {si},\
                                      {curr_si},\
                                      {sleep_time},\
                                      {range_max},\
                                      {count},\
                                      {olap_latency}\n").as_bytes())
                        .unwrap();
                }
            }
            else {
                terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                index_handler = Some(sp_index_handler.join().unwrap());
                total_running_time = SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_millis();
            }

            let mut index_handler
                = index_handler.unwrap();

            let (h, r) = height_root(&index_handler);
            let (alloc, reuse) = block_alloc_reuses(&index_handler);
            let (olap_w, olaps_per_w, olaps_joint_workload)
                = (experiment.olap_workers, experiment.olaps_tx_per_worker, experiment.olap_joint_workload);

            println!(",{experiment},{h},{r},{alloc},{reuse},\
            {total_olap_time},{olap_w},{olaps_per_w},{avg_olap_sleep_time},{olaps_joint_workload},{total_running_time}");
            experiment
                .chain_groups
                .into_iter()
                .enumerate()
                .for_each(|(num, inner_group)| {
                    // unsafe {
                    //     force_read_success()
                    // }

                    let subgroup = num + 1;
                    let target_tx = inner_group.total_tx;
                    let mut olap_handle = None;

                    if inner_group.olap_workers > 0 {
                        print!("{SYSTEM_STR},{experiment_id},{subgroup},{target_tx}");
                        let si = index_handler.as_ref().left().unwrap().committed_version();
                        olap_handle = Some(run_olaps(
                            index_handler.clone(), inner_group.olap_workers,
                            inner_group.olaps_tx_per_worker,
                            init_target_tx, si));
                    }
                    else {
                        print!("{SYSTEM_STR},{experiment_id},{subgroup},{target_tx}");
                    }

                    if let Either::Left(ref m_manager) = index_handler {
                        m_manager.block_manager.reset_alloc_reuse_counts();
                    }

                    let terminate_workload = match olap_handle {
                        Some(..) => Some(Arc::new(AtomicBool::new(false))),
                        _ => None
                    };
                    let terminate_clone
                        = terminate_workload.clone();

                    let exp_clone
                        = inner_group.clone();

                    let handle_clone
                        = index_handler.clone();

                    start_time = SystemTime::now();
                    let sp_index_handler
                        = spawn(move || chain_experiment_by_config(&exp_clone, handle_clone, terminate_clone));

                    let mut total_olap_time = 0;
                    let mut avg_olap_sleep_time = 0;
                    if let Some(olap_handle) = olap_handle {
                        let olap_data_result = olap_handle
                            .into_iter()
                            .flat_map(|jh| jh.join().unwrap())
                            .map(|t@(.., olap_time, olap_sleep_time, _)| {
                                total_olap_time += olap_time;
                                avg_olap_sleep_time += olap_sleep_time;
                                t
                            }).collect_vec();

                        terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                        index_handler = sp_index_handler.join().unwrap();

                        total_running_time
                            = SystemTime::now().duration_since(start_time).unwrap().as_millis();

                        total_olap_time /= 1_000_000;
                        avg_olap_sleep_time /= inner_group.olap_workers as CurrentVersionSI;
                        avg_olap_sleep_time /= inner_group.olaps_tx_per_worker as CurrentVersionSI;

                        let _nc = fs::remove_file(format!("{v_index_prefix}_olap_{experiment_id}_{subgroup}.csv"));
                        let mut olap_file = fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .write(true)
                            .open(format!("{v_index_prefix}_olap_{experiment_id}_{subgroup}.csv"))
                            .unwrap();

                        olap_file.write_all(b"target_snapshot,current_snapshot,sleep_time,range_end,count_results,latency\n").unwrap();
                        for (si, range_max, olap_latency, curr_si, sleep_time, count) in olap_data_result {
                            olap_file.write_all(format!("\
                            {si},\
                            {curr_si},\
                            {sleep_time},\
                            {range_max},\
                            {count},\
                            {olap_latency}\n").as_bytes()).unwrap();
                        }
                    }
                    else {
                        terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                        index_handler = sp_index_handler.join().unwrap();
                        total_running_time = SystemTime::now()
                            .duration_since(start_time)
                            .unwrap()
                            .as_millis();
                    }
                    // drop(olap_handle.take());

                    let (h, r) = height_root(&index_handler);
                    let (alloc, reuse) = block_alloc_reuses(&index_handler);
                    let (olap_w, olaps_per_w, olaps_joint_workload)
                        = (inner_group.olap_workers, inner_group.olaps_tx_per_worker, inner_group.olap_joint_workload);


                    println!(",{},{},{},{},{h},{r},{alloc},{reuse},\
                    {total_olap_time},{olap_w},{olaps_per_w},{avg_olap_sleep_time},{olaps_joint_workload},{total_running_time}",
                             experiment.protocol,
                             experiment.v_index_type,
                             experiment.clock,
                             inner_group);
                });
        })
}

fn start_experiment_by_config(
    config: &GroupConfig,
    index_handler: Option<IndexHandler>,
    terminate_workload: Option<Arc<AtomicBool>>) -> IndexHandler
{
    if terminate_workload.is_some() {
        run_experiment_with_params_until(
            config.threads,
            index_handler.unwrap_or(config.index_handler()),
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            terminate_workload.unwrap()
        )
    }
    else {
        run_experiment_with_params(
            config.threads,
            index_handler.unwrap_or(config.index_handler()),
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            config.total_tx,
        )
    }
}

fn chain_experiment_by_config(
    config: &SubGroupConfig,
    index_handler: IndexHandler,
    terminate_workload: Option<Arc<AtomicBool>>) -> IndexHandler
{
    if terminate_workload.is_some() {
        run_experiment_with_params_until(
            config.threads,
            index_handler,
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            terminate_workload.unwrap()
        )
    }
    else {
        run_experiment_with_params(
            config.threads,
            index_handler,
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            config.total_tx,
        )
    }
}


fn run_experiment_with_params_until(
    threads: usize,
    index: IndexHandler,
    gc_enable: bool,
    skew: f64,
    skew_n: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    terminate: Arc<AtomicBool>
) -> IndexHandler {
    let total_tx_counter
        = Arc::new(AtomicUsize::new(0));

    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        skew,
        skew_n,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
    );

    while !terminate.load(SeqCst) {
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


fn run_experiment_with_params(
    threads: usize,
    index: IndexHandler,
    gc_enable: bool,
    skew: f64,
    skew_n: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    limit_tx: usize,
) -> IndexHandler {
    let total_tx_counter
        = Arc::new(AtomicUsize::new(0));

    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        skew,
        skew_n,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
    );

    while total_tx_counter.load(SeqCst) < limit_tx {
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

// pub const FAN_OUT: usize = 255;
// pub const NUM_RECORDS: usize = 102;

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
    skew_n: usize,
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
        Either::Right((protocol, v_index_kind)) =>
            Arc::new(new_INDEX(protocol, v_index_kind)),
    };

    type WorkerSignal = ();

    let is_nop =
        insert_ratio == 0 &&
            delete_ratio == 0 &&
            update_ratio == 0 &&
            points_reads_ratio == 0 &&
            range_reads_ratio == 0;

    let handles = (0..num_threads)
        .map(|_| {
            let manager = manager.clone();

            let (thread_killer, thread_control)
                = bounded::<WorkerSignal>(0);

            let total_tx = total_tx.clone();

            // tx_success, tx_error, time_spent
            let handle = spawn(move || {
                let mut sampler
                    = Sampler::new(skew, skew_n as Key);

                let (mut tx_success, mut tx_error, start_execution_time) =
                    (0usize, 0usize, SystemTime::now());

                let random_number
                    = rand::rng().random_range(0..100);

                let local_tx = move |key: Key| -> CRUDOperation<Key, Payload> {
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
                        _ if is_nop => thread::sleep(Duration::from_millis(1)),
                        _ => {
                            let next
                                = local_tx(sampler.sample());

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

const SYSTEM_STR: &str = "B+Tree";

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

fn bernhard_tests() {
    const INSERTIONS: Key = 10_000;
    const UPDATES: Key = 100_000_000 as Key;
    const DELETIONS: f64 = 0.9_f64;
    const NUMBER_OLAPS: usize = 1;
    const NUMBER_UPDATERS: usize = 0;
    const OLAP_TX_PER_WORKER: usize = 2000;
    const RANGE_SIZE: Key = 1_000;
    const SKEWs: [f64; 3] = [0f64, 0.4, 1.4];

    const V_INDEX: VersionIndexType = VersionIndexType::SkipList;

    let deletions_number = (DELETIONS * INSERTIONS as f64) as usize;
    println!(
        "\t- Inserts = {}\n\t- Updates = {}\n\t- Deletions = {} ({}% of keys)",
        format_insertions(INSERTIONS as _),
        format_insertions(UPDATES as _),
        format_insertions(deletions_number),
        DELETIONS * 100.0
    );

    for skew in SKEWs {
        println!("\t- Skew = {}\n\t- ####################################################", skew);
        let btree = new_INDEX(OLC, V_INDEX);

        let mut data_inserts = (0..INSERTIONS).collect_vec();

        data_inserts.shuffle(&mut rand::rng());

        data_inserts.iter().for_each(|key| {
            let (_nv, crud_ins)
                = btree.dispatch(CRUDOperation::Insert(*key, *key));

            match crud_ins {
                CRUDOperationResult::Inserted(..) => {}
                _ => panic!("Error in Inserted crud"),
            }
        });

        let mut sampler
            = Sampler::new(skew, INSERTIONS - 1);

        (0..UPDATES).for_each(|_| {
            let crud = CRUDOperation::Update(sampler.sample(), Payload::default());
            let (_nv, crud_update)
                = btree.dispatch(crud.clone());

            match crud_update {
                CRUDOperationResult::Updated(..) => {}
                _ => panic!("Error in Updated crud = {crud}"),
            }
        });

        let mut deletes = data_inserts.clone();
        deletes.shuffle(&mut rand::rng());
        deletes.truncate(deletions_number);

        deletes.into_iter().for_each(|key| {
            let (_nv, crud_ins) = btree.dispatch(CRUDOperation::Delete(key));

            match crud_ins {
                CRUDOperationResult::Deleted(..) => {}
                _ => panic!("Error in Deleted crud"),
            }
        });

        mem::drop(data_inserts);

        println!(
            "\t- BTree Init. \n\t- \
    [{NUMBER_OLAPS}] OLAPs starting with [{OLAP_TX_PER_WORKER}] transactions per worker."
        );

        // Start OLAPs here
        let index = Arc::new(btree);
        let mut olaps = vec![];

        let _nc = fs::remove_file(format!("btree_{V_INDEX}_olap_skew_{skew}.csv"));
        let mut olap_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(format!("btree_{V_INDEX}_olap_skew_{skew}.csv"))
            .unwrap();

        olap_file
            .write_all(
                b"target_snapshot,\
            current_snapshot,\
            sleep_time,\
            range_start,\
            range_end,\
            count_results,\
            latency\n",
            )
            .unwrap();

        let mut updaters = vec![];
        for _ in 0..NUMBER_UPDATERS {
            let index = index.clone();

            let (sender, receiver)
                = std::sync::mpsc::channel::<()>();

            updaters.push((sender, spawn(move || {
                let mut sampler
                    = Sampler::new(skew, INSERTIONS - 1);

                loop {
                    match receiver.try_recv() {
                        Err(..) => break,
                        _ => {
                            index.dispatch(CRUDOperation::Update(sampler.sample(), Payload::default()));
                        }
                    }
                }
            })))
        }
        for _ in 0..NUMBER_OLAPS {
            let index = index.clone();
            olaps.push(spawn(move || {
                let mut results = vec![];
                for _ in 1..OLAP_TX_PER_WORKER {
                    let mut key_min
                        = rand::random_range(0..INSERTIONS);

                    let mut key_max
                        = key_min + RANGE_SIZE;

                    if key_max >= INSERTIONS {
                        key_max = key_min;
                        key_min -= RANGE_SIZE;
                    }

                    let current_si
                        = index.committed_version();

                    let si
                        = rand::random_range(1..=current_si);

                    let time_start
                        = SystemTime::now();

                    let (_nv, crud) =
                        index.dispatch(CRUDOperation::Range((key_min, key_max).into(), si));

                    let time_spent
                        = SystemTime::now().duration_since(time_start).unwrap().as_nanos();

                    let count_results =  match crud {
                        CRUDOperationResult::MatchedRecords(data) =>  data.len(),
                        _ => 0
                    };
                    results.push(
                        (si, current_si, 0u128, key_min, key_max, count_results, time_spent)
                    )
                }
                results
            }))
        }

        let olaps = olaps.into_iter().map(|j| j.join().unwrap())
            .flatten()
            .collect::<Vec<_>>();

        // mem::drop(updaters);

        olaps.into_iter()
            .for_each(|(target_si,
                           current_si,
                           sleep_time,
                           key_min,
                           key_max,
                           count_results,
                           time_spent)|
                {
                    olap_file.write_all(format!("\
                            {target_si},\
                            {current_si},\
                            {sleep_time},\
                            {key_min},\
                            {key_max},\
                            {count_results},\
                            {time_spent}\n").as_bytes()).unwrap();
                })
    }
}

pub const VALIDATE_OPERATION_RESULT: bool = false;
pub const EXE_LOOK_UPS: bool = false;
pub const EXE_RANGE_LOOK_UPS: bool = false;

pub const BSZ_BASE: usize = _4KB;
pub const BSZ: usize = BSZ_BASE - bsz_alignment::<Key, Payload>();
pub const FAN_OUT: usize = BSZ / 8 / 2;
pub const NUM_RECORDS: usize = (BSZ - 2) / (5 * 8);

// pub const FAN_OUT: usize = 16;
// pub const NUM_RECORDS: usize = 16;

// pub const NUM_RECORDS: usize = 64;

pub type Key = u64;
pub type Payload = u64;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}



pub const TREE: fn(CRUDProtocol) -> Tree = |crud| {
    Arc::new(if let MonoWriter = crud {
        TreeDispatcher::Wrapper(RwLock::new(MAKE_INDEX(crud)))
    } else {
        TreeDispatcher::Ref(MAKE_INDEX(crud))
    })
};

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_with(ls, Key::MIN, Key::MAX, inc_key, dec_key);

pub type Tree = Arc<TreeDispatcher>;

pub enum TreeDispatcher {
    Wrapper(RwLock<INDEX>),
    Ref(INDEX),
}

impl CRUDDispatcher<Key, Payload> for TreeDispatcher {
    #[inline(always)]
    fn dispatch(&self, crud: CRUDOperation<Key, Payload>) -> (NodeVisits, CRUDOperationResult<Key, Payload>) {
        match self {
            TreeDispatcher::Ref(inner) => inner.dispatch(crud),
            TreeDispatcher::Wrapper(sync) => if crud.is_read() {
                sync.read().dispatch(crud)
            } else {
                sync.write().dispatch(crud)
            }
        }
    }
}

// unsafe impl Send for TreeDispatcher {}
// unsafe impl Sync for TreeDispatcher {}

impl TreeDispatcher {
    pub fn as_index(&self) -> &INDEX {
        match self {
            TreeDispatcher::Wrapper(inner) => unsafe { &*inner.data_ptr() },
            TreeDispatcher::Ref(inner) => inner
        }
    }
}

pub fn dump_to_json(tree: Tree) {
    const VERSION_STAR: Version = Version::MAX - 1;
    let (nv, data)
        = tree.dispatch(CRUDOperation::Range((Key::MIN..=Key::MAX).into(), VERSION_STAR));

    if let CRUDOperationResult::MatchedRecords(all_data) = data {
        println!("Node Visits: {}, Records: {}", format_insertions(nv), format_insertions(all_data.len()));

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("vlists.json")
            .unwrap();

        let data = all_data
            .iter()
            .map(|r| r.key)
            .collect_vec();

        serde_json::to_writer(file, data.as_slice()).unwrap();
    }
}

#[inline(always)]
pub fn bulk_crud(worker_threads: usize, tree: Tree, operations_queue: &[CRUDOperation<Key, Payload>]) -> (u128, u64, NodeVisits) {
    let mut data_buff = operations_queue
        .iter()
        .chunks(operations_queue.len() / worker_threads)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > worker_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(worker_threads);

    let start = SystemTime::now();
    for _ in 1..=worker_threads {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = tree.clone();
        handles.push(spawn(move || {
            let mut counter_errs = 0;
            let mut node_visits = 0;
            current_chunk
                .into_iter()
                .for_each(|next_query| match index.dispatch(next_query) { // mvb_tree.execute(operation),
                    (visits, CRUDOperationResult::Error) => {
                        counter_errs += 1;
                        node_visits += visits;
                    }
                    (visits, ..) => node_visits += visits
                });
            (counter_errs, node_visits)
        }));
    }

    let (dups, node_visits) = handles
        .into_iter()
        .map(|handle| handle
            .join()
            .unwrap()
        ).fold((0, 0), |(errors, visits), (n_e, n_v)| (errors + n_e, visits + n_v));

    let time_elapsed
        = SystemTime::now().duration_since(start).unwrap();

    (time_elapsed.as_millis(), dups, node_visits)
}

fn make_leaf_hits_map(tree: Tree) -> Vec<(Interval<Key>, usize)> {
    let retrieve_fence_right = |key: Key| {
        let mut fence_right = Key::MAX;
        let mut node = tree.as_index().root.block.unsafe_borrow().as_ref();

        loop {
            match node {
                Node::Index(index_page) => unsafe {
                    match index_page.keys().binary_search(&key) {
                        Ok(pos) => {
                            if index_page.keys().len() > pos + 1 {
                                fence_right = *index_page.keys().get(pos + 1).unwrap();
                            }

                            // make sure no child from here has the key
                            if index_page.get_child_unsafe(pos).unsafe_borrow().is_leaf() {
                                break fence_right;
                            } else {
                                node = index_page.get_child_unsafe(pos).unsafe_borrow().as_ref();
                            }
                        }
                        Err(pos) => {
                            let key_pos = if pos >= index_page.keys_len() {
                                pos - 1
                            } else {
                                pos
                            };
                            fence_right = *index_page.keys().get(key_pos).unwrap();
                            node = index_page.get_child_unsafe(pos).unsafe_borrow().as_ref()
                        }
                    }
                }
                _ => break fence_right
            }
        }
    };

    let mut map
        = Vec::new();

    let mut queue = VecDeque::new();
    queue.push_back(tree.as_index().root.block.unsafe_borrow().as_ref());

    let mut start = 0;
    while !queue.is_empty() {
        let next = queue.pop_front().unwrap();

        match next.as_ref() {
            Node::Index(index_page) => unsafe {
                index_page
                    .children()
                    .iter()
                    .filter(|c| c.unsafe_borrow().is_directory())
                    .for_each(|child|
                        queue.push_back(child.unsafe_borrow().as_ref()));

                if index_page.get_child_unsafe(0).0.as_ref().is_leaf() {
                    let mut prev: Interval<Key> = Default::default();
                    index_page
                        .keys()
                        .iter()
                        .chain([retrieve_fence_right(*index_page.keys().last().unwrap())].as_slice())
                        .for_each(|k| {
                            prev = (start, *k).into();
                            map.push((prev.clone(), 0usize));
                            start = *k + 1;
                        });

                    if queue.is_empty() {
                        let (val, _)
                            = map.last_mut().unwrap();

                        // right most node we ignore father fence, since it must be max
                        val.set_upper(Key::MAX);
                    }
                }
            }
            _ => {}
        }
    }

    map
}

pub fn start_paper_tests() {
    const MAKE_HIST: bool
    = false;

    const RQ_ENABLED: bool
    = true;

    const N: u64
    = 10_000_000;

    const KEY_RANGE: RangeInclusive<Key>
    = 1..=N;

    const REPEATS: usize
    = 3;

    const UPDATES_THRESHOLD: [f64; 4] = [
        0.0_f64,
        0.1_f64,
        0.5_f64,
        0.9_f64,
        // 1_f64
    ];

    const THREADS: [usize; 9]
    = [1, 2, 4, 8, 12, 16, 32, 64, 128];

    const LAMBDAS: [f64; 4]
    = [
        0.1_f64,
        // 0.8_f64,
        // 4_f64,
        8_f64,
        // 16_f64,
        // 24_f64,
        // 32_f64,
        // 48_f64,
        // 64_f64,
        // 72_f64,
        // 96_f64,
        128_f64,
        // 256_f64,
        // 512_f64,
        1024_f64
    ];

    const RQ_PROBABILITY: [f64; 1]
    = [1.0];

    const RQ_OFFSET: [u64; 3] = [
        1 * (NUM_RECORDS as u64 + 1_u64),
        16 * (NUM_RECORDS as u64 + 1_u64),
        128 * (NUM_RECORDS as u64 + 1_u64),
    ];

    let file = OpenOptions::new()
        .read(true)
        .open("/home/amir/Schreibtisch/100k.json")
        .unwrap();

    let data_lambdas: Vec<u64>
        = serde_json::from_reader(file).unwrap();

    let data_lambdas: Vec<Vec<CRUDOperation<Key, Payload>>> = vec![data_lambdas
        .into_iter()
        .map(|v| CRUDOperation::Insert(v, 0))
        .collect_vec()];

    // let data_lambdas = LAMBDAS
    //     .iter()
    //     .map(|lambda| {
    //         let mut rnd = StdRng::seed_from_u64(90501960);
    //         gen_data_exp(N, *lambda, &mut rnd)
    //             .into_iter()
    //             .map(|key| CRUDOperation::Insert(key, Payload::default()))
    //             .collect::<Vec<_>>()
    //     }).collect::<Vec<_>>();

    if MAKE_HIST {
        for lambda in 0..LAMBDAS.len() {
            println!("[Lambda={}] -\tStep 1/3: Creating mvb_tree with '{}' keys via '{}' threads ..",
                     LAMBDAS[lambda],
                     format_insertions(N as usize),
                     num_cpus::get_physical());

            let tree
                = TREE(OLC());

            let (_create_time, _errs, _visits) = bulk_crud(
                num_cpus::get_physical(),
                tree.clone(),
                data_lambdas[lambda].as_slice(),
            );

            println!("[Lambda={}] -\tStep 1/3: Tree creation completed.",
                     LAMBDAS[lambda]);

            println!("[Lambda={}] -\tStep 2/3: Creating hits, min = {}, max = {}, avg = {} keys/leaf (total leafs = {}) ..",
                     LAMBDAS[lambda],
                     NUM_RECORDS / 2,
                     NUM_RECORDS,
                     (NUM_RECORDS - (NUM_RECORDS / 4)),
                     format_insertions(N as usize / (NUM_RECORDS - (NUM_RECORDS / 4))));

            let mut map
                = make_leaf_hits_map(tree);

            println!("[Lambda={}] -\tStep 2/3: Hits map creation completed.",
                     LAMBDAS[lambda]);

            println!("[Lambda={}] -\tStep 3/3: Creating histogram from hits map ..",
                     LAMBDAS[lambda]);

            make_hist(LAMBDAS[lambda], &mut map, N, KEY_RANGE);
            println!("[Lambda={}] -\tStep 3/3: Histogram completed.\n##############################################\n",
                     LAMBDAS[lambda]);
        }

        println!("All histograms completed.");
        return;
    }

    let protocols = [
        // MonoWriter,
        // LockCoupling,
        // orwc_attempts(0),
        // orwc_attempts(1),
        orwc_attempts(4),
        // orwc_attempts(16),
        OLC(),
        // OLC(),

        // LHL_read(0),
        // LHL_read(1),
        // LHL_read(4),
        // LHL_read(16),

        // LHL_write(0),
        // LHL_write(1),
        // LHL_write(4),
        // LHL_write(16),
        // LHL_read_write(0, 0),
        // LHL_read_write(1, 1),
        // LHL_read_write(4, 4),
        // LHL_read_write(16, 16),
        // hybrid_lock(),
    ];

    println!("Records,Threads,Protocol,Create Time,Create Node Visits,Create Duplicates,Lambda,Run,\
    Mixed Time,Mixed Node Visits,U-TH,Updates,Reads,Ranges,Range Offset,RQ-TH,Total,Leaf Size");
    // println!("Protocol,PAUSE,sched_yield,lambda,threads");

    for protocol in protocols {
        let tree
            = TREE(protocol.clone());

        let (create_time, errs, create_node_visits)
            = bulk_crud(num_cpus::get_physical(),
                        tree.clone(),
                        data_lambdas[0].as_slice());

        println!("Starting JSON Serializer...");
        dump_to_json(tree);
        println!("Finished JSON Serializer!");

        return;
        for lambda in 0..LAMBDAS.len() {
            for thread in THREADS {
                for ut in UPDATES_THRESHOLD {
                    if RQ_ENABLED {
                        for rq in RQ_PROBABILITY {
                            for rq_off in RQ_OFFSET {
                                mixed_test_new(
                                    create_node_visits,
                                    create_time,
                                    errs,
                                    protocol.clone(),
                                    tree.clone(),
                                    N,
                                    KEY_RANGE.clone(),
                                    thread,
                                    LAMBDAS[lambda],
                                    REPEATS,
                                    ut,
                                    rq,
                                    rq_off)
                            }
                        }
                    } else {
                        mixed_test_new(
                            create_node_visits,
                            create_time,
                            errs,
                            protocol.clone(),
                            tree.clone(),
                            N,
                            KEY_RANGE.clone(),
                            thread,
                            LAMBDAS[lambda],
                            REPEATS,
                            ut,
                            0.0,
                            0)
                    }
                }
            }
        }
    }
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

fn make_hist(lambda: f64, map: &mut Vec<(Interval<Key>, usize)>, n: u64, key_range: RangeInclusive<Key>) {
    let stats_lambda_leaf_hits
        = format!("leaf_hits_lambda_{}.csv", lambda);

    fs::remove_file(stats_lambda_leaf_hits.as_str());

    // map.values_mut().for_each(|count| *count = 0);

    let mut rnd
        = StdRng::seed_from_u64(0x3A5F72B9C81D4EF2);

    let mut gen_key = || gen_rand_key(
        n,
        *key_range.start(),
        *key_range.end(),
        lambda,
        &mut rnd);

    let mut leaf_hits = |key| {
        match map.binary_search_by_key(&key, |(i, _)| i.upper) {
            Ok(pos) | Err(pos) => {
                let (.., i)
                    = map.get_mut(pos).unwrap();

                *i = *i + 1;
            }
        }
    };

    (0..n as usize)
        .for_each(|_| leaf_hits(gen_key()));

    assert_eq!(map.last().unwrap().0.upper, Key::MAX);
    assert_eq!(map.first().unwrap().0.lower, Key::MIN);

    let mut s = "Low,High,Count,Leaf Size,N\n".to_string();
    s.push_str(map
        .as_slice()
        .iter()
        .map(|(i, c)| format!("{},{},{},{},{}", i.lower, i.upper, c, NUM_RECORDS, n))
        .join("\n")
        .as_str());

    fs::write(stats_lambda_leaf_hits, s).unwrap();
}

fn mixed_test_new(
    create_node_visits: NodeVisits,
    create_time: u128,
    dups: u64,
    ls: CRUDProtocol,
    tree: Tree,
    n: u64,
    key_range: RangeInclusive<Key>,
    threads: usize,
    lambda: f64,
    runs: usize,
    updates_thresh_hold: f64,
    rq_probability: f64,
    rq_offset: Key,
) {
    let operations_count
        = n as usize;

    let operation_per_thread
        = operations_count / threads;

    let mut rnd
        = StdRng::seed_from_u64(0x3A5F72B9C81D4EF2);

    let mut gen_key = || gen_rand_key(
        n,
        *key_range.start(),
        *key_range.end(),
        lambda,
        &mut rnd);

    let operations = thread_rng()
        .sample_iter(Uniform::new(0_f64, 1_f64))
        .take(operations_count)
        .collect::<Vec<_>>()
        .into_iter()
        .map(|t| {
            let key
                = gen_key();

            if t <= updates_thresh_hold {
                CRUDOperation::Update(key, Payload::default())
            } else {
                if thread_rng().gen_bool(rq_probability) {
                    match key.checked_add(rq_offset) {
                        None => {
                            let key1 = key.sub(rq_offset);
                            CRUDOperation::Range(Interval::new(
                                key1,
                                key), Version::MAX - 1)
                        }
                        Some(key1) => CRUDOperation::Range(Interval::new(
                            key,
                            key1), Version::MAX - 1)
                    }
                } else {
                    CRUDOperation::Point(key, Version::MAX)
                }
            }
        })
        .chunks(operation_per_thread)
        .into_iter()
        .map(|chunk| Arc::new(chunk.collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    let (actual_reads_count, actual_rq_count, actual_updates_count) = operations
        .iter()
        .fold((0usize, 0usize, 0usize), |(p, r, u), inner| {
            let (n_p, n_r, n_u) = inner
                .iter()
                .fold((0usize, 0usize, 0usize), |(p, r, u), op|
                    match op {
                        CRUDOperation::Point(..) => (p + 1, r, u),
                        CRUDOperation::Range(..) => (p, r + 1, u),
                        _ => (p, r, u + 1)
                    });
            (n_p + p, n_r + r, n_u + u)
        });

    let worker = |which: usize| {
        let u_tree
            = tree.clone();

        let working_queue
            = operations.get(which).unwrap().clone();

        spawn(move || working_queue
            .iter()
            .map(|op| u_tree.dispatch(op.clone()).0)
            .fold(NodeVisits::MIN, |n, acc| acc + n))
    };

    for run in 1..=runs {
        let start = SystemTime::now();
        let node_visits = (0..threads)
            .map(|which| (worker)(which))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .fold(NodeVisits::MIN, |n, acc| acc + n);

        println!("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                 operations_count,
                 threads,
                 ls.clone(),
                 create_time,
                 create_node_visits,
                 dups,
                 lambda,
                 run,
                 SystemTime::now().duration_since(start).unwrap().as_millis(),
                 node_visits,
                 updates_thresh_hold,
                 actual_updates_count,
                 actual_reads_count,
                 actual_rq_count,
                 rq_offset,
                 rq_probability,
                 actual_reads_count + actual_rq_count + actual_updates_count,
                 NUM_RECORDS);
    }
}

pub fn gen_data_exp(limit: u64, lambda: f64, rnd: &mut StdRng) -> Vec<u64> {
    (1..=limit)
        .map(|i|
            gen_rand_key(i, 0, i, lambda, rnd))
        .collect()
}

pub fn gen_rand_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut StdRng) -> u64 {
    #[inline(always)]
    fn sample_next(lambda: f64, rnd: &mut StdRng) -> f64 {
        let num
            = rnd.gen_range(0_f64..1_f64);

        (1_f64 - num)
            .ln()
            .div(-lambda)
    }

    let range = range_end - range_start;

    (((loop {
        let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
        if key >= 0_f64 {
            break key;
        }
    }) / range as f64) * u64::MAX as f64) as _
}