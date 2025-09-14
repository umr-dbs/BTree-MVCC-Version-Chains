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
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::ops::Div;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::{fs, thread};
use std::io::Write;
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
use crate::record_model::v_record_point::VersionIndexType;
use crate::record_model::Version;
use crate::tree::bplus_tree::{new_INDEX, BPlusTree};
// use crate::utils::smart_cell::{force_read_success, unforce_read_success};

const SYSTEM_STR: &str = "B+Tree";

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
    version_index,\
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
                VersionIndexType::VANILLA => "ll",
                VersionIndexType::SkipList => "sk",
                // VersionIndexType::SkipListSynced => "sk_synced",
                VersionIndexType::BTree => "btree",
                VersionIndexType::VWEAVER => "vweaver"
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

pub fn format_insertions(mut i: usize) -> String {
    let mut parts = Vec::new();

    let units = [
        (1_000_000_000, "B"),
        (1_000_000, "Mio"),
        (1_000, "K"),
    ];

    for &(value, suffix) in &units {
        if i >= value {
            let count = i / value;
            parts.push(format!("{} {}", count, suffix));
            i %= value;
        }
    }

    if i > 0 {
        parts.push(i.to_string());
    }

    if parts.is_empty() {
        "0".to_string()
    } else {
        parts.join(" + ")
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
