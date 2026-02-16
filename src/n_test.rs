use std::collections::HashMap;
use std::{fs, mem, thread};
use std::fmt::format;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::thread::{spawn, yield_now};
use std::time::{Instant, SystemTime};
use crossbeam_channel::{unbounded, Receiver, TryRecvError};
use itertools::{Either, Itertools};

use crate::mvb_crud_model::crud_api::CRUDDispatcher;
use crate::mvb_crud_model::crud_operation::CRUDOperation;
use crate::mvb_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mvb_crud_model::dispatch::LAZY_RANGE_SCAN_ENABLED;
use crate::mvb_locking::locking_strategy::LockingStrategy::OLC;
use crate::mvb_record_model::v_record_point::VersionIndexType;
use crate::mvb_record_model::Version;
use crate::mvb_tree::bplus_tree::{new_INDEX, MVBPlusTree};
use crate::mvb_utils::crud_rate_limiter::{ThreadWorker, ThreadWorkerInfo};

pub type MVBTree = MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;
pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

pub(crate) fn main_load(parms: Vec<String>) {
    {
        println!("###### Command: {} ######", parms.iter().skip(1).join(" "));

        let query_file_name = parms[2].to_string();
        let concurrent = parms[3].parse::<bool>().unwrap();
        let num_olaps = parms[4].parse().unwrap();

        let scans_per_thread = parms[5].parse().unwrap();

        let skew = parms[6].parse().unwrap();
        let range = parms[7].parse().unwrap_or(Key::MAX);
        let v_index = match parms[8].as_str() {
            "sk" => VersionIndexType::SkipList,
            "ll" => VersionIndexType::VANILLA,
            "fg" => VersionIndexType::FrugalSkipList,
            "bt" => VersionIndexType::BTree,
            "w"  => VersionIndexType::VWEAVER,
            _ => VersionIndexType::default()
        };

        let gc =
            parms[9].parse::<bool>().unwrap_or(false);

        let update_in_place
            = parms[10].parse::<bool>().unwrap_or(false);

        let index
            = Arc::new(new_INDEX(OLC, v_index, gc));

        println!("- QueryFile = {query_file_name}\n\
                - Concurrent = {concurrent}\n\
                - OLAP Threads = {num_olaps} (Cores = {}, Threads = {})\n\
                - Scans/Thread = {}\n\
                - Skew = {skew}\n\
                - Range = {range}\n\
                - Version Index = {v_index}\n\
                - GC = {gc}",
                 num_cpus::get_physical(),
                 num_cpus::get(),
                 if concurrent { format!("Continuous\n- OLTP Threads = {scans_per_thread}") } else { format!("{scans_per_thread}") });

        let oltp_there = fs::exists("oltp.csv").unwrap();
        let mut oltp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("oltp.csv")
            .unwrap();

        if !oltp_there {
            oltp_file.write_all(b"\
            is_concurrent,\
            oltp_threads,\
            olap_threads,\
            v_index,\
            skew,\
            gc,\
            update_in_place,\
            slice_per_thread,\
            rest_slice,\
            blocks_allocated,\
            blocks_reused,\
            total_num_scan_tx,\
            total_num_oltp_tx,\
            total_oltp_time,\
            total_olap_time\n"
            ).unwrap();
        }

        if concurrent {
            let query_file_name_clone = query_file_name.clone();
            let mut oltp = load_query_into_memory(
                query_file_name_clone.as_str());

            let oltp_threads = scans_per_thread;
            let slice = oltp.len() / oltp_threads;

            let mut work_oltp = (0..oltp_threads)
                .map(|_| oltp.drain(..slice).collect_vec())
                .collect_vec();

            let rest_slice = oltp.len();
            work_oltp.first_mut().unwrap().extend(oltp);
            oltp_file.write_all(format!("\
            true,\
            {oltp_threads},\
            {num_olaps},\
            {v_index},\
            {skew},\
            {gc},\
            {update_in_place},\
            {slice},\
            {rest_slice}").as_bytes()).unwrap();

            let start_time_oltp = Instant::now();
            let oltp_joins = work_oltp
                .into_iter()
                .map(|work| {
                    let index = index.clone();
                    spawn(move || {
                        let mut count_crud = 0;
                        work.into_iter().for_each(|crud| {
                            let _ = index.dispatch(crud);
                            count_crud += 1;
                        });
                        count_crud
                    })
                }).collect_vec();

            let (olap_signal, olap_sink)
                = unbounded();

            let index_olaps
                = index.clone();

            let olaps = spawn(move || olap_tests(
                index_olaps,
                num_olaps,
                1,
                skew,
                Either::Left(range),
                false,
                Some(olap_sink)));

            let oltp_executed = oltp_joins
                .into_iter()
                .map(|j| j.join().unwrap())
                .sum::<usize>();

            let oltp_total_time = start_time_oltp.elapsed().as_nanos();
            drop(olap_signal);
            let (num_scans_executed, olap_total_time) = olaps.join().unwrap();

            let alloc_blocks
                = index.block_manager.alloc_count.load(SeqCst);

            let reuse_blocks = 0;

            oltp_file.write_all(format!(",\
            {alloc_blocks},\
            {reuse_blocks},\
            {num_scans_executed},\
            {oltp_executed},\
            {oltp_total_time},\
            {olap_total_time}\n").as_bytes()).unwrap();

            println!("- Executed {} OLTPs from {query_file_name}\n\
        - Executed = {} OLAPs", format_insertions(oltp_executed),
                     format_insertions(num_scans_executed));

            println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
        } else {
            let oltp_tx_buff = load_query_into_memory(
                query_file_name.as_str());

            let num = oltp_tx_buff.len();
            let start_oltp_time = Instant::now();

            oltp_tx_buff.into_iter().for_each(|crud| {
                let _ = index.dispatch(crud);
            });

            let oltp_total_time = start_oltp_time.elapsed().as_nanos();

            println!("- Executed {} CRUD operations from {query_file_name}, \
                 starting OLAPs...", format_insertions(num));

            let (num_scans_executed, olap_total_time) = olap_tests(
                index.clone(),
                num_olaps,
                scans_per_thread,
                skew,
                Either::Left(range),
                false,
                None);

            let alloc_blocks
                = index.block_manager.alloc_count.load(SeqCst);

            let reuse_blocks = 0;

            oltp_file.write_all(format!("\
            false,\
            1,\
            {num_olaps},\
            {v_index},\
            {skew},\
            {gc},\
            {update_in_place},\
            {num},\
            0,\
            {alloc_blocks},\
            {reuse_blocks},\
            {num_scans_executed},\
            {num},\
            {oltp_total_time},\
            {olap_total_time}\n").as_bytes()).unwrap();

            println!("- Executed = {} OLAPs", format_insertions(num_scans_executed));
            println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
        }

        oltp_file.flush().unwrap();
    }
}

fn olap_tests(index: Arc<MVBTree>,
              num_olaps: usize,
              tx_per_thread: usize,
              skew: f32,
              range: Either<Key, Arc<AtomicU64>>,
              fixed_si: bool,
              control_signal: Option<Receiver<ThreadWorkerInfo>>) -> (usize, u128)
{
    if control_signal.is_none() {
        println!("> Starting OLAPs...{num_olaps} threads, \
        {tx_per_thread} scans per thread.");
    } else {
        println!("> Starting OLAPs...{num_olaps} threads, \
         with control signal for continuous scans per thread");
    }

    if range.is_left() {
        println!("> Scan key-range is fixed to 0..={}", range.as_ref().left().unwrap())
    } else {
        println!("> Scan key-range is dynamic to 0..=LastKey")
    }

    let v_index = index.v_index_type;

    let lazy = if LAZY_RANGE_SCAN_ENABLED {
        "_lazy"
    } else { "" };

    let mut olaps = vec![];

    let file_log = format!("btree_{v_index}{lazy}_olap_skew_{skew}.csv");
    let _nc = fs::remove_file(file_log.as_str());
    let mut olap_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(file_log.as_str())
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

    let g_counter = Arc::new(AtomicUsize::new(0));

    let start_olap_time = Instant::now();
    for _ in 0..num_olaps {
        let index
            = index.clone();

        let signal
            = control_signal.clone();

        let range
            = range.clone();

        let count_olaps
            = g_counter.clone();

        olaps.push(spawn(move || {
            let mut results = vec![];
            let mut tx_c = 0;
            while tx_c < tx_per_thread {
                let mut key_max = 1000;
                let mut key_min= Key::MIN;
                if let Either::Left(range) = range {
                    key_min = 0;
                    key_max = range;
                }
                else if let Either::Right(ref range) = range {
                    key_max = range.load(Acquire);
                    key_min = key_max.checked_sub(1000).unwrap_or(0);
                }

                let mut current_si
                    = index.current_version_for_reader();

                while current_si == 1 {
                    yield_now();
                    current_si = index.current_version_for_reader(); // todo: check reader clock
                }

                let si = if fixed_si {
                    current_si
                } else {
                    rand::random_range(1..=current_si)
                };

                // println!("{key_min} - {key_max}: SI = {si}");
                let time_start
                    = SystemTime::now();

                let (_nv, crud) =
                    index.dispatch(CRUDOperation::Range((key_min, key_max).into(), si));

                let time_spent
                    = SystemTime::now().duration_since(time_start).unwrap().as_nanos();

                let count_results = match crud {
                    CRUDOperationResult::MatchedRecords(data) =>  data.len(),
                    _ => panic!()
                };

                let _ = count_olaps.fetch_add(1, Relaxed);

                results.push(
                    (si, current_si, 0u128, key_min, key_max, count_results, time_spent));

                if let Some(signal) = signal.as_ref() {
                    match signal.try_recv() {
                        Err(TryRecvError::Disconnected) => break,
                        _ => continue
                    }
                }

                tx_c += 1;
            }
            results
        }))
    }

    let olaps = olaps.into_iter().map(|j| j.join().unwrap())
        .flatten()
        .collect::<Vec<_>>();

    let time_olap = start_olap_time.elapsed().as_nanos();
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
            });

    (g_counter.load(SeqCst), time_olap)
}

fn load_query(query_file: &str,
              index: Arc<MVBTree>,
              report_signal: Option<Arc<AtomicU64>>) -> usize
{
    let mut query_file = BufReader::new(OpenOptions::new()
        .read(true)
        .open(format!("{query_file}"))
        .unwrap());

    let mut query_count = 0;
    let payload = Payload::default();
    let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
    loop {
        match query_file.read_exact(buff.as_mut_slice()) {
            Ok(..) => match buff[0] {
                INSERT => {
                    let crud = CRUDOperation::Insert(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()),
                        payload
                    );

                    let (_nv, exe) = index.dispatch(crud);
                    if let CRUDOperationResult::Inserted(key, ..) = exe {
                        if let Some(ref sender) = report_signal {
                            sender.store(key, Release);
                        }
                    }
                    else {
                        panic!("Insert failed");
                    }
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()),
                        payload
                    );

                    let (_nv, exe) = index.dispatch(crud);
                    if let CRUDOperationResult::Updated(key, ..) = exe {
                        if let Some(ref sender) = report_signal {
                            sender.store(key, Release);
                        }
                    }
                    else {
                        panic!("Update failed");
                    }
                }
                DELETE => {
                    let crud = CRUDOperation::Delete(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    let (_nv, exe) = index.dispatch(crud);
                    if let CRUDOperationResult::Deleted(key, ..) = exe {
                        if let Some(ref sender) = report_signal {
                            sender.store(key, Release);
                        }
                    }
                    else {
                        panic!("Delete failed");
                    }
                }
                _ => panic!("Unknown CRUD Operation for blocks in load query!"),
            }
            Err(..) => break
        }

        query_count += 1
    }

    assert!(query_file.read_exact([0].as_mut_slice()).is_err());
    query_count
}


pub(crate) fn main_test(parms: Vec<String>) {
    {
        let n = parms[2].parse().unwrap();
        let num_olaps = parms[3].parse().unwrap();
        let olaps_per_worker = parms[4].parse().unwrap();
        let skew = parms[5].parse().unwrap();
        let key_range = parms[6].parse().unwrap_or(Key::MAX);
        let v_type = match parms[7].as_str() {
            "l" | "ll" | "linkedlists" | "vanilla" => VersionIndexType::VANILLA,
            "sk" | "skiplist" | "skiplists" => VersionIndexType::SkipList,
            "fg" | "f" | "frugallists" => VersionIndexType::FrugalSkipList,
            "weaver" | "vweaver" | "w" => VersionIndexType::VWEAVER,
            "btree" | "index" | "dexa" | _ => VersionIndexType::BTree,
        };

        println!("v_index = {v_type}");
        let tree = Arc::new(new_INDEX(OLC, v_type, false));
        let mut check = HashMap::new();
        let mut errors = 0;

        while check.len() < n {
            let key
                = rand::random_range(0..Key::MAX);

            if !check.contains_key(&key) {
                match tree.dispatch(CRUDOperation::Insert(key, Payload::default())).1 {
                    CRUDOperationResult::Inserted(v) => {
                        check.insert(key, v);
                    }
                    _ => {
                        println!("Error insert key={key}");
                        errors += 1
                    }
                };

            }
        }

        // for (k, v) in check.iter() {
        //     match mvb_tree.dispatch(CRUDOperation::Point(*k, *v)).1 {
        //         CRUDOperationResult::MatchedRecord(Some(..)) => {}
        //         CRUDOperationResult::MatchedRecord(None) => {
        //             println!("Empty result of point: key={k}, version={v}");
        //         }
        //         _ => {
        //             println!("Error crud point: key={k}, version={v}");
        //             errors += 1
        //         }
        //     }
        // }

        mem::drop(check);

        olap_tests(tree, num_olaps,
                   olaps_per_worker,
                   skew,
                   Either::Left(key_range),
                   false,
                   None);
    }
}
pub(crate) fn main_load_cc_new(parms: Vec<String>) {
    {
        let query_file_name= parms[2].to_string();
        let v_index = parms[3].as_str().to_lowercase();
        let num_olaps = parms[4].parse().unwrap();
        let workers_per_thread = parms[5].parse().unwrap();
        let skew = parms[6].parse().unwrap();

        let v_type = match v_index.as_str() {
            "l" | "ll" | "linkedlists" | "vanilla" => VersionIndexType::VANILLA,
            "sk" | "skiplist" | "skiplists" => VersionIndexType::SkipList,
            "fg" | "f" | "frugallists" => VersionIndexType::FrugalSkipList,
            "weaver" | "vweaver" | "w" => VersionIndexType::VWEAVER,
            "btree" | "index" | "dexa" | _ => VersionIndexType::BTree,
        };
        let index
            = Arc::new(new_INDEX(OLC, v_type, false));

        println!("Created BTree with version index = '{v_type}..");

        let atomic_key = Arc::new(AtomicU64::new(0));

        let index_c = index.clone();
        let (olap_signal, olap_sink)
            = unbounded();

        let query_file_name_clone = query_file_name.clone();
        let atomic_key_clone = atomic_key.clone();
        let num = spawn(move ||
            load_query(query_file_name_clone.as_str(), index_c, Some(atomic_key_clone)));

        let olaps = spawn(move || olap_tests(
            index,
            num_olaps,
            workers_per_thread,
            skew,
            Either::Right(atomic_key),
            true,
            Some(olap_sink)));

        let num = num.join().unwrap();
        mem::drop(olap_signal);

        olaps.join().unwrap();

        println!("Finished executing {} CRUD operations from {query_file_name}", format_insertions(num));
    }
}
const INSERT: u8 = 0;
const UPDATE: u8 = 1;
const DELETE: u8 = 2;
fn load_query_into_memory(query_file: &str) -> Vec<CRUDOperation<Key, Payload>> {
    let mut query_file = BufReader::new(OpenOptions::new()
        .read(true)
        .open(format!("{query_file}"))
        .unwrap());

    let payload = Payload::default();
    let mut loaded = vec![];

    loop {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match query_file.read_exact(buff.as_mut_slice()) {
            Ok(..) => match buff[0] {
                INSERT => {
                    let key = Key::from_le_bytes((&buff[1..]).try_into().unwrap());
                    let crud = CRUDOperation::Insert(key, payload);
                    loaded.push(crud);
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()), payload);

                    loaded.push(crud);
                }
                DELETE => {
                    let crud = CRUDOperation::Delete(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    loaded.push(crud);
                }
                _ => panic!("Unknown CRUD Operation for blocks in load query into memory!"),
            }
            Err(..) => break
        }
    }

    assert!(query_file.read_exact([0].as_mut_slice()).is_err());

    loaded
}

pub const BSZ: usize = 4096;

pub type Payload = u64;
pub type Key = u64;

pub type SnapShot = Version;
pub const FAN_OUT: usize = 255; // was earlier 256
pub const NUM_RECORDS: usize = 170;

pub type INDEX = MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

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