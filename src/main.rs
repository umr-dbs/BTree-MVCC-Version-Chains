use std::{env, fs, mem, thread};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread::spawn;
use std::time::{Instant, SystemTime};
use chrono::{DateTime, Local};
use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};
use itertools::{Either, Itertools};
use rand::prelude::SliceRandom;
use crate::block::block::Block;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy::*;
use crate::n_test::{format_insertions, hle, Key, Payload, Sampler, DEBUG, FAN_OUT, INDEX, NUM_RECORDS};
use crate::record_model::v_record_point::VersionIndexType;
use crate::tree::bplus_tree::{new_INDEX, BPlusTree};
use crate::utils::crud_rate_limiter::{ThreadWorker, ThreadWorkerInfo};

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
// mod test;
mod n_test;
mod version_index;

fn main() {
    // make_splash();
    let args = env::args();
    let parms = args.collect_vec();
    if parms.len() > 1  {
        match parms[1].as_str() {
            "insert_rate_limiter" => {
                let log                = parms[2].parse::<bool>().unwrap_or(false);
                let runtime_sec        = parms[3].parse::<u64>().unwrap_or(10);
                let num_workers        = parms[4].parse::<usize>().unwrap_or(10);
                let fps               = parms[5].parse::<usize>().unwrap_or(100);
                let crud                    = CRUDOperation::InsertRand;
                let olap_workers      = parms[6].parse::<usize>().unwrap_or(10);
                let olaps_per_worker  = parms[7].parse::<usize>().unwrap_or(10);
                let olap_skew_workers   = parms[8].parse::<f32>().unwrap_or(0f32);
                let olaps_key_range    = parms[9].parse::<Key>().unwrap_or(Key::MAX);
                let olaps_si_freshest  = parms[10].parse::<bool>().unwrap_or(false);

                let v_type = match parms[7].as_str() {
                    "l" | "ll" | "linkedlists" | "vanilla" => VersionIndexType::VANILLA,
                    "sk" | "skiplist" | "skiplists" => VersionIndexType::SkipList,
                    "fg" | "f" | "frugallists" => VersionIndexType::FrugalSkipList,
                    "weaver" | "vweaver" | "w" => VersionIndexType::VWEAVER,
                    "btree" | "index" | "dexa" | _ => VersionIndexType::BTree,
                };

                let index = Arc::new(new_INDEX(OLC, v_type));

                let (info_sender, info_receiver)
                    = unbounded();

                let file_name
                    = format!("btree_runtime_{runtime_sec}_workers_{num_workers}_fps_{fps}_crud_{crud}.csv");

                let _ = fs::remove_file(file_name.as_str());
                let mut log_file = BufWriter::new(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(file_name.as_str()).unwrap());

                log_file.write_all(b"tid,crud,fps,load,tick_ops,total_ops\n").unwrap();

                let start_time       = Instant::now();
                let workers = (0..num_workers)
                    .map(|_| ThreadWorker::new(
                        index.clone(),
                        fps,
                        crud.clone(),
                        log,
                        info_sender.clone()))
                    .collect_vec();

                let signal = info_receiver.clone();
                spawn(move || olap_tests(
                    index,
                    olap_workers,
                    olaps_per_worker,
                    olap_skew_workers,
                    Either::Left(olaps_key_range),
                    olaps_si_freshest,
                    Some(signal)));

                while start_time.elapsed().as_secs() < runtime_sec {
                    match info_receiver.try_recv() {
                        Ok(info) =>
                            log_file.write_all(format!("{}\n", info).as_bytes()).unwrap(),
                        _ => thread::yield_now()
                    }
                }

                println!("Total Ops = {}", workers
                    .into_iter()
                    .map(|t| t.stop())
                    .collect_vec()
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .sum::<usize>());

                mem::drop(info_receiver);
            }
            "test" => {
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
                let tree = Arc::new(new_INDEX(OLC, v_type));
                let mut check = HashMap::new();
                let mut errors = 0;

                while check.len() < n {
                    let key
                        = rand::random_range(0..Key::MAX);

                    if !check.contains_key(&key) {
                        match tree.dispatch(CRUDOperation::Insert(key, Payload::default())).1 {
                            CRUDOperationResult::Inserted(_, v) => {
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
                //     match tree.dispatch(CRUDOperation::Point(*k, *v)).1 {
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
                           None)
            }
            "generate" => {
                let query_file_name= parms[2].as_str();
                let init_population: usize = parms[3].parse().unwrap();
                let total_blocks: usize = parms[4].parse().unwrap();
                let block_inserts: usize = parms[5].parse().unwrap();
                let block_updates: usize = parms[6].parse().unwrap();
                let block_deletes: usize = parms[7].parse().unwrap();

                println!("Generate only supported in MVTree!")
            }
            "load_cc" => {
                let query_file_name= parms[2].to_string();
                let v_index = parms[3].as_str().to_lowercase();
                let num_olaps = parms[4].parse().unwrap();
                let workers_per_thread = parms[5].parse().unwrap();
                let skew = parms[6].parse().unwrap();
                let range = parms[7].parse().unwrap_or(Key::MAX);

                let v_type = match v_index.as_str() {
                    "l" | "ll" | "linkedlists" | "vanilla" => VersionIndexType::VANILLA,
                    "sk" | "skiplist" | "skiplists" => VersionIndexType::SkipList,
                    "fg" | "f" | "frugallists" => VersionIndexType::FrugalSkipList,
                    "weaver" | "vweaver" | "w" => VersionIndexType::VWEAVER,
                    "btree" | "index" | "dexa" | _ => VersionIndexType::BTree,
                };
                let index
                    = Arc::new(new_INDEX(OLC, v_type));

                println!("Created BTree with version index = '{v_type}..");

                let index_c = index.clone();
                let (olap_signal, olap_sink)
                    = unbounded();

                let query_file_name_clone = query_file_name.clone();
                let num = spawn(move ||
                    load_query(query_file_name_clone.as_str(), index_c, None));

                let olaps = spawn(move || olap_tests(
                    index, num_olaps, workers_per_thread, skew, Either::Left(range), false, Some(olap_sink)));

                let num = num.join().unwrap();
                mem::drop(olap_signal);

                olaps.join().unwrap();

                println!("Finished executing {} CRUD operations from {query_file_name}", format_insertions(num));
            }
            "load_cc_new" => {
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
                    = Arc::new(new_INDEX(OLC, v_type));

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
            "load" => {
                let query_file_name= parms[2].as_str();

                let num_olaps = parms[3].parse().unwrap();
                let workers_per_thread = parms[4].parse().unwrap();
                let skew = parms[5].parse().unwrap();
                let range = parms[6].parse().unwrap_or(Key::MAX);

                let v_index = parms[7].as_str().to_lowercase();
                let v_type = match v_index.as_str() {
                    "l" | "ll" | "linkedlists" | "vanilla" => VersionIndexType::VANILLA,
                    "sk" | "skiplist" | "skiplists" => VersionIndexType::SkipList,
                    "fg" | "f" | "frugallists" => VersionIndexType::FrugalSkipList,
                    "weaver" | "vweaver" | "w" => VersionIndexType::VWEAVER,
                    "btree" | "index" | "dexa" | _ => VersionIndexType::BTree,
                };

                let index
                    = Arc::new(new_INDEX(OLC, v_type));

                println!("Created BTree with version index = '{v_type}..");

                let num = load_query(query_file_name, index.clone(), None);

                println!("Finished executing {} CRUD operations from {query_file_name},\
                 starting OLAP testings...", format_insertions(num));
                olap_tests(index,
                           num_olaps,
                           workers_per_thread,
                           skew,
                           Either::Left(range),
                           false,
                           None);
            }
            // s => println!("unknown command '{s}'-")
            "help" | _ => {
                println!("\t Command: generate \
                <query_file_name> \
                <init_population> \
                <total_blocks>\
                <block_inserts>\
                <block_updates>\
                <block_deletes>");
            }
        }
    }
    else {
        if DEBUG {
            println!(">>HLE: \t\t\t{}", hle());
            // println!(">>size_of::<Block<127, 127, u64, u64>>()) = {}",
            //          size_of::<Block<127, 127, u64, u64>>());
            // println!();
            let block_size = size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>();
            let kb = block_size as f32 / 1024f32;
            println!("\
        >>FAN_OUT: \t\t{FAN_OUT}\n\
        >>NUM_RECORDS: \t\t{NUM_RECORDS}\n\
        >>size_of(BLOCK): \t{} bytes; {kb} kb",
                     size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>());
            println!();
        }
        else {
            make_splash()
        }
    }


    // let skip_map = SkipMap::new();
    // println!("{}", serde_json::to_string_pretty(&GroupConfig::default()).unwrap());


    // seq_create();
    // seq_run();
    // bernhard_tests()
}

fn olap_tests(index: Arc<BTree>,
              num_olaps: usize,
              workers_per_thread: usize,
              skew: f32,
              range: Either<Key, Arc<AtomicU64>>,
              fixed_si: bool,
              control_signal: Option<Receiver<ThreadWorkerInfo>>)
{
    println!("Starting OLAPs...");
    let v_index = index.v_index_type;
    println!(".... BTree via {v_index}");

    let mut olaps = vec![];

    let _nc = fs::remove_file(format!("btree_{v_index}_olap_skew_{skew}.csv"));
    let mut olap_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(format!("btree_{v_index}_olap_skew_{skew}.csv"))
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

    for _ in 0..num_olaps {
        let index
            = index.clone();

        let signal
            = control_signal.clone();

        let range
            = range.clone();

        olaps.push(spawn(move || {
            let mut results = vec![];
            for _ in 1..workers_per_thread {
                let mut key_max = 1000;
                let mut key_min= Key::MIN;
                if let Either::Left(range) = range {
                    key_min = rand::random_range(0..range);
                    key_max = key_min.checked_add(1000).unwrap_or(Key::MAX);

                    if range == Key::MAX {
                        key_min = 0;
                        key_max = Key::MAX;
                    }
                    else if key_max >= Key::MAX {
                        key_max = key_min;
                        key_min -= range;
                    }
                }
                else if let Either::Right(ref range) = range {
                    key_max = range.load(Acquire);
                    key_min = key_max.checked_sub(1000).unwrap_or(0);
                }

                let current_si
                    = index.committed_version();

                let si = if fixed_si  {
                    current_si
                }
                else {
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
                results.push(
                    (si, current_si, 0u128, key_min, key_max, count_results, time_spent));

                if let Some(signal) = signal.as_ref() {
                    match signal.try_recv() {
                        Err(TryRecvError::Disconnected) => break,
                        _ => { }
                    }
                }
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

const INSERT: u8 = 0;
const UPDATE: u8 = 1;
const DELETE: u8 = 2;

fn load_query(query_file: &str,
              index: Arc<BTree>,
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

type BTree = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

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

/// Essential function.
fn make_splash() {
    let datetime: DateTime<Local> = fs::metadata(std::env::current_exe().unwrap())
        .unwrap().modified().unwrap().into();

    println!("                         _________________________");
    println!("                 _______/                         \\_______");
    println!("                /                                         \\");
    println!(" +-------------+                                           +-------------+");
    println!(" |                                                                       |");
    println!(" |               ------------------------------                          |");
    println!(" |               # Build:   {}                          |", datetime.format("%d-%m-%Y %T"));
    println!(" |               # Current version: {}                               |", env!("CARGO_PKG_VERSION"));
    println!(" |               -------------------------                               |");
    println!(" |               # OLC-HLE:   {}                                     |", hle());
    // println!(" |               # RW-HLE:    AUTO                                       |");
    // println!(" |               # SYS-YIELD: {}                                       |",
    //          if ENABLE_YIELD { "ON  " } else { "OFF " });
    println!(" |               -----------------                                       |");
    println!(" |                                                                       |");
    println!(" |               --------------------------------------------            |");
    println!(" |               # E-Mail: elshaikh@mathematik.uni-marburg.de            |");
    println!(" |               # Written by: Amir El-Shaikh                            |");
    println!(" |               # First released: 09-09-2024                            |");
    println!(" |               # Repository:                                           |");
    println!(" |               https://github.com/umr-dbs/DEXA-VersionLists-BPlusTree  |");
    println!(" |               ----------------------------                            |");
    println!(" |                                                                       |");
    println!(" |               ...MVCC-B+Tree Application Launching...                 |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

