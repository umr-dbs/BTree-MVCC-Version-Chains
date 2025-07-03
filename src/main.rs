use std::{env, fs, mem};
use std::io::Write;
use std::sync::Arc;
use std::thread::spawn;
use std::time::SystemTime;
use chrono::{DateTime, Local};
use crossbeam_skiplist::SkipMap;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use crate::block::block::Block;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::locking::locking_strategy::LockingStrategy;
use crate::locking::locking_strategy::LockingStrategy::*;
use crate::n_test::{execute_experiments, format_insertions, hle, GroupConfig, Key, Payload, Sampler, DEBUG, FAN_OUT, NUM_RECORDS};
use crate::record_model::v_record_point::VersionIndexType;
use crate::record_model::v_record_point::VersionIndexType::SkipListSynced;
use crate::tree::bplus_tree::{new_INDEX, BPlusTree};
use crate::utils::smart_cell::ENABLE_YIELD;

mod block;
mod crud_model;
mod locking;
mod page_model;
mod record_model;
mod tree;
mod utils;
// mod test;

mod n_test;

fn main() {
    // let skip_map = SkipMap::new();
    // println!("{}", serde_json::to_string_pretty(&GroupConfig::default()).unwrap());
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

    // println!("{}", serde_json::to_string(&ORWC {
    //     write_level: 1f32,
    //     write_attempt: 4
    // }).unwrap());
    // println!("Size of Node = {}", mem::size_of::<Block<FAN_OUT, NUM_RECORDS, u64, u64>>());
    // execute_experiments()
    bernhard_tests()
}

type BTree = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

fn bernhard_tests() {
    const INSERTIONS: Key = 10_000_000;
    const UPDATES: Key = INSERTIONS as Key;
    const DELETIONS: f64 = 0.9_f64;
    const NUMBER_OLAPS: usize = 12;
    // const NUMBER_UPDATERS: usize = 6;
    const OLAP_TX_PER_WORKER: usize = 2000;
    const RANGE_SIZE: Key = 1_000;
    const SKEWs: [f64; 3] = [0f64, 0.4, 1.4];

    const V_INDEX: VersionIndexType = SkipListSynced;

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
            "\t- MVTree Init. \n\t- \
    [{NUMBER_OLAPS}] OLAPs starting with [{OLAP_TX_PER_WORKER}] transactions per worker."
        );

        // Start OLAPs here
        let index = Arc::new(btree);
        let mut olaps = vec![];

        let _nc = fs::remove_file(format!("{V_INDEX}_olap_skew_{skew}.csv"));
        let mut olap_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(format!("{V_INDEX}_olap_skew_{skew}.csv"))
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

        // let mut updaters = vec![];
        // for _ in 0..NUMBER_UPDATERS {
        //     let index = index.clone();
        //
        //     let (sender, receiver)
        //         = std::sync::mpsc::channel::<()>();
        //
        //     updaters.push((sender, spawn(move || {
        //         let mut sampler
        //             = Sampler::new(skew, INSERTIONS - 1);
        //
        //         loop {
        //             match receiver.try_recv() {
        //                 Err(..) => break,
        //                 _ => {
        //                     index.dispatch(CRUDOperation::Update(sampler.sample(), Payload::default()));
        //                 }
        //             }
        //         }
        //     })))
        // }
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
    println!(" |               # Current version: {}                                |", env!("CARGO_PKG_VERSION"));
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

