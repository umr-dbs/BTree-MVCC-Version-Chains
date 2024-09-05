use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::{fs, thread};
use std::hash::{Hash, Hasher};
use std::ops::{Add, Deref, DerefMut, Div, RangeInclusive, Sub};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::thread::spawn;
use std::time::{Duration, SystemTime};
use crossbeam::channel::TryRecvError;
use hashbrown::HashMap;
use itertools::Itertools;
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

use crate::tree::bplus_tree::BPlusTree;

use crate::utils::interval::Interval;
use crate::utils::smart_cell::COUNTERS;

pub const VALIDATE_OPERATION_RESULT: bool = false;
pub const EXE_LOOK_UPS: bool = false;
pub const EXE_RANGE_LOOK_UPS: bool = false;

pub const BSZ_BASE: usize = _4KB;
pub const BSZ: usize = BSZ_BASE - bsz_alignment::<Key, Payload>();
pub const FAN_OUT: usize = BSZ / 8 / 2;
pub const NUM_RECORDS: usize = (BSZ - 2) / (8 + 8);

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

pub type INDEX = BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const TREE: fn(CRUDProtocol) -> Tree = |crud| {
    Arc::new(if let MonoWriter = crud {
        TreeDispatcher::Wrapper(RwLock::new(MAKE_INDEX(crud)))
    }
    else {
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
                .for_each(|next_query| match index.dispatch(next_query) { // tree.execute(operation),
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
    = false;

    const N: u64
    = 10_000_000;

    const KEY_RANGE: RangeInclusive<Key>
    = 1..=N;

    const REPEATS: usize
    = 3;

    const UPDATES_THRESHOLD: [f64; 3] = [
        // 0.0_f64,
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

    const RQ_OFFSET: [u64; 1] = [
        // 4 * (NUM_RECORDS as u64 + 1_u64),
        64 * (NUM_RECORDS as u64 + 1_u64),
    ];

    let data_lambdas = LAMBDAS
        .iter()
        .map(|lambda| {
            let mut rnd = StdRng::seed_from_u64(90501960);
            gen_data_exp(N, *lambda, &mut rnd)
                .into_iter()
                .map(|key| CRUDOperation::Insert(key, Payload::default()))
                .collect::<Vec<_>>()
        }).collect::<Vec<_>>();

    if MAKE_HIST {
        for lambda in 0..LAMBDAS.len() {
            println!("[Lambda={}] -\tStep 1/3: Creating tree with '{}' keys via '{}' threads ..",
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
        // OLC(),
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
        for lambda in 0..LAMBDAS.len() {
            for thread in THREADS {
                let tree
                    = TREE(protocol.clone());

                // unsafe {
                //     COUNTERS.0.store(0, SeqCst);
                //     COUNTERS.1.store(0, SeqCst);
                // }

                // thread::sleep(Duration::from_millis(10));

                // for _ in 0..5 {
                let (create_time, errs, create_node_visits)
                    = bulk_crud(thread,
                                tree.clone(),
                                data_lambdas[lambda].as_slice());
                // }

                // let (create_time, errs, create_node_visits)
                //     = (0, 0, 0);

                // thread::sleep(Duration::from_secs(1));

                // unsafe {
                //     let (pause, yields)
                //         = (COUNTERS.0.load(SeqCst), COUNTERS.1.load(SeqCst));
                // 
                //     println!("{},{},{},{},{}", protocol, pause, yields, LAMBDAS[lambda], thread);
                // }

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
                                key))
                        }
                        Some(key1) => CRUDOperation::Range(Interval::new(
                            key,
                            key1))
                    }
                } else {
                    CRUDOperation::Point(key)
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