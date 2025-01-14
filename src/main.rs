use std::{env, fs, mem};
use chrono::{DateTime, Local};
use rand::SeedableRng;
use crate::block::block::Block;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::locking::locking_strategy::LockingStrategy;
use crate::locking::locking_strategy::LockingStrategy::*;
use crate::n_test::{execute_experiments, hle};
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
    make_splash();

    // println!("Size of Node = {}", mem::size_of::<Block<250, 102, u64, u64>>());
    execute_experiments()
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

