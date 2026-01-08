use std::{env, fs};
use chrono::{DateTime, Local};
use itertools::Itertools;
use crate::mvb_block::block::Block;
use crate::n_test::{ main_load, Key, Payload, FAN_OUT, NUM_RECORDS};

mod mvb_block;
mod mvb_crud_model;
mod mvb_locking;
mod mvb_page_model;
mod mvb_record_model;
mod mvb_tree;
mod mvb_utils;
// mod test;
mod n_test;
mod mvb_version_index;

fn main() { 
    startup();
    
    let args = env::args();
    let parms = args.collect_vec();
    if parms.len() > 1  {
        match parms[1].as_str() {
            // "insert_rate_limiter" => main_insert_rate_limiter(parms),
            // "load_cc_new" => main_load_cc_new(parms),
            // "test" => main_test(parms),
            "load" => main_load(parms),
            // s => println!("unknown command '{s}'-")
            s => println!("Unknown Command '{s}'")
        }
    }
    else {
        println!("*********** Use a Command ***********")
    }
}

fn startup() {
    make_splash();
    println!(">>HLE: \t\t\t{}", hle());
    let block_size = size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>();
    let kb = block_size as f32 / 1024f32;
    println!("\
        >>FAN_OUT: \t\t{FAN_OUT}\n\
        >>NUM_RECORDS: \t\t{NUM_RECORDS}\n\
        >>size_of(BLOCK): \t{} bytes; {kb} kb",
             size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>());
    println!();
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
    println!(" |               -----------------------------------------------         |");
    println!(" |               # E-Mail: amir.tonta@mathematik.uni-marburg.de          |");
    println!(" |               # Written by: Amir Tonta                                |");
    println!(" |               # First released: 09-09-2024                            |");
    println!(" |               # Repository: https://github.com/umr-dbs/MVBTree        |");
    println!(" |               ----------------------------                            |");
    println!(" |                                                                       |");
    println!(" |               ...MVCC-B+Tree Application Launching...                 |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

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