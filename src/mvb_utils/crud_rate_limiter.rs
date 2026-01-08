use std::sync::Arc;
use std::{mem, thread};
use std::fmt::{Display, Formatter};
use std::thread::{JoinHandle, ThreadId};
use std::time::{Duration, Instant, SystemTime};
use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};
use crate::mvb_crud_model::crud_api::CRUDDispatcher;
use crate::mvb_crud_model::crud_operation::CRUDOperation;
use crate::n_test::{Key, Payload, INDEX};
use crate::mvb_utils::crud_rate_limiter::ThreadControl::{Crud, Fps};

pub enum ThreadControl {
    Crud(CRUDOperation<Key, Payload>),
    Fps(usize),
}

pub struct ThreadWorker {
    handle: JoinHandle<usize>, // handle of the worker
    controller: Sender<ThreadControl>, // used to instruct the thread for exe
    log: bool
}

pub struct ThreadWorkerInfo {
    pub thread_id: ThreadId,
    pub crud: CRUDOperation<Key, Payload>,
    pub fps: usize,
    pub load: f64,
    pub tick_ops: usize,
    pub total_ops: usize
}

impl Display for ThreadWorkerInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let tid = format!("{:?}", self.thread_id)
            .trim_start_matches("ThreadId(")
            .trim_end_matches(")")
            .parse::<u64>()
            .unwrap_or(0);

        write!(f, "{},{},{},{:.1},{},{}",
               tid,
               self.crud,
               self.fps,
               self.load,
               self.tick_ops,
               self.total_ops)
    }
}

impl ThreadWorker {
    pub fn new(
        index: Arc<INDEX>,
        p_fps: usize,
        p_crud: CRUDOperation<Key, Payload>,
        log: bool,
        info_pipe: Sender<ThreadWorkerInfo>
    )-> ThreadWorker
    {
        let (controller, thread_sink)
            = unbounded();

        let handle = thread::spawn(move || {
            let mut total_executed_ops = 0_usize;

            let mut fps = p_fps; // ops/sec
            let mut crud = p_crud;

            let mut start_time_frame = Instant::now();
            let mut ops_executed_in_frame = 0_usize;
            let mut cpu_load = 0_f64;
            let mut time_per_tick = 0_usize;

            loop {
                match thread_sink.try_recv() {
                    Ok(Crud(n_crud)) => crud = n_crud,
                    Ok(Fps(n_fps)) => fps = n_fps,
                    Err(TryRecvError::Disconnected) => break total_executed_ops,
                    Err(TryRecvError::Empty) => { }
                }

                let _ = index.dispatch(crud.clone());
                total_executed_ops += 1;
                ops_executed_in_frame += 1;

                if ops_executed_in_frame >= fps {
                    time_per_tick = start_time_frame.elapsed().as_millis() as usize;
                    while start_time_frame.elapsed().as_millis() < 999 {
                        thread::sleep(Duration::from_millis(1))
                    }

                    cpu_load = (time_per_tick as f64 / 1000_f64) * 100_f64;
                    if log {
                        let load_str = if time_per_tick > 900 {
                            "Heavy"
                        }
                        else {
                            "Normal"
                        };
                        println!("[Log] \t- {load_str} load ({:.1}%)\n\
                       \t- thread    = {:?}\n\
                       \t- crud      = {crud}\n\
                       \t- fps       = {p_fps}\n\
                       \t- ops/s     = {ops_executed_in_frame}\n\
                       \t- time      = {time_per_tick}\n\
                       \t- total_ops = {total_executed_ops}\n\
                       #############################################",
                                 cpu_load,
                                 thread::current().id()
                        );
                    }

                    info_pipe.send(ThreadWorkerInfo {
                        thread_id: thread::current().id(),
                        crud: crud.clone(),
                        fps,
                        load: cpu_load,
                        tick_ops: ops_executed_in_frame,
                        total_ops: total_executed_ops,
                    }).unwrap();

                    start_time_frame = Instant::now();
                    ops_executed_in_frame = 0;
                }
                else if ops_executed_in_frame < fps && start_time_frame.elapsed().as_millis() > 999 {
                    // overload
                    time_per_tick = start_time_frame.elapsed().as_millis() as usize;
                    cpu_load = (time_per_tick as f64 / 1000_f64) * 100_f64;
                    if log {
                        println!("[Log] \t- *Over* load ({:.1}%)\n\
                       \t- thread    = {:?}\n\
                       \t- crud      = {crud}\n\
                       \t- fps       = {p_fps}\n\
                       \t- ops/s     = {ops_executed_in_frame}, diff/s = {}\n\
                       \t- time      = {time_per_tick}\n\
                       \t- total_ops = {total_executed_ops}\n\
                       #############################################",
                                 cpu_load,
                                 thread::current().id(),
                                 fps - ops_executed_in_frame
                        );
                    }

                    info_pipe.send(ThreadWorkerInfo {
                        thread_id: thread::current().id(),
                        crud: crud.clone(),
                        fps,
                        load: cpu_load,
                        tick_ops: ops_executed_in_frame,
                        total_ops: total_executed_ops,
                    }).unwrap();

                    start_time_frame = Instant::now();
                    ops_executed_in_frame = 0;
                }
            }
        });

        ThreadWorker {
            handle,
            controller,
            log
        }
    }

    pub fn set_crud(&self, crud: CRUDOperation<Key, Payload>) {
        self.controller.send(Crud(crud)).unwrap();
    }

    pub fn set_fps(&self, fps: usize) {
        self.controller.send(Fps(fps)).unwrap();
    }

    pub fn stop(self) -> JoinHandle<usize> {
        mem::drop(self.controller);
        self.handle
    }
}