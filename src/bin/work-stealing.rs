use std::{
    cell::Cell,
    collections::VecDeque,
    mem,
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread::{self, Thread},
    time::Duration,
};

enum TimerFutureState {
    Unresumed,
    Started,
    Done,
}

struct TimerFuture {
    duration: Duration,
    state: Arc<Mutex<TimerFutureState>>,
}

impl TimerFuture {
    fn new(duration: Duration) -> Self {
        TimerFuture {
            duration,
            state: Arc::new(Mutex::new(TimerFutureState::Unresumed)),
        }
    }
}

impl Future for TimerFuture {
    type Output = String;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();

        match *state {
            TimerFutureState::Unresumed => {
                let duration_clone = self.duration;
                let state_clone = Arc::clone(&self.state);
                let waker = cx.waker().clone();

                thread::spawn(move || {
                    thread::sleep(duration_clone);

                    *state_clone.lock().unwrap() = TimerFutureState::Done;

                    waker.wake();
                });

                *state = TimerFutureState::Started;

                Poll::Pending
            }
            TimerFutureState::Started => Poll::Pending,
            TimerFutureState::Done => Poll::Ready("thanks for waiting".to_string()),
        }
    }
}

async fn fake_main() {
    let timer_future = TimerFuture::new(Duration::from_secs(2));

    let result = timer_future.await;

    println!("timer_future returned: {}", result);

    let timer_future2 = TimerFuture::new(Duration::from_secs(2));

    let join_handle = spawn(timer_future2);

    let result2 = join_handle.await;

    println!("timer_future2 returned: {}", result2);
}

const THREAD_COUNT: usize = 4;

thread_local! {
    static THREAD_ID: Cell<Option<usize>> = Cell::new(None);
}

static DEQUES: RwLock<Vec<Mutex<VecDeque<Arc<dyn TaskTrait>>>>> = RwLock::new(Vec::new());
static THREADS: OnceLock<Vec<Thread>> = OnceLock::new();

static BARRIER: OnceLock<Barrier> = OnceLock::new();

static SHUTDOWN: AtomicBool = AtomicBool::new(false);
static ACTIVE_TASKS: Mutex<u64> = Mutex::new(0);

//helper function for run
fn on_task_complete() {
    let done = {
        let mut tasks = ACTIVE_TASKS.lock().unwrap();
        *tasks -= 1;
        *tasks == 0
    };

    if done {
        SHUTDOWN.store(true, Ordering::Relaxed);
        if let Some(threads) = THREADS.get() {
            for thread in threads {
                thread.unpark();
            }
        }
    }
}

fn main_for_workers() {
    let thread_id = {
        let mut deques = DEQUES.write().unwrap();
        let id = deques.len();
        deques.push(Mutex::new(VecDeque::new()));
        id
    };
    THREAD_ID.with(|id| id.set(Some(thread_id)));

    BARRIER.get().unwrap().wait();

    if thread_id == 0 {
        for _ in 0..4 {
            let fake_main = fake_main();
            spawn(fake_main);
        }
    }

    run();
}

fn main() {
    BARRIER.set(Barrier::new(THREAD_COUNT + 1)).unwrap();

    let handles = (0..THREAD_COUNT)
        .map(|_| thread::spawn(move || main_for_workers()))
        .collect::<Vec<_>>();

    THREADS
        .set(handles.iter().map(|h| h.thread().clone()).collect())
        .unwrap();

    BARRIER.get().unwrap().wait();

    for handle in handles {
        let _ = handle.join();
    }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(
    |clone_me| unsafe {
        let arc = Arc::from_raw(clone_me as *const Arc<dyn TaskTrait>);
        mem::forget(Arc::clone(&arc));
        RawWaker::new(Arc::into_raw(arc) as *const (), &VTABLE)
    },
    |wake_me| unsafe {
        let arc = Arc::from_raw(wake_me as *const Arc<dyn TaskTrait>);
        let _ = DEQUES.read().unwrap()[Arc::clone(&*arc).get_thread_id()]
            .lock()
            .unwrap()
            .push_back(Arc::clone(&*arc));
        if let Some(threads) = THREADS.get() {
            for thread in threads {
                thread.unpark();
            }
        }
    },
    |wake_by_ref_me| unsafe {
        let arc = Arc::from_raw(wake_by_ref_me as *const Arc<dyn TaskTrait>);
        let _ = DEQUES.read().unwrap()[Arc::clone(&*arc).get_thread_id()]
            .lock()
            .unwrap()
            .push_back(Arc::clone(&*arc));
        if let Some(threads) = THREADS.get() {
            for thread in threads {
                thread.unpark();
            }
        }
        mem::forget(arc);
    },
    |drop_me| unsafe {
        drop(Arc::from_raw(drop_me as *const Arc<dyn TaskTrait>));
    },
);

trait TaskTrait: Send + Sync {
    fn poll(self: Arc<Self>, cx: &mut Context) -> Poll<()>;
    fn get_thread_id(self: Arc<Self>) -> usize;
    fn new_parent_thread(self: Arc<Self>, thread_id: usize);
}

struct Task<T> {
    future: Arc<Mutex<dyn Future<Output = T> + Send + Sync>>,
    join_handle: JoinHandle<T>,
    thread_id: Mutex<usize>,
}

impl<T> Task<T> {
    fn new(
        future: Arc<Mutex<dyn Future<Output = T> + Send + Sync>>,
        join_handle: JoinHandle<T>,
    ) -> Self {
        Task {
            future,
            join_handle,
            thread_id: Mutex::new(THREAD_ID.with(|id| id.get().unwrap())),
        }
    }
}

impl<T: Send + Sync> TaskTrait for Task<T> {
    fn poll(self: Arc<Self>, cx: &mut Context) -> Poll<()> {
        let mut pinned_future = unsafe { Pin::new_unchecked(self.future.lock().unwrap()) };

        match Future::poll(pinned_future.as_mut(), cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(val) => {
                *self.join_handle.result.lock().unwrap() = Some(val);
                if let Some(waker) = (*self.join_handle.waker.lock().unwrap()).take() {
                    waker.wake();
                }
                Poll::Ready(())
            }
        }
    }

    fn get_thread_id(self: Arc<Self>) -> usize {
        *self.thread_id.lock().unwrap()
    }

    fn new_parent_thread(self: Arc<Self>, thread_id: usize) {
        *self.thread_id.lock().unwrap() = thread_id;
    }
}

struct JoinHandle<T> {
    result: Arc<Mutex<Option<T>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> JoinHandle<T> {
    fn new() -> Self {
        JoinHandle {
            result: Arc::new(Mutex::new(None)),
            waker: Arc::new(Mutex::new(None)),
        }
    }

    fn clone(&self) -> Self {
        JoinHandle {
            result: Arc::clone(&self.result),
            waker: Arc::clone(&self.waker),
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let result = (*self.result.lock().unwrap()).take();

        match result {
            Some(val) => Poll::Ready(val),
            None => {
                *self.waker.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

fn spawn<T: Send + Sync + 'static>(
    future: impl Future<Output = T> + Send + Sync + 'static,
) -> JoinHandle<T> {
    let join_handle = JoinHandle::new();
    let task = Task::new(Arc::new(Mutex::new(future)), join_handle.clone());

    *ACTIVE_TASKS.lock().unwrap() += 1;

    let id = THREAD_ID.with(|id| id.get().unwrap());
    DEQUES.read().unwrap()[id]
        .lock()
        .unwrap()
        .push_back(Arc::new(task));

    if let Some(threads) = THREADS.get() {
        for thread in threads {
            thread.unpark();
        }
    }

    join_handle
}

fn run() {
    let id = THREAD_ID.with(|id| id.get().unwrap());

    loop {
        if SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }

        let task = { DEQUES.read().unwrap()[id].lock().unwrap().pop_front() };
        if let Some(task) = task {
            println!("executor: thread {} working on own task", id);
            let raw_waker = RawWaker::new(
                Arc::into_raw(Arc::new(Arc::clone(&task))) as *const (),
                &VTABLE,
            );
            let waker = unsafe { Waker::from_raw(raw_waker) };
            let mut cx = Context::from_waker(&waker);

            if let Poll::Ready(_) = TaskTrait::poll(task, &mut cx) {
                on_task_complete();
            }
        } else {
            let mut found_work = false;

            for i in 1..THREAD_COUNT {
                let thread_id_we_gone_steal_from = (id + i) % THREAD_COUNT;
                let task = {
                    DEQUES.read().unwrap()[thread_id_we_gone_steal_from]
                        .lock()
                        .unwrap()
                        .pop_back()
                };
                if let Some(task) = task {
                    found_work = true;

                    Arc::clone(&task).new_parent_thread(id);

                    println!(
                        "executor: thread {} working on stolen task from thread {}",
                        id, thread_id_we_gone_steal_from
                    );
                    let raw_waker = RawWaker::new(
                        Arc::into_raw(Arc::new(Arc::clone(&task))) as *const (),
                        &VTABLE,
                    );
                    let waker = unsafe { Waker::from_raw(raw_waker) };
                    let mut cx = Context::from_waker(&waker);

                    if let Poll::Ready(_) = TaskTrait::poll(task, &mut cx) {
                        on_task_complete();
                    }
                }
            }

            if !found_work {
                thread::park();
            }
        }
    }
}
