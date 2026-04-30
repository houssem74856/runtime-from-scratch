use std::{
    cell::RefCell,
    mem,
    pin::Pin,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread,
    time::Duration,
};

thread_local! {
  static TX: RefCell<Option<Sender<Arc<dyn TaskTrait>>>> = RefCell::new(None);
  static ACTIVE_TASKS: RefCell<u64> = RefCell::new(0);
}

fn get_tx() -> Sender<Arc<dyn TaskTrait>> {
    TX.with(|t| t.borrow().as_ref().unwrap().clone())
}

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

fn main() {
    let (tx, rx) = channel();
    TX.with(|t| *t.borrow_mut() = Some(tx));

    let fake_main = fake_main();

    spawn(fake_main);

    run(rx);
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    |clone_me| unsafe {
        let arc = Arc::from_raw(clone_me as *const Arc<dyn TaskTrait>);
        mem::forget(Arc::clone(&arc));
        RawWaker::new(Arc::into_raw(arc) as *const (), &VTABLE)
    },
    |wake_me| unsafe {
        let arc = Arc::from_raw(wake_me as *const Arc<dyn TaskTrait>);
        let _ = TaskTrait::get_tx_task(Arc::clone(&*arc)).send(Arc::clone(&*arc));
    },
    |wake_by_ref_me| unsafe {
        let arc = Arc::from_raw(wake_by_ref_me as *const Arc<dyn TaskTrait>);
        let _ = TaskTrait::get_tx_task(Arc::clone(&*arc)).send(Arc::clone(&*arc));
        mem::forget(arc);
    },
    |drop_me| unsafe {
        drop(Arc::from_raw(drop_me as *const Arc<dyn TaskTrait>));
    },
);

trait TaskTrait {
    fn poll(self: Arc<Self>, cx: &mut Context) -> Poll<()>;
    fn get_tx_task(self: Arc<Self>) -> Sender<Arc<dyn TaskTrait>>;
}

struct Task<T> {
    future: Arc<Mutex<dyn Future<Output = T>>>,
    join_handle: JoinHandle<T>,
    tx: Sender<Arc<dyn TaskTrait>>,
}

impl<T> Task<T> {
    fn new(future: Arc<Mutex<dyn Future<Output = T>>>, join_handle: JoinHandle<T>) -> Self {
        Task {
            future,
            join_handle,
            tx: get_tx(),
        }
    }
}

impl<T> TaskTrait for Task<T> {
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

    fn get_tx_task(self: Arc<Self>) -> Sender<Arc<dyn TaskTrait>> {
        self.tx.clone()
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

fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let join_handle = JoinHandle::new();
    let task = Task::new(Arc::new(Mutex::new(future)), join_handle.clone());

    ACTIVE_TASKS.with(|c| {
        *c.borrow_mut() += 1;
    });

    let _ = get_tx().send(Arc::new(task));

    join_handle
}

fn run(rx: Receiver<Arc<dyn TaskTrait>>) {
    loop {
        let task = rx.recv().unwrap();

        let raw_waker = RawWaker::new(
            Arc::into_raw(Arc::new(Arc::clone(&task))) as *const (),
            &VTABLE,
        );
        let waker = unsafe { Waker::from_raw(raw_waker) };
        let mut cx = Context::from_waker(&waker);

        if let Poll::Ready(()) = TaskTrait::poll(task, &mut cx) {
            let done = ACTIVE_TASKS.with(|c| {
                *c.borrow_mut() -= 1;
                *c.borrow() == 0
            });

            if done {
                break;
            }
        }
    }
}
