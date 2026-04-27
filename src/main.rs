use std::{
    mem,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread,
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
}

fn main() {
    let fake_main = fake_main();

    run(fake_main);
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(
    |clone_me| unsafe {
        let arc = Arc::from_raw(clone_me as *const Park);
        mem::forget(Arc::clone(&arc));
        RawWaker::new(Arc::into_raw(arc) as *const (), &VTABLE)
    },
    |wake_me| unsafe {
        let arc = Arc::from_raw(wake_me as *const Park);
        *arc.0.lock().unwrap() = true;
        arc.1.notify_one();
    },
    |wake_by_ref_me| unsafe {
        let arc = &*(wake_by_ref_me as *const Park);
        *arc.0.lock().unwrap() = true;
        arc.1.notify_one();
    },
    |drop_me| unsafe {
        drop(Arc::from_raw(drop_me as *const Park));
    },
);

struct Park(Arc<Mutex<bool>>, Condvar);

impl Park {
    fn new() -> Self {
        Park(Arc::new(Mutex::new(false)), Condvar::new())
    }
}

fn run<T>(mut future: impl Future<Output = T>) -> T {
    let mut pinned_future = unsafe { Pin::new_unchecked(&mut future) };

    let arc_park = Arc::new(Park::new());
    let raw_waker = RawWaker::new(Arc::into_raw(Arc::clone(&arc_park)) as *const (), &VTABLE);
    let waker = unsafe { Waker::from_raw(raw_waker) };
    let mut cx = Context::from_waker(&waker);

    loop {
        match Future::poll(pinned_future.as_mut(), &mut cx) {
            Poll::Pending => {
                let mut bool = arc_park.0.lock().unwrap();

                while !*bool {
                    bool = arc_park.1.wait(bool).unwrap();
                }
            }
            Poll::Ready(val) => return val,
        }
    }
}
