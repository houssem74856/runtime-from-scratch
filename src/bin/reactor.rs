use std::{
    cell::Cell,
    collections::{HashMap, VecDeque},
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

use libc::{
    AF_INET, EAGAIN, EPOLL_CTL_ADD, EPOLL_CTL_MOD, EPOLLIN, EPOLLOUT, EWOULDBLOCK, F_GETFL,
    F_SETFL, O_NONBLOCK, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, epoll_event, in_addr, sockaddr_in,
};

use std::os::unix::io::RawFd;

const THREAD_COUNT: usize = 4;

thread_local! {
    static THREAD_ID: Cell<Option<usize>> = Cell::new(None);
}

static DEQUES: RwLock<Vec<Mutex<VecDeque<Arc<dyn TaskTrait>>>>> = RwLock::new(Vec::new());
static THREADS: OnceLock<Vec<Thread>> = OnceLock::new();

static BARRIER: OnceLock<Barrier> = OnceLock::new();

static SHUTDOWN: AtomicBool = AtomicBool::new(false);
static ACTIVE_TASKS: Mutex<u64> = Mutex::new(0);

const MAX_EVENTS: usize = 32;

static REACTOR: OnceLock<Reactor> = OnceLock::new();

//helper functions
fn make_nonblocking(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, F_GETFL, 0);
        libc::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
}

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

//futures
struct TcpListener {
    server_fd: RawFd,
}

impl TcpListener {
    fn bind(port: u16) -> Self {
        let server_fd = unsafe {
            let fd = libc::socket(AF_INET, SOCK_STREAM, 0);
            assert!(fd >= 0);

            let opt: i32 = 1;
            libc::setsockopt(
                fd,
                SOL_SOCKET,
                SO_REUSEADDR,
                &opt as *const _ as *const _,
                4,
            );

            let addr = sockaddr_in {
                sin_family: AF_INET as _,
                sin_port: port.to_be(),
                sin_addr: in_addr { s_addr: 0 },
                sin_zero: [0; 8],
            };

            libc::bind(
                fd,
                &addr as *const _ as *const _,
                std::mem::size_of::<sockaddr_in>() as _,
            );

            libc::listen(fd, 128);

            make_nonblocking(fd);

            fd
        };

        TcpListener { server_fd }
    }

    fn accept(&self) -> AcceptFuture {
        AcceptFuture {
            server_fd: self.server_fd,
        }
    }
}

pub struct AcceptFuture {
    server_fd: RawFd,
}

impl Future for AcceptFuture {
    type Output = TcpStream;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut client_addr: sockaddr_in = unsafe { std::mem::zeroed() };
        let mut addr_len = std::mem::size_of::<sockaddr_in>() as u32;

        let client_fd = unsafe {
            libc::accept(
                self.server_fd,
                &mut client_addr as *mut _ as *mut _,
                &mut addr_len as *mut _ as *mut _,
            )
        };

        if client_fd == -1 {
            let err = unsafe { *libc::__errno_location() };
            if err == EAGAIN || err == EWOULDBLOCK {
                REACTOR
                    .get()
                    .unwrap()
                    .register_read(self.server_fd, cx.waker().clone());

                return Poll::Pending;
            }
            panic!("accept failed errno={}", err);
        }

        make_nonblocking(client_fd);

        let ip = u32::from_be(client_addr.sin_addr.s_addr);
        let port = u16::from_be(client_addr.sin_port);

        Poll::Ready(TcpStream {
            fd: client_fd,
            peer_ip: ip,
            peer_port: port,
        })
    }
}

pub struct TcpStream {
    fd: RawFd,
    peer_ip: u32,
    peer_port: u16,
}

impl TcpStream {
    fn peer_addr(&self) -> String {
        format!(
            "{}.{}.{}.{}:{}",
            (self.peer_ip >> 24) & 0xff,
            (self.peer_ip >> 16) & 0xff,
            (self.peer_ip >> 8) & 0xff,
            self.peer_ip & 0xff,
            self.peer_port
        )
    }

    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        ReadFuture { fd: self.fd, buf }
    }

    fn write_all<'a>(&'a mut self, data: &'a [u8]) -> WriteFuture<'a> {
        WriteFuture {
            fd: self.fd,
            data,
            written: 0,
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

struct ReadFuture<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
}

impl<'a> Future for ReadFuture<'a> {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let ret = unsafe {
            libc::read(
                self.fd,
                self.buf.as_mut_ptr() as *mut _,
                self.buf.len() as _,
            )
        };

        if ret >= 0 {
            return Poll::Ready(ret as usize);
        }

        let err = unsafe { *libc::__errno_location() };
        if err == EAGAIN || err == EWOULDBLOCK {
            REACTOR
                .get()
                .unwrap()
                .register_read(self.fd, cx.waker().clone());

            return Poll::Pending;
        }

        Poll::Ready(0)
    }
}

struct WriteFuture<'a> {
    fd: RawFd,
    data: &'a [u8],
    written: usize,
}

impl<'a> Future for WriteFuture<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            let remaining = &self.data[self.written..];
            if remaining.is_empty() {
                return Poll::Ready(());
            }

            let ret = unsafe {
                libc::write(
                    self.fd,
                    remaining.as_ptr() as *const _,
                    remaining.len() as _,
                )
            };

            if ret >= 0 {
                self.written += ret as usize;
                continue;
            }

            let err = unsafe { *libc::__errno_location() };
            if err == EAGAIN || err == EWOULDBLOCK {
                REACTOR
                    .get()
                    .unwrap()
                    .register_write(self.fd, cx.waker().clone());

                return Poll::Pending;
            }

            return Poll::Ready(());
        }
    }
}

enum TimerFutureState {
    Unresumed,
    Started,
    Done,
}

struct TimerFuture {
    duration: Duration,
    state: TimerFutureState,
    timer_fd: Option<RawFd>,
}

impl TimerFuture {
    fn new(duration: Duration) -> Self {
        TimerFuture {
            duration,
            state: TimerFutureState::Unresumed,
            timer_fd: None,
        }
    }
}

impl Future for TimerFuture {
    type Output = String;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.state {
            TimerFutureState::Unresumed => {
                let timer_fd = unsafe { libc::timerfd_create(libc::CLOCK_MONOTONIC, 0) };
                if timer_fd == -1 {
                    panic!("Failed to create timerfd");
                }

                make_nonblocking(timer_fd);

                let mut spec: libc::itimerspec = unsafe { std::mem::zeroed() };
                spec.it_value.tv_sec = self.duration.as_secs() as _;
                spec.it_value.tv_nsec = self.duration.subsec_nanos() as _;

                if unsafe { libc::timerfd_settime(timer_fd, 0, &spec, std::ptr::null_mut()) } == -1
                {
                    panic!("Failed to set timerfd");
                }

                self.timer_fd = Some(timer_fd);
                self.state = TimerFutureState::Started;

                REACTOR
                    .get()
                    .unwrap()
                    .register_read(timer_fd, cx.waker().clone());
                Poll::Pending
            }
            TimerFutureState::Started => {
                let mut buf = [0u8; 8];
                let ret = unsafe { libc::read(self.timer_fd.unwrap(), buf.as_mut_ptr().cast(), 8) };

                if ret < 0 {
                    let err = unsafe { *libc::__errno_location() };
                    if err == EAGAIN || err == EWOULDBLOCK {
                        return Poll::Pending;
                    }
                    panic!("accept failed errno={}", err);
                }

                REACTOR.get().unwrap().deregister(self.timer_fd.unwrap());
                self.state = TimerFutureState::Done;
                Poll::Ready("thanks for waiting".to_string())
            }
            TimerFutureState::Done => Poll::Ready("thanks for waiting".to_string()),
        }
    }
}

//reactor
#[derive(Debug)]
struct Reactor {
    epoll_fd: RawFd,
    wakers: Mutex<HashMap<RawFd, Waker>>,
}

impl Reactor {
    pub fn new() -> Self {
        let epoll_fd = unsafe { libc::epoll_create1(0) };
        assert!(epoll_fd >= 0);

        Reactor {
            epoll_fd,
            wakers: Mutex::new(HashMap::new()),
        }
    }

    fn epoll_add(&self, fd: RawFd, events: u32) {
        let mut ev = epoll_event {
            events,
            u64: fd as u64,
        };
        unsafe { libc::epoll_ctl(self.epoll_fd, EPOLL_CTL_ADD, fd, &mut ev) };
    }

    fn epoll_mod(&self, fd: RawFd, events: u32) {
        let mut ev = epoll_event {
            events,
            u64: fd as u64,
        };
        unsafe { libc::epoll_ctl(self.epoll_fd, EPOLL_CTL_MOD, fd, &mut ev) };
    }

    pub fn register_read(&self, fd: RawFd, waker: Waker) {
        let mut wakers = self.wakers.lock().unwrap();

        let already = wakers.contains_key(&fd);
        wakers.insert(fd, waker);
        if !already {
            self.epoll_add(fd, EPOLLIN as u32);
        } else {
            self.epoll_mod(fd, EPOLLIN as u32);
        }
    }

    pub fn register_write(&self, fd: RawFd, waker: Waker) {
        let mut wakers = self.wakers.lock().unwrap();

        let already = wakers.contains_key(&fd);
        wakers.insert(fd, waker);
        if !already {
            self.epoll_add(fd, EPOLLOUT as u32);
        } else {
            self.epoll_mod(fd, EPOLLOUT as u32);
        }
    }

    fn deregister(&self, fd: RawFd) {
        unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
        unsafe { libc::close(fd) };
    }
}

//mains
async fn handle_client(mut socket: TcpStream) {
    let mut buffer = [0u8; 1024];

    loop {
        let bytes_read = socket.read(&mut buffer).await;

        if bytes_read == 0 {
            return;
        }

        let timer_future = TimerFuture::new(Duration::from_millis(100));
        let join_handle = spawn(timer_future);
        let result = join_handle.await;
        println!("timer_future returned: {}", result);

        socket.write_all(&buffer[0..bytes_read]).await;
    }
}

async fn fake_main() {
    let listener = TcpListener::bind(9000);
    println!("server running on port 9000");

    loop {
        let socket = listener.accept().await;
        println!("new client connected: {}", socket.peer_addr());

        spawn(handle_client(socket));
    }
}

fn main_for_reactor() {
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS];
    let reactor = REACTOR.get().unwrap();

    loop {
        let n = unsafe {
            libc::epoll_wait(
                reactor.epoll_fd,
                events.as_mut_ptr(),
                events.len() as i32,
                -1,
            )
        };
        assert!(n >= 0);
        for event in &events[0..n as usize] {
            let fd = event.u64 as RawFd;
            if let Some(waker) = reactor.wakers.lock().unwrap().remove(&fd) {
                waker.wake();
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
        let fake_main = fake_main();
        spawn(fake_main);
    }

    run();
}

fn main() {
    REACTOR.set(Reactor::new()).unwrap();

    thread::spawn(|| main_for_reactor());

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
