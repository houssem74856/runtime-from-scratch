# runtime-from-scratch

in apps if we want to achieve concurrency or parallelism we use OS threads, we spawn a thread and tell it what it should do and it starts immediately doing it's job along
with what the main thread is doing, but this spawning takes significant space in memory, so tasks in rust or goroutines in go or green threads came to solve this problem,
cause they are more lightweight, but the kernel used to handle the scheduling of OS threads, with tasks we need our own mechanism to handle their scheduling and their running,
and that is the work of a runtime, a famous runtime in rust is tokio, we usually use tokio without questioning how it works underneath, and for me I was already really
interested in concurrency and parallelism and so I naturally wanted to implement what tokio is doing for me, not to be able to use it in production apps, but just to
understandwhat's going on.

## project structure

`main.rs`: simple single-task executor.

`bin/spawn-queue.rs`: multi-task executor with a queue to store tasks, and spawn to push to it.

`bin/work-stealing.rs`: multi-threaded executor with work(task) stealing between threads.

`bin/reactor.rs`: multi-threaded executor + reactor to handle waking more efficiently.

## in depth

this project went through 4 stages, I am going to talk about what's new in each stage in depth: 

### stage 1: single-task executor
my starting point is what rust gives us, which is the waker. the whole idea of a waker is: we want something that polls a task till completion, but some stuff in a task needs
time to be ready, and so it would waste resources to keep trying until it's ready, and so we set a waker to tell the executor "hey this task is ready to advance again".

rust gives us these four structs to build that:

`RawWakerVTable`: four closures we define: clone, wake, wake_by_ref, drop.

`RawWaker`: fat pointer: pointer to data (we decide what that data is) + reference to RawWakerVTable.

`Waker`: RawWaker but at a level where we can use it safely.

`Context`: just a reference to a Waker.

the user's main becomes `async fake_main()`, and in our main we call it and get a future (a struct that impls Future), and what we need to do is run it.

**fn run:**

polls the future passed to it until it returns ready. poll takes two parameters: `Pin<&mut impl Future>` and `&mut Context` :

**on Pin:** needed because of self referential structs. if inside poll we move the struct, pointers to things inside it would now point to the old location. to prevent that,
poll enforces Pin.

what Pin means to me: the thing pinned either implements `Unpin` (auto implemented for everything except async blocks and what async functions return) and you can move it
freely, just switch from Pin to normal `&mut` via `get_mut()`. or it implements `!Unpin` (async blocks and what async functions return) and you need unsafe + 
`get_unchecked_mut()` to get `&mut` to it, unsafe here means you take responsibility of not producing dangling pointers by moving the future in the case it has self
referential pointers because a clear note here is that `!Unpin` doesn’t automatically mean self referential, for example `async { 1u32 }` is `!Unpin`.

in some cases you can't pin a mutable reference to a future because the future's lifetime would end before you're done with repeated poll calls, and so we use `Box::pin`
which produces `Pin<Box<T>>`. to call poll on it you switch from `Pin<Box<T>>` to `Pin<&mut T>` using `as_mut()`.

**on Context setup:**

the main thing to think about is waker data and it's four functions. the main one is wake, it's role is to tell fn run to poll the future once again.

wake and fn run both need access to the same data, which tells us data should be an `Arc`. in it's simplest form data is a `Condvar`, where fn run calls `data.wait()`
and wake calls `data.signal()`, then the future gets polled again.

but spurious wakeups exist (caused by undetermined thread behavior), we can't prevent them, so we deal with them by adding an `Arc<Mutex<bool>>` alongside the condvar,
that way we always check the bool to confirm the wakeup is real before polling again.

### stage 2: multi-task executor
coming soon

### stage 3: multi-threaded executor
coming soon

### stage 4: reactor
coming soon
