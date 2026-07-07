# Building a Single-Node KV Engine to Saturate an Optane SSD — in Java

*December 20, 2018*

I spent the past few weeks going deep into a problem I don't usually get to touch in my day job: squeezing every last drop of throughput out of a single machine's storage stack. The occasion was the [Tianchi Contest — Alibaba Cloud's first PolarDB Database Performance Challenge](https://tianchi.aliyun.com/competition/entrance/231689/introduction), where the goal was to build a single-node storage engine on top of an NVMe Optane SSD and race it against everyone else's. Both C++ and Java were allowed.

My final result: **first place among all Java entries, and 20th overall** (out of 1,653 participants — team name: neoremind). The gap between me and the top C++ finisher was just **2.1% (under 9 seconds)**. It's worth saying upfront: when you're trying to fully exploit the hardware, the closer you sit to the metal the better, and Java carries some inherent disadvantages here. But after all these years as a seasoned JAVAer, I wanted to take the challenge anyway.

This is my write-up of how I solved it. The full source code lives at [https://github.com/neoremind/2018-polar-race](https://github.com/neoremind/2018-polar-race). The architecture I describe is language-agnostic — it just happens to be implemented in Java. Rewriting it in C++ would very likely push the numbers higher.

Here's the roadmap for the rest of the article:

1. The contest problem
2. My thinking before writing a line of code, and the final scores
3. Storage design
4. Implementation deep-dive: Write
5. Implementation deep-dive: Read
6. Implementation deep-dive: Range
7. What makes a Java implementation special (and hard)
8. Reflections on single-node database engines
9. Wrap-up

---

## 1. The Contest Problem

You're given an Intel Optane SSD as storage, with memory capped at 3G via cgroup (for the Java track). The task: implement a simplified, high-performance KV storage engine supporting three interfaces — `Write`, `Read`, and `Range`.

The evaluation program runs in two phases:

**1. Correctness evaluation.**
This phase writes specific data concurrently (key 8B, value 4KB) while issuing an arbitrary number of `kill -9` signals to simulate unexpected process crashes. (Your engine must guarantee that persisted data survives an unexpected exit.) It then reopens the DB and calls `Read` and `Range` to verify correctness.

**2. Performance evaluation.**
- **2.1 Random write:** 64 threads writing concurrently, each calling `Write` 1 million times with random data (key 8B, value 4KB).
- **2.2 Random read:** 64 threads reading concurrently, each calling `Read` 1 million times at random.
- **2.3 Sequential read:** 64 threads reading concurrently, each using `Range` to iterate the entire DB in global order — twice.

A couple of important details:
1. After each phase, the page cache is cleared, and the clearing time counts toward the total.
2. `Read` and `Range` verify that keys and values match, and `Range` additionally verifies ordering.

---

## 2. Thinking Before Implementation, and the Final Scores

The problem asks for a single-node KV engine that guarantees high write throughput, low-latency point lookups, range queries, and crash consistency — all at once. The first thing that popped into my head was LevelDB and RocksDB, both built on WAL + LSM-tree. But those are general-purpose engines. As everyone knows, the LSM-tree architecture turns random writes into sequential writes, at the cost of multi-level compaction and lookups — which means **write amplification and read amplification**.

Back in the HDD era, that trade was well worth it: random disk writes were roughly 1000x more expensive than sequential ones, so anything that converted random to sequential paid for itself. On SSDs, though, that penalty is much smaller — and Optane in particular has excellent concurrent read/write performance. So reaching for a stock LSM-tree in this contest would be the wrong move.

Coming back to the problem, let's pull out what actually matters:
1. **Fixed-length KV** (key 8B, value 4KB)
2. **Large values** (4K)
3. **64-way concurrent queries**

Following the LSM-tree thread of thought, I was reminded of a paper — ["WiscKey: Separating Keys from Values in SSD-Conscious Storage"](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), published by the University of Wisconsin in 2016. Its core idea is how to optimize LevelDB *specifically for SSDs*: reduce read/write amplification, maximize bandwidth utilization, and lean into SSD characteristics like high sequential-IO throughput and outstanding random-concurrency performance. This idea — **separating keys from values** — fits the problem beautifully, given the large values and concurrent queries. I combined the paper's insights with the specifics of the contest to arrive at my storage design and engine implementation. The fixed-length KV requirement, conveniently, simplified things even further.

Since the contest scores you purely on total elapsed time, the whole problem reduces to a single question: **how do you saturate the IO and hit maximum throughput?**

Let's do the math first. Total random write volume is `(4K + 8 bytes) * 64 * 1M ≈ 256G`. Random read is another 256G. Two `Range` passes scan `512G`. According to [Intel's official Optane SSD specs](https://www.intel.com/content/www/us/en/products/memory-storage/solid-state-drives/data-center-ssds/optane-dc-p4800x-series/p4800x-375gb-2-5-inch-20nm.html): sequential write 2G/s, sequential read 2.4G/s, random read 550K IOPS, random write 500K IOPS, read/write latency ~10µs. Interestingly, the measured throughput and IOPS for sequential write and random read came out [slightly above the official numbers](https://lenovopress.com/lp0770-intel-p4800x-performance-nvme-pcie-ssd) — I measured sequential write at 2.2G/s, random read at 2.5G/s, and sequential read faster still. Theoretically, the absolute floor is around 410s.

The first-place C++ finisher's score:

```
413.69s  (Write 116s + Read 103s + Range 193s)
Write throughput: 2.21G/s   Read throughput: 2.49G/s   Range throughput: 2.65G/s
```

That's essentially the disk fully saturated.

My score — first place in Java:

```
422.31s  (Write 116s + Read 109s + Range 196s)
Write throughput: 2.21G/s   Read throughput: 2.35G/s   Range throughput: 2.61G/s
```

---

## 3. Storage Design

Rather than an LSM-tree, I applied WiscKey's central idea: **separate keys from values**.

The design has two files. A **WAL** (Write-Ahead Log) stores each key alongside the offset of its value in the vlog. The **vlog** stores the values themselves, written sequentially. Both the WAL and the vlog are **append-only, fixed-length writes**. Because of the fixed length, the WAL only needs to store the vlog *sequence number* — a 4-byte int (endianness is up to you) — and multiplying that by 4096 gives the byte offset within the vlog file. The problem doesn't require deletion, so vlog garbage collection is a non-issue here.

![Data storage format: WAL and vlog](https://neoremind.com/wp-content/uploads/data_storage_format.png)

Since both files are pure sequential IO, there's **no write amplification** of the kind an LSM-tree suffers.

There's a catch, though. Because keys and values are separated, each write has to hold a lock — and locks cap your concurrency. Since the writes are random, the natural fix is divide-and-conquer: **shard the data to reduce contention**. My strategy was to split by key lexicographic order into **1024 shards**. Concretely, I take the first byte of the key (8 bytes total) plus the top 2 bits of the second byte, convert that to an int, and run it through a partition function to route to the correct shard.

![Sharded DB layout](https://neoremind.com/wp-content/uploads/shareddb.png)

---

## 4. Implementation Deep-Dive: Write

With 1024 shards, each write locks a single shard. The flow is simple:

```
synchronized(lock) {
  write vlog 4k;
  write wal 8byte + 4byte vlog seq;
  vlog seq++;
}
```

For the actual IO, you have two families to choose from: **buffered IO** and **direct IO**. And buffered IO further splits into VFS read/write versus mmap.

**The WAL uses mmap.** Two reasons:

- **One fewer memory copy.** In Java, a `FileChannel` write travels through `byte[] → off-heap direct memory → kernel page cache → disk`. With mmap, the path collapses to `byte[] → memory-mapped address (also page cache) → disk`. mmap maps the file directly into user-space-accessible memory; reads and writes touch memory directly without writing through to disk, needing only a single `mmap` system call. On DB close, you just truncate the trailing unused bytes.
- **Crash consistency.** Since the contest only issues `kill -9` and never simulates a power loss, we can lean on the page cache — no synchronous flush needed to guarantee consistency. On the second startup, because vlog sequence numbers are strictly increasing, reading the WAL and hitting `0x00000000` tells us everything after that point is garbage and can be discarded.

Some sizing: the total WAL size is `(8-byte key + 4-byte vlog seq) * 64 concurrency * 1M = 768MB`. Because the evaluation program is sufficiently random, each WAL comes out to `768MB / 1024 ≈ 750KB`. So each shard can be mmap'd directly as a file of size `12byte << 16`, with re-mmap on ensure-capacity — this handles the uneven file sizes in the correctness test, where writes concentrate in just 5 shards.

Each vlog file is `4K * 64M / 1024 = 256MB`.

**The vlog uses direct IO.** If I used Java's `FileChannel` here, I'd pay for an extra copy from heap to off-heap direct memory relative to C++ — and going through the page cache means the subsequent `echo 3 > /proc/sys/vm/drop_caches` also counts against my time. So direct IO it is. The JDK doesn't offer a direct IO API, so the options are calling into libc through [JNA](https://github.com/java-native-access/jna) via JNI, or using [jaydio](https://github.com/smacke/jaydio) (which wraps JNA). I went straight to the JNA API.

Sequential IO solves one problem, but to *truly* saturate the bandwidth you need **large block IO**. To combine direct IO with crash consistency, you need an mmap sitting out front. My approach: buffer up 4 values (16K of value) before flushing to disk, repeatedly rewriting the same mmap'd memory region. On a normal shutdown, delete the mmap temp file; otherwise, on the next initialization, append the mmap file's contents to the WAL to recover.

The whole Write phase triggered **4 young GCs and zero full GCs**. The on-CPU flame graph shows 86% IO + 4% lock overhead + the rest. That's basically hitting the target.

![Write phase on-CPU flame graph](https://neoremind.com/wp-content/uploads/write-flame(1).png)

---

## 5. Implementation Deep-Dive: Read

Initializing the database — as partly covered in the Write analysis — means the WAL and vlog both need some work to guarantee crash consistency. The next question is how to build an index to support point lookups.

The total WAL is 768MB, which comfortably fits in memory. Building the index per shard goes like this:

1. Load the WAL into `key 8byte + vlog seq 4byte` records.
2. Sort those 12 bytes — first by key lexicographic order, then take the max vlog seq to handle duplicate keys.
3. Place the sorted data in memory.

This is independent per shard, so it parallelizes. **This is one area where Java falls short:** 64-way concurrent loading takes 1.5–2.5s with jitter — well behind C++'s 300ms. Whether I compared bytes one at a time or converted keys to unsigned longs first, the difference was small; the culprit, on analysis, is cache-line behavior. Since the next phase (Range) needs the same sorted data, I can optionally persist the sorted, deduplicated `key + vlog seq` to disk as a `wal.sort` file to avoid repeating the work.

Because each `key 8byte + vlog seq 4byte` record is fixed-length, an in-memory **binary search** is all you need. My implementation uses off-heap memory, allocated and freed via the JDK's `Unsafe`, to keep it out of the heap's old region and avoid GC overhead. The binary search cost is tiny — around 1% of total time.

A single point lookup is **one in-memory binary search + one disk IO**. Reading a 4K value from disk just means restoring the actual offset via `vlog seq * 4096` and reading 4K from the vlog. Here again, buffered vs. direct IO matters. In the *internal* contest round, the evaluation read keys in the same order it wrote them, giving strong locality — so buffered IO through the page cache let the OS's read-ahead kick in and sped things up nicely. The *external* round fixed that, so buffered IO with read-ahead became actively harmful, prefetching mountains of useless data into the page cache and wasting bandwidth.

So Read uses **direct IO**. Using direct IO from Java via JNA takes two steps: first align memory with `int posix_memalign(void **memptr, size_t alignment, size_t size)` (call the result `MemPointer`), then read via the `pread(fd, MemPointer, 4096, offset)` syscall into that address, and finally copy it into the user-space heap. That process needs a lock; going lock-free requires pooling the `MemPointer`, and in practice the two approaches came out about even.

One small trick to avoid frequent young GCs: the 64 threads read their 4K values through a `ThreadLocal`, avoiding repeated allocation. (Thanks to [@岛风](https://mp.weixin.qq.com/s/8r_IirkTO7Ya3e9yalJT_g) for the reminder — a fellow Java contestant whose own write-up is linked here.)

This phase has the **largest gap versus C++**: throughput of 2.35G/s against C++'s 2.49G/s. That 140MB/s deficit comes down to slow index building, plus direct IO through JNA/JNI simply not being as fast as raw syscalls.

The whole Read phase: 4–5 young GCs, no full GC.

---

## 6. Implementation Deep-Dive: Range

**This is the phase where the contest was won or lost.** The problem does two 64-way concurrent `Range` passes — that's 128 full scans, which is basically impossible to brute-force. There are generally two ways to attack it: (1) a "ride-sharing" model, where the 64 concurrent callers wait while an async thread visits and invokes callbacks; or (2) 64 threads visiting in lockstep. I chose the former, implementing an `AccumulativeRunner` utility on top of `java.util.concurrent`.

The basic idea: concurrent `Range` requests arrive, each submits a Range Task and blocks. A background trigger fires on one of two conditions — enough Range Tasks have accumulated, or a timeout (say 5s) elapses — at which point it triggers a single full scan, invokes every request's visitor callback, and once the scan is done, notifies all the blocked Range threads to unblock. Then the second Range pass runs.

![AccumulativeRunner sequence diagram](https://neoremind.com/wp-content/uploads/acc_runner.png)

Next, how do you make a single full scan efficient? My first attempt used a **"sliding window + concurrent random IO query"** model, hoping to exploit the SSD's concurrent-random-IO strength. The results disappointed, for two reasons: first, buffered IO through the page cache reads slower than a single large-block IO load into memory; and second, Java's `FileChannel` holds an internal position lock that throttles performance.

So I abandoned that and switched to **"sliding window + concurrent in-memory query"** — and it worked, coming close to fully saturating the bandwidth. Since keys are sharded into 1024 pieces in lexicographic order, the idea is to walk the 1024 shards in order, loading each into memory for access and seamlessly chaining one shard to the next. Each shard's access breaks into three steps:

1. **Prefetch:** sort the WAL to build the index, and load the vlog into memory.
2. **Range read:** iterate the sorted WAL, and for each key + vlog seq, find its value — which is now a *memory access*. This is the essence of "concurrent in-memory query."
3. **Evaluator visit:** the evaluation program verifies ordering, value correctness, etc., which itself costs something.

To seamlessly chain 1024 shards through those three steps, I use a **sliding window**. The window classifies shards into five categories:
- Already visited and finished
- Currently being Range-read and visited
- Prefetch done, ready to be read
- Currently prefetching
- Not yet visited

![Sliding window over the 1024 shards](https://neoremind.com/wp-content/uploads/slidingwindow.png)

The window holds at most **3 shards**, for a peak memory of `vlog (256MB * 3) + wal index (750KB * 3) ≈ 770MB` — which keeps us safely under the cgroup memory limit.

The bottleneck among prefetch, Range read, and evaluator visit should ultimately land on *prefetch* — that's how you saturate the bandwidth. So for the latter two steps, benchmarking everything showed they really can drag things down; the old trick applies — turn serial into parallel with a **multi-stage pipeline** architecture.

Prefetching each shard means building the sorted-WAL index and loading + caching the vlog — and those two can run in parallel.

For **index building**, load the WAL (or `wal.sort` file) into memory, exactly as in the Read phase: sorted keys and vlog seqs go into off-heap memory as the index, for the Range read to consume.

For **loading and caching the vlog**, this part can add concurrency: a single shard's vlog is 256MB, loaded in parallel with 8-way concurrency * 32M large-block IO reads. You can load via mmap / FileChannel / direct IO; direct IO and FileChannel came out about even in practice, and I settled on direct IO. For caching, the options are off-heap `DirectBuffer` / `Unsafe`-allocated memory / heap — off-heap direct memory and `Unsafe` were about even. If you use `DirectBuffer`, the subsequent in-memory `get` isn't thread-safe, so you convert to an address and access it lock-free via `Unsafe.copyMemory`. And since the vlog load uses direct IO, I **pool the MemPointer** here — pre-allocating `3 windows * 8 concurrency = 24` MemPointers of 32MB each, so reads become concurrent, lock-free memory-address accesses.

For the **Range read**, batch-read 256 KVs with 2-way parallelism, then drop the results into a lock-free queue. The evaluator's visit function runs in a separate thread, polling that lock-free queue and, for each KV, fanning out the 64 visitor callbacks across 4-way parallelism. That keeps neither of these two steps a bottleneck.

For shards that are done, you release resources — WAL index and vlog cache — and return the MemPointers to the pool.

---

## 7. What Makes a Java Implementation Special (and Hard)

Java carries real disadvantages in a contest like this, and that's directly why — despite finishing first *in Java* — I only placed 20th overall. Still, being within 2.1% (under 9s) of the top C++ finisher is a result I'm pretty happy with.

The JVM's disadvantages versus C++:

1. **Not close enough to the metal.** There's a JVM layer in between, interpreting bytecode. JIT does compile hot code to native, but it's still not as direct.
2. **GC overhead.** Cache things on the heap and you'll GC frequently, hurting throughput. Concurrent GC alongside user threads helps, but you *must* keep it to young GC only, never full GC. This contest is IO-bound, so CPU is theoretically plentiful — I observed a median of 400%–600% CPU usage.
3. **Awkward access to OS APIs.** Native JDK doesn't support direct IO, releasing mmap'd memory is clumsy, binding threads to CPU cores is inconvenient, and so on.

To overcome all this as a Java contestant, you have to reach for some heavy artillery. Let me summarize the tools, one by one.

**1. mmap.** Used in the Write phase for the WAL to guarantee crash consistency. The JDK offers a native API, though releasing the mapping is a hassle.

**2. Direct IO.** Via JNA wrapping, or jaydio — stitching small IOs into large-block writes. `FileChannel` went completely unused in this contest, because its internal position lock and buffered-IO path make it unsuitable here. That said, in the vast majority of Java IO scenarios, NIO's `FileChannel` is the first choice.

**3. Off-heap memory.** Either `DirectBuffer` or `Unsafe.malloc`/`free`.

**4. GC control.** My contest flags:

```
-server -XX:-UseBiasedLocking -Xms2000m -Xmx2000m -XX:NewSize=1400m \
-XX:MaxMetaspaceSize=32m -XX:MaxDirectMemorySize=1G -XX:+UseG1GC
```

Young GC is rare in Write and Read, and mostly happens in Range. Because the multi-stage pipeline is memory-hungry, young GC is relatively frequent there — but there's no full GC, so it's acceptable.

**5. Pooling.** Pre-allocate direct memory, pool it, and rewrite it repeatedly to reuse resources. In the Read phase, reuse the value buffer via `ThreadLocal` to avoid frequent young GCs.

**6. Lock control.** KV-separated writes inevitably need locks. The Read phase's direct IO — loading into one memory region, then handing it back to user space — also needs a lock. Keep the lock granularity as small as possible and scatter contention, à la `ConcurrentHashMap`, and you drive lock overhead to a minimum.

**7. Concurrency toolkit.** Use `java.util.concurrent` well — the Range phase's ride-sharing model, parallel vlog loading, and sliding window all lean on thread pools, locks, conditions, and mutexes. Lock-free libraries like `ConcurrentLinkedQueue`, [jctools' `MpmcArrayQueue`](https://github.com/JCTools/JCTools), and [Disruptor's lock-free queue](https://github.com/LMAX-Exchange/disruptor) are worth trying too — I experimented with all of them in the contest. In practice, plain lock-free was enough; the bottleneck is IO, so these are negligible.

**8. Reducing context switches.** The contest used AliJDK, which has a Wisp API for Java coroutines, usable in scenarios where resource release doesn't require waiting. Trying it, `vmstat -w 1` did show a lower `cs` column — but it didn't meaningfully improve my score.

---

## 8. Reflections on Single-Node Database Engines

**First**, in my view, the factors that determine a database's performance rank as: **storage architecture design > engine implementation quality > language choice**. So for this particular problem, Java and C++ have no qualitative difference.

**Second**, in the platform/application-service domain, Java's strengths are engineering maturity, rich libraries, and design-pattern support — so there's plenty of room for it. In the distributed-computing domain, the jump from HDD to SSD is a ms-to-µs improvement, whereas a same-datacenter distributed call is ms-level and a cross-region one is tens of ms — *that's* the real problem. Which is exactly why so many open-source big-data projects (Spark, Hadoop, Flink) are written in Java: a well-engineered, maintainable language that still meets the requirements.

**Third**, on choosing an implementation language for a storage engine, consider:
1. **Engineering** — e.g., static typing and abstraction/design support.
2. **Tail-latency control.**
3. **Runtime overhead.**

For Java, points 2 and 3 are weaknesses versus C++ (GC and JVM overhead) — and yet those two are exactly what a DB demands. From a system-layering perspective, C++ and Rust suit the layers sensitive to (2) and (3), while Java and Go have their place where engineering maturity matters more.

---

## 9. Wrap-Up

This was my first time in an engineering-focused contest, surrounded by experts all fighting over seconds and milliseconds — thrilling stuff. It was also my first time systematically putting Java IO techniques into practice, and my goal of learning and accumulating experience was met. I hope I'll still have the energy and drive to compete in the future, to learn from the best and sharpen my skills through friendly rivalry. The road of technology is long, and every step of accumulation is an investment in a better tomorrow. Here's to that — for you reading this, and for me.

---

*Source code: [https://github.com/neoremind/2018-polar-race](https://github.com/neoremind/2018-polar-race)*
