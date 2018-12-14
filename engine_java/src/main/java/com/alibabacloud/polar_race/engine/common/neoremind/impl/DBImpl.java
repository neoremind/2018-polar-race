package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.neoremind.DB;
import com.alibabacloud.polar_race.engine.common.neoremind.DBConstants;
import com.alibabacloud.polar_race.engine.common.neoremind.ImmutableIndex;
import com.alibabacloud.polar_race.engine.common.neoremind.KeyComparator;
import com.alibabacloud.polar_race.engine.common.neoremind.LogWriter;
import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.VLogReader;
import com.alibabacloud.polar_race.engine.common.neoremind.VlogSeqComparator;
import com.alibabacloud.polar_race.engine.common.neoremind.WalLogIterator;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.index.NopImmutableIndex;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.index.OffHeapImmutableIndex;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache.ConcurrentJNADirectIOLoad2DirectOffheapUsingUnsafeVLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.iterator.MmapWalLogByteArrayIterator;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.reader.JNADirectIOSynchronizedVLogReader;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.writer.JNADirectIOLogWriter;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.writer.MmapLogWriter;
import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Filename;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * DBImpl
 *
 * @author xu.zx
 */
public class DBImpl implements DB, DBConstants {

  /**
   * 用于做shard db分片时候的一个片的编号，默认是0
   */
  private int shardId;

  /**
   * DB的文件夹
   */
  private String path;

  /**
   * DB初始化和写入的锁
   */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * vlog writer
   */
  private LogWriter vLogWriter;

  /**
   * wal writer
   */
  private LogWriter walLogWriter;

  /**
   * vlog reader
   */
  private VLogReader vLogReader;

  /**
   * wal log reader
   */
  private WalLogIterator<byte[]> walLogIterator;

  /**
   * vlog的sequence id。sequence id * 4096k = offset in vlog
   */
  private volatile int vlogSequence;

  /**
   * key to vlog seq index
   */
  private ImmutableIndex immutableIndex;

  private AtomicBoolean writePrepared = new AtomicBoolean(false);

  private ExecutorService cleanerExecutor;

  private ExecutorService concurrentPutQueueExecutor;

  private Queue<byte[][][]> asyncVisitQueue;

  private VLogCache vLogCache;

  /**
   * key to vlog seq index
   */
  private SortedKeyAndVLogSeqs orderedVLogOffset;

  public static boolean ANY_TMP_VLOG_EXISTS = false;

  public DBImpl(String path) {
    this(path, 0);
  }

  public DBImpl(String path, int shardId) {
    this.shardId = shardId;
    this.path = path;
  }

  @Override
  public void write(byte[] key, byte[] value) {
    if (!writePrepared.get()) {
      prepareWrite();
    }
    lock.lock();
    try {
      // 写vlog
      vLogWriter.append(value);

      // 写wal
      walLogWriter.append(key, BytewiseUtil.toBEIntByteArray(vlogSequence));

      // vlog seq + 1
      vlogSequence++;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  private void prepareWrite() {
    synchronized (this) {
      try {
        if (!writePrepared.get()) {
          if (ANY_TMP_VLOG_EXISTS) {
            Recoverables.recoverVLog(path, shardId);
          }

          walLogWriter = new MmapLogWriter(new File(path, Filename.walFileName(shardId)));
          vLogWriter = new JNADirectIOLogWriter(path + File.separator + Filename.vlogFileName(shardId));

          /**
          for failover please uncomment this

          long vLogFileSize = vLogWriter.getFileSize();
          checkState(vLogFileSize % SIZE_OF_VLOG_RECORD == 0, "vlog is corrupt");

          vlogSequence = (int) (vLogFileSize / SIZE_OF_VLOG_RECORD);

         */
          writePrepared.set(true);
        }
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public byte[] read(byte[] key) {
    int offset = immutableIndex.binarySearch(key);
    if (offset == -1) {
      return null;
    }
    return vLogReader.readValue(offset * (long) SIZE_OF_VALUE);
  }

  public void prepareRead() {
    prepareRead(Executors.newFixedThreadPool(4), false);
  }

  public void prepareRead(ExecutorService prepareReadIOAsyncExecutor, boolean distinctWal) {
    if (ANY_TMP_VLOG_EXISTS) {
      Recoverables.recoverVLog(path, shardId);
    }

    try {
      walLogIterator = new MmapWalLogByteArrayIterator(new File(path, Filename.walFileName(shardId)), !distinctWal);
      // walLogIterator = new DirectIOWalLogByteArrayIterator(path, new File(path, Filename.walFileName(shardId)));
    } catch (FileNotFoundException e) {
      immutableIndex = NopImmutableIndex.instance();
      return;
    }

    byte[][] records = new byte[walLogIterator.getRecordSize()][];
    int index = 0;
    while (walLogIterator.hasNext()) {
      records[index++] = walLogIterator.next();
    }

    int actualRecordSize = walLogIterator.getActualRecordSize();
    prepareReadIOAsyncExecutor.submit(this::destroyWalLogIterator);

    Arrays.sort(records, 0, actualRecordSize, KeyComparator.COMPARATOR);

    try {
      final LogWriter sortedIndexWriter = new MmapLogWriter(new File(path, Filename.sortedWalFileName(shardId)));
      immutableIndex = new OffHeapImmutableIndex(actualRecordSize);
      if (distinctWal) {
        for (int i = 0; i < actualRecordSize; i++) {
          byte[] record = records[i];
          immutableIndex.add(record);
          sortedIndexWriter.append(record);
        }
      } else {
        byte[] pre = null;
        for (int i = 0; i < actualRecordSize; i++) {
          byte[] record = records[i];
          if (pre != null) {
            if (KeyComparator.COMPARATOR.compare(pre, record) != 0) {
              immutableIndex.add(pre);
              sortedIndexWriter.append(pre);
              pre = record;
            } else {
              if (VlogSeqComparator.COMPARATOR.compare(pre, record) < 0) {
                pre = record;
              }
            }
          } else {
            pre = record;
          }
        }
        if (pre != null) {
          immutableIndex.add(pre);
          sortedIndexWriter.append(pre);
        }
      }

      prepareReadIOAsyncExecutor.submit(() -> {
        try {
          sortedIndexWriter.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }

    vLogReader = new JNADirectIOSynchronizedVLogReader(path + File.separator + Filename.vlogFileName(shardId));
    try {
      vLogReader.init();
    } catch (FileNotFoundException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {
    try {
      if (orderedVLogOffset == null) {
        return;
      }

      if (CONCURRENT_RANGE_LOAD) {
        int totalSize = orderedVLogOffset.getIndex();
        int totalBatchSize = totalSize / BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE + 1;
        int lastBatchSize = totalSize % BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE;
        List<Future<byte[][][]>> futures = new ArrayList<>(RANGE_LOAD_FROM_MEM_CONCURRENCY);
        for (int batchOffset = 0; batchOffset < totalBatchSize; batchOffset++) {
          if (batchOffset > 0 && batchOffset % RANGE_LOAD_FROM_MEM_CONCURRENCY == 0) {
            for (Future<byte[][][]> future : futures) {
              while (!asyncVisitQueue.offer(future.get())) {

              }
            }
            futures.clear();
          }
          int startIndex = batchOffset * BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE;
          int endIndex = batchOffset == totalBatchSize - 1 ? startIndex + lastBatchSize : startIndex + BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE;
          futures.add(concurrentPutQueueExecutor.submit(new RangeLoadTask(startIndex, endIndex)));
        }

        for (Future<byte[][][]> future : futures) {
          while (!asyncVisitQueue.offer(future.get())) {

          }
        }
      } else {
        int limit = orderedVLogOffset.getIndex();
        byte[][][] batchList = new byte[BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE][][];
        int curr = 0;
        for (int i = 0; i < limit; ) {
          batchList[curr++] = new byte[][]{orderedVLogOffset.getKeys()[i], vLogCache.read(orderedVLogOffset.getVlogSequences()[i] * 4096)};
          if ((++i & BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE_MASK) == 0) {
            byte[][][] temp = batchList;
            while (!asyncVisitQueue.offer(temp)) {

            }
            int size = (limit - i) < BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE ? (limit - i) : BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE;
            batchList = new byte[size][][];
            curr = 0;
          }
        }

        while (!asyncVisitQueue.offer(batchList)) {

        }
      }

      cleanerExecutor.submit(() -> {
        vLogCache.freeUp();
        orderedVLogOffset.destroy();
        destroyWalLogIterator();
      });

    } catch (Exception e) {
      e.printStackTrace();
      Throwables.propagate(e);
    }
  }

  public void prefetch(ExecutorService prepareRangeLoadExecutor,
                       ExecutorService prefetchWalLoadExecutor,
                       ExecutorService cleanerExecutor,
                       ExecutorService concurrentPutQueueExecutor,
                       Queue<byte[][][]> asyncVisitQueue) {
    this.cleanerExecutor = cleanerExecutor;
    this.concurrentPutQueueExecutor = concurrentPutQueueExecutor;
    this.asyncVisitQueue = asyncVisitQueue;

    if (ANY_TMP_VLOG_EXISTS) {
      Recoverables.recoverVLog(path, shardId);
    }

    Future future = prefetchWalLoadExecutor.submit(() -> {
      File sortedWalFile = new File(path, Filename.sortedWalFileName(shardId));
      // 如果已去重、排序好的wal存在，则不用重新排序
      if (sortedWalFile.exists()) {
        try {
          walLogIterator = new MmapWalLogByteArrayIterator(sortedWalFile, false);
        } catch (FileNotFoundException e) {
          immutableIndex = NopImmutableIndex.instance();
          return;
        }
        orderedVLogOffset = new SortedKeyAndVLogSeqs(walLogIterator.getRecordSize());
        while (walLogIterator.hasNext()) {
          orderedVLogOffset.add(walLogIterator.next());
        }
      } else {
        try {
          walLogIterator = new MmapWalLogByteArrayIterator(new File(path, Filename.walFileName(shardId)));
        } catch (FileNotFoundException e) {
          immutableIndex = NopImmutableIndex.instance();
          return;
        }

        byte[][] records = new byte[walLogIterator.getRecordSize()][];
        int index = 0;
        while (walLogIterator.hasNext()) {
          records[index++] = walLogIterator.next();
        }

        int actualRecordSize = walLogIterator.getActualRecordSize();
        Arrays.sort(records, 0, actualRecordSize, KeyComparator.COMPARATOR);

        orderedVLogOffset = new SortedKeyAndVLogSeqs(actualRecordSize);
        byte[] pre = null;
        for (int i = 0; i < actualRecordSize; i++) {
          byte[] record = records[i];
          if (pre != null) {
            if (KeyComparator.COMPARATOR.compare(pre, record) != 0) {
              orderedVLogOffset.add(pre);
              pre = record;
            } else {
              if (VlogSeqComparator.COMPARATOR.compare(pre, record) < 0) {
                pre = record;
              }
            }
          } else {
            pre = record;
          }
        }
        if (pre != null) {
          orderedVLogOffset.add(pre);
        }
      }
    });

    try {
      if (vLogCache == null) {
        vLogCache = new ConcurrentJNADirectIOLoad2DirectOffheapUsingUnsafeVLogCache(new File(path, Filename.vlogFileName(shardId)), prepareRangeLoadExecutor, cleanerExecutor);
      }
      vLogCache.load();
    } catch (FileNotFoundException e) {
      return;
    } catch (IOException e) {
      e.printStackTrace();
      Throwables.propagate(e);
    }

    try {
      future.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }


  private void destroyWalLogIterator() {
    if (walLogIterator != null) {
      walLogIterator.freeUp();
      try {
        walLogIterator.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      walLogIterator = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (walLogWriter != null) {
      walLogWriter.close();
    }

    if (vLogWriter != null) {
      vLogWriter.close();
    }

    if (vLogReader != null) {
      vLogReader.close();
    }

    if (immutableIndex != null) {
      immutableIndex.destroy();
    }

    if (vLogCache != null) {
      vLogCache.close();
    }
  }

  public int getShardId() {
    return shardId;
  }

  class RangeLoadTask implements Callable<byte[][][]> {

    private int startIndex;

    private int endIndex;

    @Override
    public byte[][][] call() throws Exception {
      byte[][][] batchList = new byte[endIndex - startIndex][][];
      int curr = 0;
      for (int index = startIndex; index < endIndex; index++) {
        batchList[curr++] = new byte[][]{orderedVLogOffset.getKeys()[index], vLogCache.read(orderedVLogOffset.getVlogSequences()[index] * 4096)};
      }
      return batchList;
    }

    public RangeLoadTask(int startIndex, int endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }
  }
}
