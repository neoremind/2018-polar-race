package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.neoremind.DB;
import com.alibabacloud.polar_race.engine.common.neoremind.DBConstants;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.reader.JNADirectIOSynchronizedVLogReader;
import com.alibabacloud.polar_race.engine.common.neoremind.partitioner.Partitioner;
import com.alibabacloud.polar_race.engine.common.neoremind.partitioner.impl.RangePartitioner;
import com.alibabacloud.polar_race.engine.common.neoremind.util.AccumulativeRunner;
import com.alibabacloud.polar_race.engine.common.neoremind.util.AccumulativeRunnerSingleton;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Filename;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 带sharding的db。
 *
 * @author xu.zx
 */
public class ShardDBImpl implements DB, DBConstants {

  private static AtomicBoolean READ_PREPARED = new AtomicBoolean(false);

  private final DBImpl[] shardDB;

  private Partitioner partitioner;

  private ExecutorService prepareReadIOAsyncExecutor;

  private AccumulativeRunner runner;

  private ConcurrentVisitShardDBOrderedRangeTaskHandler shardDBOrderedRangeTaskHandler;

  public ShardDBImpl(String path, int shardNumber) throws IOException {
    partitioner = new RangePartitioner(shardNumber);
    shardDB = new DBImpl[shardNumber];
    for (int i = 0; i < shardNumber; i++) {
      shardDB[i] = new DBImpl(path, i);
    }
    File parent = new File(path);
    if (!parent.exists()) {
      parent.mkdir();
    } else {
      DBImpl.ANY_TMP_VLOG_EXISTS = anyTmpVLogExists(path);
    }
  }

  @Override
  public void write(byte[] key, byte[] value) {
    shardDB[partitioner.partition(key)].write(key, value);
  }

  @Override
  public byte[] read(byte[] key) {
    if (!READ_PREPARED.get()) {
      prepareRead();
    }
    return shardDB[partitioner.partition(key)].read(key);
  }

  private synchronized void prepareRead() {
    if (READ_PREPARED.get()) {
      return;
    }
    //JNADirectIOLockFreeVLogReader.globalInit();
    JNADirectIOSynchronizedVLogReader.globalInit();

    prepareReadIOAsyncExecutor = Executors.newFixedThreadPool(10);

    int threadNumber = 32;
    int taskPerThread = (1024 / threadNumber);
    Thread[] threads = new Thread[threadNumber];
    for (int i = 0; i < threadNumber; i++) {
      final int start = i * taskPerThread;
      threads[i] = new Thread(() -> {
        for (int j = 0; j < taskPerThread; j++) {
          shardDB[start + j].prepareRead(prepareReadIOAsyncExecutor, false);
        }
      });
    }
    for (int i = 0; i < threadNumber; i++) {
      threads[i].start();
    }
    for (int i = 0; i < threadNumber; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    READ_PREPARED.set(true);
  }

  @Override
  public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {
    shardDBOrderedRangeTaskHandler = new ConcurrentVisitShardDBOrderedRangeTaskHandler(shardDB);
    runner = AccumulativeRunnerSingleton.getInstance(
        ACCUMULATIVE_RUNNER_ACC_SIZE, ACCUMULATIVE_RUNNER_MAX_WAIT_TIME_IN_MS,
        ACCUMULATIVE_RUNNER_SCHEDULER_DELAY_IN_MS, shardDBOrderedRangeTaskHandler);
    runner.submitTaskAndWait(new ShardDBRangeTask(lower, upper, visitor));
  }

  @Override
  public void close() throws IOException {
    if (shardDB == null || shardDB.length == 0) {
      return;
    }

    int threadNumber = 64;
    int taskPerThread = (1024 / threadNumber);
    Thread[] threads = new Thread[threadNumber];
    for (int i = 0; i < threadNumber; i++) {
      final int start = i * taskPerThread;
      threads[i] = new Thread(() -> {
        for (int j = 0; j < taskPerThread; j++) {
          try {
            shardDB[start + j].close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
    for (int i = 0; i < threadNumber; i++) {
      threads[i].start();
    }

    if (prepareReadIOAsyncExecutor != null) {
      prepareReadIOAsyncExecutor.shutdownNow();
    }

    if (runner != null) {
      runner.stop();
      AccumulativeRunnerSingleton.clear();
    }

    for (int i = 0; i < threadNumber; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private boolean anyTmpVLogExists(String path) {
    String[] fileNames = new File(path).list();
    for (String fileName : fileNames) {
      if (fileName.endsWith(Filename.VLOG_FILE_SUFFIX)) {
        return true;
      }
    }
    return false;
  }

  private boolean distinctWal() {
    return new File("d").exists();
  }

  private void makeDistinct() {
    try {
      new File("d").createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
