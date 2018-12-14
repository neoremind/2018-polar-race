package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.neoremind.DBConstants;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache.DirectByteBufferPool;
import com.alibabacloud.polar_race.engine.common.neoremind.util.AccumulativeRunner;
import com.alibabacloud.polar_race.engine.common.neoremind.util.SlidingWindowExecutor;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 严格保序range操作的线程聚集处理
 *
 * @author xu.zx
 */
public class ConcurrentVisitShardDBOrderedRangeTaskHandler implements AccumulativeRunner.Handler<ShardDBRangeTask>, DBConstants {

  private DBImpl[] dbs;

  private static AtomicBoolean POOL_INIT = new AtomicBoolean(false);

  private ExecutorService concurrentVisitExecutor;

  private ExecutorService concurrentPutQueueExecutor;

  private ExecutorService prefetchWalLoadExecutor;

  private ExecutorService prefetchExecutor;

  private ExecutorService cleanerExecutor;

  private static int[][] START_END_PARTITION_INDEX;

  private ExecutorService visitorExecutor;

  private Future future;

  private Queue<byte[][][]> asyncVisitQueue;

  public ConcurrentVisitShardDBOrderedRangeTaskHandler(DBImpl[] dbs) {
    this.dbs = dbs;
  }

  @Override
  public void handle(List<ShardDBRangeTask> taskList) {
    Preconditions.checkArgument(ACCUMULATIVE_RUNNER_ACC_SIZE % VISIT_CONCURRENCY == 0, "concurrency should be power of 2");

    if (taskList == null || taskList.isEmpty()) {
      return;
    }

    //long start = System.currentTimeMillis();
    byte[] lower = taskList.get(0).getLowerKey();
    byte[] upper = taskList.get(0).getUpperKey();
    AbstractVisitor[] visitors = new AbstractVisitor[taskList.size()];
    for (int i = 0; i < taskList.size(); i++) {
      visitors[i] = taskList.get(i).getVisitor();
    }

    //System.out.println(String.format("1 iteration for %d tasks, will iterate %d dbs", taskList.size(), dbs.length));

    if (POOL_INIT.compareAndSet(false, true)) {
      concurrentVisitExecutor = Executors.newFixedThreadPool(VISIT_CONCURRENCY,
          new ThreadFactoryBuilder().setNameFormat("conc-visit-%d").setDaemon(true).build());

      prefetchExecutor = new ThreadPoolExecutor(PREPARE_RANGE_LOAD_THREAD_POOL_SIZE, PREPARE_RANGE_LOAD_THREAD_POOL_SIZE,
          0L, TimeUnit.MILLISECONDS,
          new LinkedTransferQueue<>(),
          new ThreadFactoryBuilder().setNameFormat("p-%d").setDaemon(true).build());

      concurrentPutQueueExecutor = new ThreadPoolExecutor(RANGE_LOAD_FROM_MEM_CONCURRENCY, RANGE_LOAD_FROM_MEM_CONCURRENCY,
          0L, TimeUnit.MILLISECONDS,
          new LinkedTransferQueue<>(),
          new ThreadFactoryBuilder().setNameFormat("cp-%d").setDaemon(true).build());

      cleanerExecutor = Executors.newFixedThreadPool(CLEANER_THREAD_POOL_SIZE,
          new ThreadFactoryBuilder().setNameFormat("cleaner-%d").setDaemon(true).build());

      prefetchWalLoadExecutor = Executors.newFixedThreadPool(PREFETCH_WAL_LOAD_THREAD_POOL_SIZE,
          new ThreadFactoryBuilder().setNameFormat("p-load-%d").setDaemon(true).build());

      START_END_PARTITION_INDEX = new int[VISIT_CONCURRENCY][];
      int step = ACCUMULATIVE_RUNNER_ACC_SIZE / VISIT_CONCURRENCY;
      for (int i = 0; i < VISIT_CONCURRENCY; i++) {
        START_END_PARTITION_INDEX[i] = new int[]{i * step, step};
      }


//    Queue<byte[][][]> asyncVisitQueue = new SpscLinkedQueue<>();
      asyncVisitQueue = new ConcurrentLinkedQueue<>();
//    BlockingQueue<byte[][][]> asyncVisitQueue = new DisruptorBlockingQueue<>(ASYNC_VISIT_QUEUE_SIZE);
//    BlockingQueue<byte[][][]> asyncVisitQueue = new SingleConsumerDisruptorQueue<>(ASYNC_VISIT_QUEUE_SIZE);

      visitorExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("async-visit-%d").setDaemon(true).build());
      future = visitorExecutor.submit(() -> {
        try {
          while (true) {
            byte[][][] kvs = asyncVisitQueue.poll();
            if (kvs == null) continue;
            //ASYNC_VISIT_TAKE.addAndGet(System.nanoTime() - s1);
            Future[] futures = new Future[START_END_PARTITION_INDEX.length];
            for (int i = 0; i < START_END_PARTITION_INDEX.length; i++) {
              int[] startEnd = START_END_PARTITION_INDEX[i];
              futures[i] = concurrentVisitExecutor.submit(() -> {
                for (int j = startEnd[0]; j < startEnd[0] + startEnd[1]; j++) {
                  for (byte[][] kv : kvs) {
                    if (kv == null) {
                      break;
                    }
                    visitors[j].visit(kv[0], kv[1]);
                  }
                }
              });
            }

            for (Future f : futures) {
              f.get();
            }
          }
          //ASYNC_VISIT_ALL.addAndGet(System.nanoTime() - s1);
        } catch (InterruptedException e) {
          //e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
          Throwables.propagate(e);
        }
      });
    }


    SlidingWindowExecutor<DBImpl> slidingWindowExecutor = new SlidingWindowExecutor<>(dbs, new DBImpl("", -1));
    SlidingWindowExecutor.Handler<DBImpl> handler = new SlidingWindowExecutor.Handler<DBImpl>() {
      @Override
      public void prefetch(DBImpl[] targets, int currentTargets) {
        targets[currentTargets].prefetch(prefetchExecutor, prefetchWalLoadExecutor, cleanerExecutor, concurrentPutQueueExecutor, asyncVisitQueue);
      }

      @Override
      public void handle(DBImpl db) {
        db.range(lower, upper, visitors[0]);
      }
    };
    slidingWindowExecutor.startAndWaitFinish(handler);
    slidingWindowExecutor.stop();

//    System.out.println("1 iteration for " + taskList.size() + " task using " + (System.currentTimeMillis() - start) + "ms");
//
//    System.out.println(String.format("READ_VALUE=                %15d", DBImpl.READ_VALUE.get()));
//    System.out.println(String.format("VISIT=                     %15d", DBImpl.VISIT.get()));
//    System.out.println(String.format("VISIT_LAST=                %15d", DBImpl.VISIT_LAST.get()));
//    System.out.println(String.format("HVLOG_CLOSE=               %15d", DBImpl.HVLOG_CLOSE.get()));
//    System.out.println(String.format("RANGE=                     %15d", DBImpl.RANGE.get()));
//    System.out.println("----------------------------------------------");
//    System.out.println(String.format("NEW_WAL_ITER=              %15d", DBImpl.NEW_WAL_ITER.get()));
//    System.out.println(String.format("WAL_ITER=                  %15d", DBImpl.WAL_ITER.get()));
//    System.out.println(String.format("WAL_ITER_FREE_UP=          %15d", DBImpl.WAL_ITER_FREE_UP.get()));
//    System.out.println(String.format("SORT=                      %15d", DBImpl.SORT.get()));
//    System.out.println(String.format("BUILD_ORDERED_VLOG_OFFSET= %15d", DBImpl.BUILD_ORDERED_VLOG_OFFSET.get()));
//    System.out.println(String.format("NEW_HEAP_VLOG=             %15d", DBImpl.NEW_HEAP_VLOG.get()));
//    System.out.println(String.format("HEAP_VLOG_LOAD=            %15d", DBImpl.HEAP_VLOG_LOAD.get()));
//    System.out.println(String.format("HEAP_VLOG_FREEUP=          %15d", DBImpl.HEAP_VLOG_FREEUP.get()));
//    System.out.println(String.format("HEAP_VLOG_NEW_AND_LOAD=    %15d", DBImpl.HEAP_VLOG_NEW_AND_LOAD.get()));
//    System.out.println(String.format("PREPARE_RANGE=             %15d", DBImpl.PREPARE_RANGE.get()));
//    System.out.println("----------------------------------------------");
//    System.out.println(String.format("ASYNC_VISIT=               %15d", ConcurrentVisitShardDBOrderedRangeTaskHandler.ASYNC_VISIT_ALL.get()));
//    System.out.println(String.format("ASYNC_VISIT_take=          %15d", ConcurrentVisitShardDBOrderedRangeTaskHandler.ASYNC_VISIT_TAKE.get()));
  }

  @Override
  public void destroy() {
    prefetchExecutor.shutdown();
    prefetchWalLoadExecutor.shutdown();
    concurrentPutQueueExecutor.shutdown();
    cleanerExecutor.shutdown();
    concurrentVisitExecutor.shutdownNow();
    //DirectByteBufferPool.getInstance().destroy();

    // 等等queue完成任务，给一段时间执行，然后暴力的cancel掉线程。
    while (!asyncVisitQueue.isEmpty()) {
      LockSupport.parkNanos(PARK_NANOS_WHILE_QUEUE_NOT_EMPTY);
    }
    LockSupport.parkNanos(PARK_NANOS_AFTER_QUEUE_EMPTY);
    future.cancel(true);
    visitorExecutor.shutdownNow();
  }

}
