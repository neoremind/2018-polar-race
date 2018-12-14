package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.neoremind.DBConstants;
import com.alibabacloud.polar_race.engine.common.neoremind.util.AccumulativeRunner;
import com.alibabacloud.polar_race.engine.common.neoremind.util.SlidingWindowExecutor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 严格保序range操作的线程聚集处理
 *
 * @author xu.zx
 */
@Deprecated
public class ShardDBOrderedRangeTaskHandler implements AccumulativeRunner.Handler<ShardDBRangeTask>, DBConstants {

  private DBImpl[] dbs;

  public static AtomicLong ASYNC_VISIT = new AtomicLong(0);

  public ShardDBOrderedRangeTaskHandler(DBImpl[] dbs) {
    this.dbs = dbs;
  }

  @Override
  public void handle(List<ShardDBRangeTask> taskList) {
    if (taskList == null || taskList.isEmpty()) {
      return;
    }

    long start = System.currentTimeMillis();
    byte[] lower = taskList.get(0).getLowerKey();
    byte[] upper = taskList.get(0).getUpperKey();
    AbstractVisitor[] visitors = new AbstractVisitor[taskList.size()];
    for (int i = 0; i < taskList.size(); i++) {
      visitors[i] = taskList.get(i).getVisitor();
    }

    int taskSize = taskList.size();
    System.out.println(String.format("1 iteration for %d tasks, will iterate %d dbs", taskList.size(), dbs.length));

    AbstractVisitor decoratedVisitor = new AbstractVisitor() {
      @Override
      public void visit(byte[] key, byte[] value) {
        for (int i = 0; i < taskSize; i++) {
          visitors[i].visit(key, value);
        }
      }
    };

    ExecutorService prefetchExecutor = Executors.newFixedThreadPool(PREPARE_RANGE_LOAD_THREAD_POOL_SIZE,
        new ThreadFactoryBuilder().setNameFormat("prefetch-%d").build());

    ExecutorService cleanerExecutor = Executors.newFixedThreadPool(CLEANER_THREAD_POOL_SIZE,
        new ThreadFactoryBuilder().setNameFormat("cleaner-%d").build());

    BlockingQueue<byte[][][]> asyncVisitQueue = new LinkedBlockingQueue<>(ASYNC_VISIT_QUEUE_SIZE);
//    BlockingQueue<List<byte[][]>> asyncVisitQueue = new DisruptorBlockingQueue<>(ASYNC_VISIT_QUEUE_SIZE);
//    BlockingQueue<List<byte[][]>> asyncVisitQueue = new SingleConsumerDisruptorQueue<>(ASYNC_VISIT_QUEUE_SIZE);

    ExecutorService visitorExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("async-visit-%d").build());
    Future future = visitorExecutor.submit(() -> {
      while (true) {
        try {
          long s1 = System.nanoTime();
          byte[][][] kvs = asyncVisitQueue.take();
          for (byte[][] kv : kvs) {
            if (kv == null) {
              break;
            }
            decoratedVisitor.visit(kv[0], kv[1]);
          }
          ASYNC_VISIT.addAndGet(System.nanoTime() - s1);
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        } catch (Exception e) {
          e.printStackTrace();
          Throwables.propagate(e);
        }
      }
    });

    SlidingWindowExecutor<DBImpl> slidingWindowExecutor = new SlidingWindowExecutor<>(dbs, new DBImpl("", -1));
    SlidingWindowExecutor.Handler<DBImpl> handler = new SlidingWindowExecutor.Handler<DBImpl>() {
      @Override
      public void prefetch(DBImpl[] targets, int currentTargets) {
        DBImpl db = targets[currentTargets];
        // TODO
        db.prefetch(prefetchExecutor, null, cleanerExecutor, null, asyncVisitQueue);
      }

      @Override
      public void handle(DBImpl db) {
        db.range(lower, upper, decoratedVisitor);
      }
    };
    slidingWindowExecutor.startAndWaitFinish(handler);

    prefetchExecutor.shutdown();
    cleanerExecutor.shutdown();
    slidingWindowExecutor.stop();

    // 等等queue完成任务，给一段时间执行，然后暴力的cancel掉线程。
    while (!asyncVisitQueue.isEmpty()) {
      LockSupport.parkNanos(PARK_NANOS_WHILE_QUEUE_NOT_EMPTY);
    }
    LockSupport.parkNanos(PARK_NANOS_AFTER_QUEUE_EMPTY);
    future.cancel(true);
    visitorExecutor.shutdownNow();

    System.out.println("1 iteration for " + taskList.size() + " task using " + (System.currentTimeMillis() - start) + "ms");


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
//    System.out.println(String.format("ASYNC_VISIT_1=             %15d", ShardDBOrderedRangeTaskHandler.ASYNC_VISIT.get()));
//    System.out.println(String.format("ASYNC_VISIT_2=             %15d", ConcurrentVisitShardDBOrderedRangeTaskHandler.ASYNC_VISIT_ALL.get()));

  }

  @Override
  public void destroy() {

  }
}
