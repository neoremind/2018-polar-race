package com.alibabacloud.polar_race.engine.common.neoremind.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibabacloud.polar_race.engine.common.neoremind.DBConstants;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.DBImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于滑动窗口的执行器，用于range操作
 *
 * @author xu.zx
 */
public class SlidingWindowExecutor<T> {

  /**
   * sliding window做range最大的执行超时时间
   */
  public static int SLIDING_WINDOW_MAX_RUN_TIME_IN_SECS = 250;

  private BlockingQueue<T> targetQueue;

  private final ReentrantLock lock = new ReentrantLock();

  private final Condition done = lock.newCondition();

  private volatile T[] targets;

  private T deadLetter;

  public SlidingWindowExecutor(T[] dbs, T deadLetter) {
    this.targets = dbs;
    this.deadLetter = deadLetter;
    targetQueue = new LinkedBlockingQueue<>(1);
  }

  public void startAndWaitFinish(Handler<T> handler) {
    new Thread(() -> {
      int current = 0;
      try {
        while (true) {
          if (current >= targets.length) {
            targetQueue.put(deadLetter);
            break;
          }
          // if sliding window size = 3, uncomment
//          while (!targetQueue.isEmpty()) {
//            LockSupport.parkNanos(1000000);
//          }
          handler.prefetch(targets, current);
          targetQueue.put(targets[current]);
          current++;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.interrupted();
      } catch (Exception e) {
        e.printStackTrace();
        Throwables.propagate(e);
      }
    }).start();

    new Thread(() -> {
      try {
        while (true) {
          T t = targetQueue.take();
          if (t == deadLetter) {
            lock.lock();
            try {
              done.signalAll();
            } finally {
              lock.unlock();
            }
            break;
          } else {
            handler.handle(t);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.interrupted();
      } catch (Exception e) {
        e.printStackTrace();
        Throwables.propagate(e);
      }
    }).start();

    waitFinish();
  }

  private void waitFinish() {
    lock.lock();
    try {
      done.await(SLIDING_WINDOW_MAX_RUN_TIME_IN_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  public void stop() {
  }

  public interface Handler<T> {

    void prefetch(T[] targets, int currentTargets) throws InterruptedException;

    void handle(T target);
  }

  public static void main(String[] args) throws IOException {
    Logger logger = LoggerFactory.getLogger(SlidingWindowExecutor.class);
    DBImpl[] dbs = new DBImpl[128];
    for (int i = 0; i < 128; i++) {
      dbs[i] = new DBImpl("/abc", i);
    }
    SlidingWindowExecutor<DBImpl> roundRobinExecutor = new SlidingWindowExecutor<>(dbs, new DBImpl("", -1));
    Handler<DBImpl> handler = new Handler<DBImpl>() {
      @Override
      public void prefetch(DBImpl[] targets, int currentTargets) throws InterruptedException {
        DBImpl db = targets[currentTargets];
        logger.info("prefetch " + db.getShardId());
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info("prefetch " + db.getShardId() + " done");
      }

      @Override
      public void handle(DBImpl db) {
        logger.info("run handle " + db.getShardId());
        //db.range(...);
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info("run handle " + db.getShardId() + " done");
      }
    };
    long start = System.currentTimeMillis();
    logger.info("start");
    roundRobinExecutor.startAndWaitFinish(handler);
    System.out.println("use " + (System.currentTimeMillis() - start));
    roundRobinExecutor.stop();
  }
}
