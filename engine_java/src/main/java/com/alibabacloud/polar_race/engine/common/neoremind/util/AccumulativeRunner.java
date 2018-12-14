package com.alibabacloud.polar_race.engine.common.neoremind.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;

/**
 * 单线程累计一定请求，统一查询，用于range操作。
 *
 * @author xu.zx
 */
public class AccumulativeRunner {

  public static final long DEFAULT_AWAIT_TIMEOUT_IN_SECONDS = 180;

  private final ReentrantLock lock;

  private final Condition notEnoughTask;

  private final Condition done;

  private BlockingQueue<Task> queue;

  private int accumulateSize;

  private volatile boolean isStop;

  private volatile boolean timeoutToRun;

  private long maxWaitTimeInMs = 5000;

  private ScheduledExecutorService scheduler;

  private int schedulerDelayInMs = 5000;

  private final Handler handler;

  AccumulativeRunner(int accumulateSize, int maxWaitTimeInMs, int schedulerDelayInMs, Handler handler) {
    lock = new ReentrantLock();
    notEnoughTask = lock.newCondition();
    done = lock.newCondition();
    this.accumulateSize = accumulateSize;
    this.maxWaitTimeInMs = maxWaitTimeInMs;
    this.schedulerDelayInMs = schedulerDelayInMs;
    this.handler = handler;
    queue = new LinkedBlockingQueue<>(accumulateSize);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("acc-runner-%s")
        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            System.err.printf("%s%n", t);
            e.printStackTrace();
          }
        })
        .build();
    scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setNameFormat("acc-checker-%d")
            .setDaemon(true)
            .build());

    new Thread(() -> {
      while (!isStop) {
        lock.lock();
        try {
          while (!isStop && queue.size() < accumulateSize && !timeoutToRun) {
            //System.out.println("[runner] accumulative runner waits to run...");
            notEnoughTask.await();
          }
          if (isStop) {
            done.signalAll();
            continue;
          }
          //System.out.println(String.format("[runner] queue size is %d(>=%d) or may be timeoutToRun, and tasks are ready to run", queue.size(), accumulateSize));
          List<Task> tasks = new ArrayList<>(queue.size());
          while (!queue.isEmpty()) {
            tasks.add(queue.poll());
          }
          handler.handle(tasks);
          done.signalAll();
        } catch (InterruptedException e) {
          System.out.println("[runner] quit waiting new task");
          break;
        } catch (Throwable e) {
          System.err.println("[runner] Failed to execute due to " + e.getMessage());
          e.printStackTrace();
        } finally {
          timeoutToRun = false;
          done.signalAll();
          lock.unlock();
        }
      }
      //System.out.println("[runner] quit runner");
    }).start();

    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        lock.lock();
        try {
          if (!queue.isEmpty()) {
            long longestWaitTimeInMs = System.currentTimeMillis() - queue.peek().getSubmittedTimestamp();
            if (longestWaitTimeInMs > maxWaitTimeInMs) {
              System.out.println("[checker] longestWaitTimeInMs=" + longestWaitTimeInMs + ", trigger run since reach max wait time " + maxWaitTimeInMs);
              timeoutToRun = true;
              notEnoughTask.signal();
            }
          }
        } finally {
          lock.unlock();
        }
      }
    }, schedulerDelayInMs, schedulerDelayInMs, TimeUnit.MILLISECONDS);

    //System.out.println(String.format("create AccumulativeRunner, accumulateSize=%d, maxWaitTimeInMs=%d, schedulerDelayInMs=%d," +
    //    " 2 threads run and timeout checker", accumulateSize, maxWaitTimeInMs, schedulerDelayInMs));
  }

  public void submitTaskAndWait(Task task) {
    checkState(!isStop, "runner is stopped");
    task.setSubmittedTimestamp(System.currentTimeMillis());
    lock.lock();
    try {
      queue.put(task);
      // OPEN System.out.println(Thread.currentThread().getName() + " got task " + task + ", pending tasks = " + queue.size());
      if (queue.size() >= accumulateSize) {
        notEnoughTask.signal();
      }
      done.await(DEFAULT_AWAIT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * //TODO 和awaitFinish分开调用，会有锁问题。
   */
  public void submitTask(Task task) {
    checkState(!isStop, "runner is stopped");
    task.setSubmittedTimestamp(System.currentTimeMillis());
    lock.lock();
    try {
      queue.put(task);
      // OPEN System.out.println(Thread.currentThread().getName() + " got task " + task + ", pending tasks = " + queue.size());
      if (queue.size() >= accumulateSize) {
        notEnoughTask.signal();
      }
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  public void awaitFinish(long timeoutInSecs) {
    checkState(!isStop, "runner is stopped");
    lock.lock();
    try {
      long start = System.currentTimeMillis();
      //System.out.println(Thread.currentThread().getName() + " waits runner to finish");
      done.await(timeoutInSecs, TimeUnit.SECONDS);
      System.out.println(Thread.currentThread().getName() + " runner done using " + (System.currentTimeMillis() - start) + "ms");
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  public void awaitFinish() {
    awaitFinish(DEFAULT_AWAIT_TIMEOUT_IN_SECONDS);
  }

  public void stop() {
    lock.lock();
    try {
      isStop = true;
      notEnoughTask.signal();
    } finally {
      lock.unlock();
    }
    scheduler.shutdownNow();
    handler.destroy();
  }

  public Handler getHandler() {
    return handler;
  }

  public abstract static class Task {

    private long submittedTimestamp;

    public Task() {
    }

    public void setSubmittedTimestamp(long submittedTimestamp) {
      this.submittedTimestamp = submittedTimestamp;
    }

    public long getSubmittedTimestamp() {
      return submittedTimestamp;
    }
  }

  public static class MyHandler implements Handler<AccumulativeRunner.MyTask> {

    private Backoff backoff;

    public MyHandler() {
      this(0);
    }

    public MyHandler(long sleepTimeInMs) {
      this.backoff = new Backoff((int) sleepTimeInMs, (int) sleepTimeInMs + 5000);
    }

    @Override
    public void handle(List<MyTask> taskList) {
      System.out.println("[default handler] start running");

      backoff.backoff();

      for (AccumulativeRunner.MyTask task1 : taskList) {
        System.out.println(task1.getName() + "======");
      }

      System.out.println("[default handler] done");
    }

    @Override
    public void destroy() {

    }
  }

  public interface Handler<T extends Task> {
    void handle(List<T> taskList);

    void destroy();
  }

  public static void main(String[] args) throws IOException {
    int concurrency = 5;
    final AccumulativeRunner runner = new AccumulativeRunner(concurrency, 5000, 5000, new MyHandler(5000));
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    CompletionService<Void> completionService = new ExecutorCompletionService<>(pool);

    AtomicInteger i = new AtomicInteger(0);
    AtomicInteger xxx = new AtomicInteger(0);

    Backoff backoff = new Backoff(500, 2000);
    for (int j = 0; j < 5; j++) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          runner.submitTask(new MyTask(String.valueOf(i.incrementAndGet())));
          runner.awaitFinish();
          xxx.incrementAndGet();
          Thread.sleep(3000);
          runner.submitTask(new MyTask(String.valueOf(i.incrementAndGet())));
          backoff.backoff();
          runner.awaitFinish();
          xxx.incrementAndGet();
          return null;
        }
      });
    }

    System.out.println("parelle running...");
//    System.in.read();

    while (xxx.get() != 10) {
      System.out.println("checking...");
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    runner.stop();
    System.out.println("finish");
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("123");
    pool.shutdown();
  }

  static class MyTask extends Task {
    private String name;

    public MyTask(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

}
