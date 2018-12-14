package com.alibabacloud.polar_race.engine.common.neoremind.util;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 异步执行器，一般用于并行的打开或者关闭数据库，因为有多个分片，所以加大并行度。
 *
 * @author xu.zx
 */
public class AsyncExecutor<T extends TaskStatus> {

  private ExecutorService executorService;

  private List<Future<T>> futures;

  public AsyncExecutor(int concurrency) {
    executorService = Executors.newFixedThreadPool(concurrency);
    futures = Lists.newArrayListWithCapacity(concurrency);
  }

  public void submitTask(Callable<T> callable) {
    futures.add(executorService.submit(callable));
  }

  public void awaitTermination() {
    awaitTermination(null);
  }

  public void awaitTermination(Handler<T> handler) {
    try {
      for (Future<T> future : futures) {
        T result = future.get();
        //if (!result.success()) {
        //  throw new EngineException(RetCodeEnum.IO_ERROR, "failed to execute async task!");
        //}
      }
    } catch (Exception e) {
      e.printStackTrace();
      Throwables.propagate(e);
    } finally {
      futures.clear();
    }
  }

  public void close() {
    executorService.shutdownNow();
  }

  public interface Handler<T> {
    void handle(List<T> resultList);
  }

}
