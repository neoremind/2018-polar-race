package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * AbstractVLogCache
 *
 * @author xu.zx
 */
public abstract class AbstractVLogCache implements VLogCache {

  protected ExecutorService prepareLoadExecutor;

  protected ExecutorService cleanerExecutor;

  protected void waitFutures(int totalRound, List<Future> futureList) {
    try {
      for (Future future : futureList) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void freeUp() {

  }
}
