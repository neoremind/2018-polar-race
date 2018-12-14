package com.alibabacloud.polar_race.engine.common.neoremind.util;

/**
 * AccumulativeRunner的单例构造。
 *
 * @author xu.zx
 */
public class AccumulativeRunnerSingleton {

  private static volatile AccumulativeRunner INSTANCE = null;

  private static final Object LOCK = new Object();

  public static AccumulativeRunner getInstance(int accumulateSize, int maxWaitTimeInMs, int schedulerDelayInMs, AccumulativeRunner.Handler handler) {
    if (INSTANCE == null) {
      synchronized (LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new AccumulativeRunner(accumulateSize, maxWaitTimeInMs, schedulerDelayInMs, handler);
        }
      }
    }
    return INSTANCE;
  }

  private AccumulativeRunnerSingleton() {
  }

  public static void clear() {
    INSTANCE = null;
  }
}
