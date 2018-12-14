package com.alibabacloud.polar_race.engine.common.neoremind.util;

import java.util.Random;

/**
 * 回退休眠，减少锁竞争
 */
public class Backoff {

  private final int minDelay, maxDelay;

  private int limit;

  private final Random random;

  /**
   * Prepare to pause for random duration.
   *
   * @param min smallest back-off
   * @param max largest back-off
   */
  public Backoff(int min, int max) {
    if (max < min) {
      throw new IllegalArgumentException("max must be greater than min");
    }
    minDelay = min;
    maxDelay = min;
    limit = minDelay;
    random = new Random();
  }

  /**
   * Backoff for random duration.
   */
  public void backoff() {
    int delay = random.nextInt(limit);
    if (limit < maxDelay) {
      limit = 2 * limit;
    }
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
  }
}
