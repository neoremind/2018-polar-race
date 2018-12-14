package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DirectByteBufferPool
 *
 * @author xu.zx
 */
public class DirectByteBufferPool {

  private static volatile DirectByteBufferPool INSTANCE = null;

  private static final Object LOCK = new Object();

  private Queue<ByteBuffer> queue;

  private volatile boolean init;

  public static DirectByteBufferPool getInstance() {
    if (INSTANCE == null) {
      synchronized (LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new DirectByteBufferPool();
        }
      }
    }
    return INSTANCE;
  }

  /**
   * 非线程安全
   */
  public void initIfPossible(int poolSize, int bufferSize) {
    if (init) {
      return;
    }
    queue = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < poolSize; i++) {
      queue.add(ByteBuffer.allocateDirect(bufferSize));
    }
    init = true;
  }

  public ByteBuffer take() {
    ByteBuffer buffer;
    while ((buffer = queue.poll()) == null) {

    }
    return buffer;
  }

  public void recycle(ByteBuffer buffer) {
    while (!queue.offer(buffer)) {

    }
  }

  public void destroy() {
    ByteBuffer buffer;
    if (queue != null) {
      while ((buffer = queue.poll()) != null) {
        ((DirectBuffer) buffer).cleaner().clean();
      }
    }
  }
}
