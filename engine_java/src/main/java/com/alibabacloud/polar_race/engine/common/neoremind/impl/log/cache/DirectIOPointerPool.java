package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * DirectByteBufferPool
 *
 * @author xu.zx
 */
public class DirectIOPointerPool {

  private static volatile DirectIOPointerPool INSTANCE = null;

  private static final Object LOCK = new Object();

  private native int posix_memalign(PointerByReference memptr, int alignment, int size);

  private static native int getpagesize();

  private Queue<Pointer> queue;

  private volatile boolean init;

  static {
    Native.register("c");
  }

  public static DirectIOPointerPool getInstance() {
    if (INSTANCE == null) {
      synchronized (LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new DirectIOPointerPool();
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
      PointerByReference pntByRef = new PointerByReference();
      posix_memalign(pntByRef, getpagesize(), bufferSize);
      queue.add(pntByRef.getValue());
    }
    init = true;
  }

  public Pointer take() {
    Pointer pointer;
    while ((pointer = queue.poll()) == null) {

    }
    return pointer;
  }

  public void recycle(Pointer pointer) {
    while (!queue.offer(pointer)) {

    }
  }

  public void destroy() {

  }
}
