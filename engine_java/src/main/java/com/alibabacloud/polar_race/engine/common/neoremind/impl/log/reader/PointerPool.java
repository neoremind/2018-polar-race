package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.reader;

import com.sun.jna.Pointer;

import org.jctools.queues.MpmcArrayQueue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * direct io native api使用的{@link Pointer}的pool
 *
 * @author xu.zx
 */
public class PointerPool {

  private Queue<Pointer> queue;

  public PointerPool(int queueSize) {
    this.queue = new ConcurrentLinkedQueue<>();
  }

  public void add(Pointer pointer) {
    queue.add(pointer);
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
}
