package com.alibabacloud.polar_race.engine.common.neoremind.util;


import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

/**
 * This is a blocking queue implementation based on Disruptor for single consumer thread
 */
@Deprecated
public class SingleConsumerDisruptorQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {
  /**
   * An event holder.
   */
  private static class Event<T> {
    private T item;

    public T getValue() {
      T t = item;
      item = null;
      return t;
    }

    public T readValue() {
      return item;
    }

    public void setValue(T event) {
      this.item = event;
    }
  }

  /**
   * Event factory to create event holder instance.
   */
  private static class Factory<T> implements EventFactory<Event<T>> {

    @Override
    public Event<T> newInstance() {
      return new Event<T>();
    }

  }

  private final RingBuffer<Event<T>> ringBuffer;
  private final Sequence consumedSeq;
  private final SequenceBarrier barrier;
  private long knownPublishedSeq;

  /**
   * Construct a blocking queue based on disruptor and allows multiple
   * producers.
   */
  public SingleConsumerDisruptorQueue(int bufferSize) {
    this(bufferSize, false);
  }

  /**
   * Construct a blocking queue based on disruptor.
   *
   * @param bufferSize     ring buffer size
   * @param singleProducer whether only single thread produce events.
   */
  public SingleConsumerDisruptorQueue(int bufferSize, boolean singleProducer) {
    if (singleProducer) {
      ringBuffer = RingBuffer.createSingleProducer(new Factory<T>(), normalizeBufferSize(bufferSize));
    } else {
      ringBuffer = RingBuffer.createMultiProducer(new Factory<T>(), normalizeBufferSize(bufferSize));
    }

    consumedSeq = new Sequence();
    ringBuffer.addGatingSequences(consumedSeq);
    barrier = ringBuffer.newBarrier();

    long cursor = ringBuffer.getCursor();
    consumedSeq.set(cursor);
    knownPublishedSeq = cursor;
  }

  @Override
  public int drainTo(Collection<? super T> collection) {
    return drainTo(collection, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super T> collection, int maxElements) {
    long pos = consumedSeq.get() + 1;
    if (pos + maxElements - 1 > knownPublishedSeq) {
      updatePublishedSequence();
    }
    int c = 0;
    try {
      while (pos <= knownPublishedSeq && c <= maxElements) {
        Event<T> eventHolder = ringBuffer.get(pos);
        collection.add(eventHolder.getValue());
        c++;
        pos++;
      }
    } finally {
      if (c > 0) {
        consumedSeq.addAndGet(c);
      }
    }
    return c;
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException();
  }

  private int normalizeBufferSize(int bufferSize) {
    if (bufferSize <= 0) {
      return 8192;
    }
    int ringBufferSize = 2;
    while (ringBufferSize < bufferSize) {
      ringBufferSize *= 2;
    }
    return ringBufferSize;
  }

  @Override
  public boolean offer(T e) {
    long seq;
    try {
      seq = ringBuffer.tryNext();
    } catch (InsufficientCapacityException e1) {
      return false;
    }
    publish(e, seq);
    return true;
  }

  @Override
  public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public T peek() {
    long l = consumedSeq.get() + 1;
    if (l > knownPublishedSeq) {
      updatePublishedSequence();
    }
    if (l <= knownPublishedSeq) {
      Event<T> eventHolder = ringBuffer.get(l);
      return eventHolder.readValue();
    }
    return null;
  }

  @Override
  public T poll() {
    long l = consumedSeq.get() + 1;
    if (l > knownPublishedSeq) {
      updatePublishedSequence();
    }
    if (l <= knownPublishedSeq) {
      Event<T> eventHolder = ringBuffer.get(l);
      T t = eventHolder.getValue();
      consumedSeq.incrementAndGet();
      return t;
    }
    return null;
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  private void publish(T e, long seq) {
    Event<T> holder = ringBuffer.get(seq);
    holder.setValue(e);
    ringBuffer.publish(seq);
  }

  @Override
  public void put(T e) throws InterruptedException {
    long seq = ringBuffer.next();
    publish(e, seq);
  }

  @Override
  public int remainingCapacity() {
    return ringBuffer.getBufferSize() - size();
  }

  @Override
  public int size() {
    return (int) (ringBuffer.getCursor() - consumedSeq.get());
  }

  @Override
  public T take() throws InterruptedException {
    long l = consumedSeq.get() + 1;
    while (knownPublishedSeq < l) {
      try {
        knownPublishedSeq = barrier.waitFor(l);
      } catch (AlertException e) {
        throw new IllegalStateException(e);
      } catch (TimeoutException e) {
        throw new IllegalStateException(e);
      }
    }
    Event<T> eventHolder = ringBuffer.get(l);
    T t = eventHolder.getValue();
    consumedSeq.incrementAndGet();
    return t;
  }

  @Override
  public String toString() {
    return "Cursor: " + ringBuffer.getCursor() + ", Consumerd :" + consumedSeq.get();
  }

  private void updatePublishedSequence() {
    long c = ringBuffer.getCursor();
    if (c >= knownPublishedSeq + 1) {
      long pos = c;
      for (long sequence = knownPublishedSeq + 1; sequence <= c; sequence++) {
        if (!ringBuffer.isPublished(sequence)) {
          pos = sequence - 1;
          break;
        }
      }
      knownPublishedSeq = pos;
    }
  }
}
