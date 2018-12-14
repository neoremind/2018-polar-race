package com.alibabacloud.polar_race.engine.common.neoremind;

/**
 * 一些固定的全局变量。
 *
 * @author xu.zx
 */
public interface DBConstants {

  /**
   * 搭车使用的等待数量，等待到了这么多数据的请求再发起一次range操作。或者超时发起。
   */
  int ACCUMULATIVE_RUNNER_ACC_SIZE = 64;

  /**
   * accumulative runner搭车超时等待最长时间。
   */
  int ACCUMULATIVE_RUNNER_MAX_WAIT_TIME_IN_MS = 10000;

  /**
   * 定期检测是否超时的scheduler线程运行interval。
   */
  int ACCUMULATIVE_RUNNER_SCHEDULER_DELAY_IN_MS = 10000;

  /**
   * 等待队列被消费完
   */
  long PARK_NANOS_WHILE_QUEUE_NOT_EMPTY = 10;

  /**
   * 为了让最后一批数据visit完毕
   */
  long PARK_NANOS_AFTER_QUEUE_EMPTY = 10;

  /**
   * prefetch线程池大小，例如每个shard记录64000个，并发load每个分片load 2048个({@link VLogCache#SEGMENT_SIZE})，
   * 则大约需要6.4w/2048~=32个并发即可
   */
  int PREPARE_RANGE_LOAD_THREAD_POOL_SIZE = 17;

  /**
   * 清理线程池，用于freeup内存，关闭file channel等
   */
  int CLEANER_THREAD_POOL_SIZE = 4;

  /**
   * prefetch wal load线程池
   */
  int PREFETCH_WAL_LOAD_THREAD_POOL_SIZE = 4;

  /**
   * 异步消费kv的队列，队列内的元素是批量的kv
   */
  int ASYNC_VISIT_QUEUE_SIZE = 256;

  /**
   * 遍历数据，批量送到{@link #ASYNC_VISIT_QUEUE_SIZE}大小的blocking队列，由单独的线程消费，
   * 为了在scan的时候不停，解耦耗CPU的操作和IO。
   */
  int BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE = 256;

  int BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE_MASK = BATCH_SIZE_FOR_ASYNC_VISIT_QUEUE - 1;

  /**
   * range时候并发调用64个请求的visitor回调，这是并行度。
   */
  int VISIT_CONCURRENCY = 8;

  /**
   * 在range的时候，是否使用批量load
   */
  boolean CONCURRENT_RANGE_LOAD = true;

  int RANGE_LOAD_FROM_MEM_CONCURRENCY = 2;

}
