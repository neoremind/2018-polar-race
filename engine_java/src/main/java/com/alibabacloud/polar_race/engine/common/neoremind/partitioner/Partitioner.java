package com.alibabacloud.polar_race.engine.common.neoremind.partitioner;

/**
 * 分区接口。用于kv系统进来的路由
 *
 * @author xu.zx
 */
public interface Partitioner {

  /**
   * 根据key来定位一个分区，shard id
   *
   * @param key key
   * @return 分区
   */
  int partition(byte[] key);

}
