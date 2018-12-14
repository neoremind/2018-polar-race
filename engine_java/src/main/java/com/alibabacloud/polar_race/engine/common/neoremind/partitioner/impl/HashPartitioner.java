package com.alibabacloud.polar_race.engine.common.neoremind.partitioner.impl;

import com.alibabacloud.polar_race.engine.common.neoremind.partitioner.Partitioner;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * 按照哈希规则的分区函数。
 * <pre>
 * abs(key的第一个byte) mod 分区数
 * </pre>
 *
 * @author xu.zx
 */
public class HashPartitioner implements Partitioner {

  private int numberOfPartition;

  public HashPartitioner(int numberOfPartition) {
    checkArgument(numberOfPartition < Byte.MAX_VALUE, "numberOfPartition should be less than MAX byte 127");
    this.numberOfPartition = numberOfPartition;
  }

  @Override
  public int partition(byte[] key) {
    return ((key[0] >> 7) == 0 ? key[0] : ~key[0] + (byte) 1) % (byte) (numberOfPartition);
  }

}
