package com.alibabacloud.polar_race.engine.common.neoremind.partitioner.impl;

import com.alibabacloud.polar_race.engine.common.neoremind.partitioner.Partitioner;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * 按照范围规则的分区函数。
 * <pre>
 * 从<code>00 00 00 00 00 00 00 00 </code>到 <code>FF FF FF FF FF FF FF FF</code>
 * 等分n个区间。
 * </pre>
 *
 * @author xu.zx
 */
public class RangePartitioner implements Partitioner {

  public RangePartitioner(int numberOfPartition) {
    checkArgument(numberOfPartition == 1024, "partition number only support 1024 now");
  }

  /**
   * // TODO assume partition number is 1024
   */
  @Override
  public int partition(byte[] key) {
    int result = ((0x03 & (key[0] >>> 6))) << 8;
    int subResult = ((0x3F & key[0]) << 2);
    int lastResult = 0x03 & (key[1] >>> 6);
    return result + subResult + lastResult;
  }
}
