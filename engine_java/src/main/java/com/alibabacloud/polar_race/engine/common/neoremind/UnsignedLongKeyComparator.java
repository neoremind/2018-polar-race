package com.alibabacloud.polar_race.engine.common.neoremind;


import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;

import java.util.Comparator;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;

/**
 * key unsigned long比较器。
 *
 * @author xu.zx
 */
public final class UnsignedLongKeyComparator implements Comparator<byte[]> {

  public static final UnsignedLongKeyComparator COMPARATOR = new UnsignedLongKeyComparator();

  @Override
  public int compare(byte[] a, byte[] b) {
    return Long.compareUnsigned(BytewiseUtil.fromBytes(a), BytewiseUtil.fromBytes(b));
  }

  public static int compare(byte[] a, long b) {
    return Long.compareUnsigned(BytewiseUtil.fromBytes(a), b);
  }
}
