package com.alibabacloud.polar_race.engine.common.neoremind;


import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;

import java.util.Comparator;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;

/**
 * vlog seq比较器。
 *
 * @author xu.zx
 */
public final class VlogSeqComparator implements Comparator<byte[]> {

  public static final VlogSeqComparator COMPARATOR = new VlogSeqComparator();

  @Override
  public int compare(byte[] a, byte[] b) {
    if (a == b) {
      return 0;
    }

    int x = BytewiseUtil.fromBEBytes(a[SIZE_OF_KEY], a[SIZE_OF_KEY + 1], a[SIZE_OF_KEY + 2], a[SIZE_OF_KEY + 3]);
    int y = BytewiseUtil.fromBEBytes(b[SIZE_OF_KEY], b[SIZE_OF_KEY + 1], b[SIZE_OF_KEY + 2], b[SIZE_OF_KEY + 3]);
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }
}
