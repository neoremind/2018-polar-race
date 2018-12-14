package com.alibabacloud.polar_race.engine.common.neoremind;


import java.util.Comparator;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;

/**
 * key字典序比较器。
 *
 * @author xu.zx
 */
public final class KeyComparator implements Comparator<byte[]> {

  public static final KeyComparator COMPARATOR = new KeyComparator();

  @Override
  public int compare(byte[] a, byte[] b) {
    for (int i = 1; i < SIZE_OF_KEY; i++) {
      int thisByte = 0xFF & a[i];
      int thatByte = 0xFF & b[i];
      if (thisByte != thatByte) {
        return (thisByte) - (thatByte);
      }
    }
    return 0;
  }

}
