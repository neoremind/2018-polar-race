package com.alibabacloud.polar_race.engine.common.neoremind;

import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author xu.zx
 */
@Ignore
public class KeyComparatorTest {

  @Test
  public void test() {
    byte[] bytes1 = BytewiseUtil.hexStringToByteArray("00 12 22 33 44 CC FF EE");
    byte[] bytes2 = BytewiseUtil.hexStringToByteArray("00 12 22 33 44 CC FF EE");
    long n1 = BytewiseUtil.fromBytes(bytes1);
    System.out.println(UnsignedLongKeyComparator.compare(bytes2, n1));
  }

}
