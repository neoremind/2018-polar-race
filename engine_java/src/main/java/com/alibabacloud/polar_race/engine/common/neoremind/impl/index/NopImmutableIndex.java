package com.alibabacloud.polar_race.engine.common.neoremind.impl.index;

import com.alibabacloud.polar_race.engine.common.neoremind.ImmutableIndex;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 空的immutable index
 *
 * @author xu.zx
 */
public class NopImmutableIndex implements ImmutableIndex {

  private static final NopImmutableIndex SELF = new NopImmutableIndex();

  public static NopImmutableIndex instance() {
    return SELF;
  }

  @Override
  public int binarySearch(byte[] targetKey) {
    return -1;
  }

  @Override
  public int getSize() {
    return 0;
  }

  @Override
  public void add(byte[] keyAndVlogSeq) {

  }

  @Override
  public void destroy() {

  }

  @Override
  public void finish() {

  }
}
