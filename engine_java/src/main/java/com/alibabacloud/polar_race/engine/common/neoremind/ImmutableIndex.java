package com.alibabacloud.polar_race.engine.common.neoremind;

/**
 * read使用的索引
 *
 * @author xu.zx
 */
public interface ImmutableIndex {

  int binarySearch(byte[] targetKey);

  int getSize();

  void add(byte[] keyAndVlogSeq);

  void destroy();

  void finish();
}
