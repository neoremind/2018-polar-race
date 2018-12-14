package com.alibabacloud.polar_race.engine.common.neoremind;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * VLogCache
 *
 * @author xu.zx
 */
public interface VLogCache {

  /**
   * heap or offheap中一块内存的大小
   */
  int SEGMENT_SIZE = SIZE_OF_VALUE * 4096;

  /**
   * 110 * {@link #SEGMENT_SIZE} = 880MB < -XX:MaxDirectMemorySize=1G
   * <p/>
   * 否则抛出can not allocate memory的ERROR。
   */
  int MAX_DIRECT_BUFFER_POLL_SIZE = 55;

  int SEGMENT_SIZE_MASK = SEGMENT_SIZE - 1;

  void load();

  byte[] read(int offset);

  void close();

  void freeUp();

}
