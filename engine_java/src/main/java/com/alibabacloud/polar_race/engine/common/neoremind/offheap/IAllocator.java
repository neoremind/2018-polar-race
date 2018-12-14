package com.alibabacloud.polar_race.engine.common.neoremind.offheap;

public interface IAllocator {
  long allocate(long size);

  void free(long peer);

  long getTotalAllocated();
}
