package com.alibabacloud.polar_race.engine.common.neoremind.impl.index;

//import com.alibabacloud.polar_race.engine.common.neoremind.ImmutableIndex;
//import com.alibabacloud.polar_race.engine.common.neoremind.KeyComparator;
//import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
//import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;
//import com.taobao.gcih.GCInvisibleHeap;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
//import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
//
///**
// * 使用alijdk GCIH特性的索引，把对象脱离JVM GC，但是发现对象大小太大了，需要设置-XX:GCIHSize=2200m才可以
// */
//@Deprecated
//public class GCIHImmutableIndex implements ImmutableIndex {
//
//  private byte[][] array;
//
//  private int index = -1;
//
//  public GCIHImmutableIndex(int recordSize) {
//    array = new byte[recordSize][];
//  }
//
//  @Override
//  public void finish() {
//    array = GCInvisibleHeap.moveIn(array);
//  }
//
//  @Override
//  public int binarySearch(byte[] targetKey) {
//    int left = 0;
//    int right = index;
//
//    while (left <= right) {
//      int mid = (left + right) / 2;
//
//      int compareResult = KeyComparator.COMPARATOR.compare(array[mid], targetKey);
//
//      if (compareResult < 0) {
//        left = mid + 1;
//      } else if (compareResult > 0) {
//        right = mid - 1;
//      } else {
//        byte[] keyVlog = array[mid];
//        return BytewiseUtil.fromBEBytes(keyVlog[8], keyVlog[9], keyVlog[10], keyVlog[11]);
//      }
//    }
//
//    return -1;
//  }
//
//  @Override
//  public int getSize() {
//    return index + 1;
//  }
//
//  @Override
//  public void add(byte[] keyAndVlogSeq) {
//    index++;
//    array[index] = keyAndVlogSeq;
//  }
//
//  @Override
//  public void destroy() {
//  }
//
//}
