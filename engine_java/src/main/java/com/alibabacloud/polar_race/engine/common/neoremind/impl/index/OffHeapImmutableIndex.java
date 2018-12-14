package com.alibabacloud.polar_race.engine.common.neoremind.impl.index;

import com.alibabacloud.polar_race.engine.common.neoremind.ImmutableIndex;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;

/**
 * 堆外二分查找的索引，用于read
 *
 * @author xu.zx
 */
public class OffHeapImmutableIndex implements ImmutableIndex {

  private long baseAddress;

  private int offset = 0;

  private int index = -1;

  public OffHeapImmutableIndex(int recordSize) {
    this.baseAddress = Uns.allocate(recordSize * SIZE_OF_WAL_RECORD);
  }

  @Override
  public int binarySearch(byte[] targetKey) {
    //KEY->ULONG比较，或者一个个字节比较
    //long unsignedLongKey = BytewiseUtil.fromBytes(targetKey);

    int left = 0;
    int right = index;

    //byte[] temp = new byte[SIZE_OF_KEY];

    while (left <= right) {
      int mid = (left + right) / 2;

      int compareResult = 0;
      for (int keyOffset = 1; keyOffset < SIZE_OF_KEY; keyOffset++) {
        int thisByte = 0xFF & Uns.getByte(baseAddress, SIZE_OF_WAL_RECORD * mid + keyOffset);
        int thatByte = 0xFF & targetKey[keyOffset];
        if (thisByte != thatByte) {
          compareResult = thisByte - thatByte;
          break;
        }
      }
      //Uns.copyMemory(baseAddress, SIZE_OF_WAL_RECORD * mid, temp, 0, SIZE_OF_KEY);

      //int compareResult = UnsignedLongKeyComparator.compare(temp, unsignedLongKey);
      if (compareResult < 0) {
        left = mid + 1;
      } else if (compareResult > 0) {
        right = mid - 1;
      } else {
        return Uns.getInt(baseAddress, SIZE_OF_WAL_RECORD * mid + SIZE_OF_KEY);
      }
    }

    return -1;
  }

  @Override
  public int getSize() {
    return index + 1;
  }

  @Override
  public void add(byte[] keyAndVlogSeq) {
    Uns.copyMemory(keyAndVlogSeq, 0, baseAddress, offset, SIZE_OF_WAL_RECORD);
    offset += SIZE_OF_WAL_RECORD;
    index++;
  }

  @Override
  public void destroy() {
    Uns.free(baseAddress);
  }

  @Override
  public void finish() {

  }
}
