package com.alibabacloud.polar_race.engine.common.neoremind.impl.index;

import com.google.common.base.Preconditions;

import com.alibabacloud.polar_race.engine.common.neoremind.ImmutableIndex;
import com.alibabacloud.polar_race.engine.common.neoremind.KeyComparator;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;

import java.util.ArrayList;
import java.util.List;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;

/**
 * 尝试Unsafe调用次数减少提高性能，多级索引，一部分放在heap，一部分放在offheap，效果不好，性能反而下降
 *
 * @author xu.zx
 */
@Deprecated
public class OffHeapMultiLevelImmutableIndex implements ImmutableIndex {

  public static final int RECORD_PER_PAGE = 256;

  public static final int RECORD_PER_PAGE_MASK = RECORD_PER_PAGE - 1;

  /**
   * 6400w/PARTITION(1024)=62500
   * 62500/RECORD_PER_PAGE(1024)+5~=61+5=66
   */
  public static final int FIRST_WAL_RECORDS_4_EVERY_PAGE_LIST_SIZE = ((64000000 / 1024) / RECORD_PER_PAGE) + 10;

  private long baseAddress;

  private int offset = 0;

  private int index = -1;

  private int pageIndex = -1;

  private List<byte[]> firstWalRecords4EveryPage;

  public OffHeapMultiLevelImmutableIndex(int recordSize) {
    this.baseAddress = Uns.allocate(recordSize * SIZE_OF_WAL_RECORD);
    this.firstWalRecords4EveryPage = new ArrayList<>(FIRST_WAL_RECORDS_4_EVERY_PAGE_LIST_SIZE);
  }

  @Override
  public int binarySearch(byte[] targetKey) {
    int toSearchPageIndex = binarySearchPages(targetKey);
    int left = toSearchPageIndex * RECORD_PER_PAGE;
    int right = (toSearchPageIndex == pageIndex) ? left + (index % RECORD_PER_PAGE) : left + RECORD_PER_PAGE - 1;
    return binarySearchOnOnePage(left, right, targetKey);
  }

  private int binarySearchPages(byte[] targetKey) {
    int left = 0;
    int right = pageIndex;

    while (left <= right) {
      int mid = (left + right) / 2;
      byte[] midRecord = firstWalRecords4EveryPage.get(mid);
      int compareResult = KeyComparator.COMPARATOR.compare(midRecord, targetKey);
      if (compareResult < 0) {
        left = mid + 1;
      } else if (compareResult > 0) {
        right = mid - 1;
      } else {
        return mid;
      }
    }

    return left - 1;
  }

  private int binarySearchOnOnePage(int left, int right, byte[] targetKey) {
    byte[] temp = new byte[SIZE_OF_KEY];

    while (left <= right) {
      int mid = (left + right) / 2;

      Uns.copyMemory(baseAddress, SIZE_OF_WAL_RECORD * mid, temp, 0, SIZE_OF_KEY);

      int compareResult = KeyComparator.COMPARATOR.compare(temp, targetKey);
      if (compareResult < 0) {
        left = mid + 1;
      } else if (compareResult > 0) {
        right = mid - 1;
      } else {
        int res = Uns.getInt(baseAddress, SIZE_OF_WAL_RECORD * mid + SIZE_OF_KEY);
        return res;
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
    Preconditions.checkArgument(keyAndVlogSeq.length == SIZE_OF_WAL_RECORD, "invalid key and vlog length");
    Uns.copyMemory(keyAndVlogSeq, 0, baseAddress, offset, SIZE_OF_WAL_RECORD);
    offset += SIZE_OF_WAL_RECORD;
    index++;
    if ((index & RECORD_PER_PAGE_MASK) == 0) {
      pageIndex++;
      firstWalRecords4EveryPage.add(keyAndVlogSeq);
    }
  }

  @Override
  public void destroy() {
    Uns.free(baseAddress);
  }

  @Override
  public void finish() {

  }
}
