package com.alibabacloud.polar_race.engine.common.neoremind.offheap;

import com.alibabacloud.polar_race.engine.common.neoremind.KeyComparator;

import java.util.Arrays;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;

/**
 * 改进版的offheap hashmap，因为冲突太严重了，所以弃用
 *
 * @author xu.zx
 */
@Deprecated
public class OffheapHashMap2 {

  public static final int SIZE_OF_INT = 4;

  public static final int SIZE_OF_ADDRESS = 8;

  private int bucketMask;

  private int keySize;

  private int valueSize;

  private int keyValueSize;

  private long bucketAddress;

  public OffheapHashMap2(int bucketSize, int keySize, int valueSize) {
    this.bucketMask = bucketSize - 1;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.keyValueSize = keySize + valueSize;
    this.bucketAddress = Uns.allocate(bucketSize * SIZE_OF_ADDRESS);
    for (long offset = 0; offset < bucketSize * SIZE_OF_ADDRESS; offset++) {
      Uns.putByte(bucketAddress, offset, (byte) 0);
    }
  }

  /**
   * bucket
   * --------------------
   * |  locationAddress 1 |  ->  [entryCount, [key, value], [key, value] ...]
   * |  locationAddress 2 |
   * |        ...         |
   * --------------------
   */
  public void put(byte[] key, byte[] value) {
    if (key == null || value == null) {
      throw new IllegalArgumentException("Key and value should not be null");
    }
    if (key.length != keySize || value.length != valueSize) {
      throw new IllegalArgumentException("Key and value size wrong");
    }

    int bucketIndex = bucketIndexFor(key);
    long locationAddress = Uns.getLong(bucketAddress, bucketIndex * SIZE_OF_ADDRESS);

    // Read how many entries we expect in this partition
    int entryCount = locationAddress == 0 ? 0 : Uns.getInt(locationAddress, 0);

    locationAddress += SIZE_OF_INT;
    for (int locationOffset = 0; locationOffset < entryCount; locationOffset++) {
      long baseLocation = keyValueSize * locationOffset;
      boolean isEqual = true;
      for (int keyOffset = 0; keyOffset < keySize; keyOffset++) {
        if (key[keyOffset] != Uns.getByte(locationAddress, baseLocation + keyOffset)) {
          isEqual = false;
          break;
        }
      }

      if (isEqual) {
        Uns.copyMemory(value, 0, locationAddress, baseLocation + keySize, valueSize);
        return;
      }
    }
    locationAddress -= SIZE_OF_INT;

    if (locationAddress == 0) {
      locationAddress = Uns.allocate(SIZE_OF_INT + keyValueSize);
    } else {
      locationAddress = Uns.reallocate(locationAddress, SIZE_OF_INT + keyValueSize * (entryCount + 1));
    }

    Uns.putInt(locationAddress, 0, entryCount + 1);
    Uns.copyMemory(key, 0, locationAddress, SIZE_OF_INT + keyValueSize * entryCount, keySize);
    Uns.copyMemory(value, 0, locationAddress, SIZE_OF_INT + keyValueSize * entryCount + keySize, valueSize);

    Uns.putLong(bucketAddress, bucketIndex * SIZE_OF_ADDRESS, locationAddress);
  }

  public byte[] get(byte[] key) {
    if (key == null) {
      throw new IllegalArgumentException("Key should not be null");
    }

    int bucketIndex = bucketIndexFor(key);
    long locationAddress = Uns.getLong(bucketAddress, bucketIndex * SIZE_OF_ADDRESS);

    if (locationAddress == 0) {
      return null;
    }

    int entryCount = Uns.getInt(locationAddress, 0);

    locationAddress += SIZE_OF_INT;
    int locationOffset = binarySearch(locationAddress, entryCount - 1, key);
    if (locationOffset == -1) {
      return null;
    } else {
      byte[] value = new byte[valueSize];
      Uns.copyMemory(locationAddress, keyValueSize * locationOffset + keySize, value, 0, valueSize);
      return value;
    }
  }

  /**
   * return locationOffset
   */
  public int binarySearch(long baseAddress, int right, byte[] targetKey) {
    int left = 0;

    byte[] temp = new byte[SIZE_OF_KEY];

    while (left <= right) {
      int mid = (left + right) / 2;

      Uns.copyMemory(baseAddress, keyValueSize * mid, temp, 0, SIZE_OF_KEY);

      int compareResult = KeyComparator.COMPARATOR.compare(temp, targetKey);
      if (compareResult < 0) {
        left = mid + 1;
      } else if (compareResult > 0) {
        right = mid - 1;
      } else {
        return mid;
      }
    }

    return -1;
  }

  private int bucketIndexFor(byte[] key) {
    return hash(key) & bucketMask;
  }

  public int hash(byte a[]) {
    int h;
    return (h = arrayHashCode(a)) ^ (h >>> 16);
  }

  public int arrayHashCode(byte a[]) {
    int result = 1;
    for (int i = 1; i < a.length; i++) {
      result = 31 * result + a[i];
    }
    return result;
  }

  public static void main(String[] args) {
    OffheapHashMap2 offheapHashMap = new OffheapHashMap2(16, 8, 2);
    byte[] key = new byte[]{10, 20, 30, 40, 50, 60, 70, 80};
    byte[] value = new byte[]{37, 55};
    offheapHashMap.put(key, value);
    System.out.println(Arrays.toString(offheapHashMap.get(key)));

    value = new byte[]{37, 56};
    offheapHashMap.put(key, value);
    System.out.println(Arrays.toString(offheapHashMap.get(key)));

    key = new byte[]{10, 20, 30, 40, 50, 60, 70, 88};
    System.out.println(Arrays.toString(offheapHashMap.get(key)));
  }
}