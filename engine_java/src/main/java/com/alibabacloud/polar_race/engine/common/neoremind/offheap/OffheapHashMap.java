package com.alibabacloud.polar_race.engine.common.neoremind.offheap;

import java.util.Arrays;

/**
 * 尝试read的时候，使用堆外hashmap，散列效果不好，冲突比较严重，退化成数组访问，再二分的效果不好。
 *
 * @author xu.zx
 */
@Deprecated
public class OffheapHashMap {

  public static final int SIZE_OF_INT = 4;

  public static final int SIZE_OF_ADDRESS = 8;

  private int bucketMask;

  private int keySize;

  private int valueSize;

  private int keyValueSize;

  private long bucketAddress;

  public OffheapHashMap(int bucketSize, int keySize, int valueSize) {
    this.bucketMask = bucketSize - 1;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.keyValueSize = keySize + valueSize;
    this.bucketAddress = Uns.allocate(bucketSize * SIZE_OF_ADDRESS);
    for (long offset = 0; offset < bucketSize * SIZE_OF_ADDRESS; offset++) {
      Uns.putByte(bucketAddress, offset, (byte) 0x00);
    }
  }

  /**
   * bucket
   * --------------------
   * |  locationAddress 1 |  ->  [entryCount, kv address1, kv address2 ...]
   * |  locationAddress 2 |                        \
   * |        ...         |                      [key, value]
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

    for (long locationOffset = 0; locationOffset < entryCount; locationOffset++) {
      long keyValueAddress = Uns.getLong(locationAddress, SIZE_OF_INT + SIZE_OF_ADDRESS * locationOffset);

      boolean isEqual = true;
      for (int keyOffset = 0; keyOffset < keySize; keyOffset++) {
        if (key[keyOffset] != Uns.getByte(keyValueAddress, keyOffset)) {
          isEqual = false;
          break;
        }
      }

      if (isEqual) {
        Uns.copyMemory(value, 0, keyValueAddress, keySize, valueSize);
        return;
      }
    }

    long entryAddress = Uns.allocate(keyValueSize);
    Uns.copyMemory(key, 0, entryAddress, 0, keySize);
    Uns.copyMemory(value, 0, entryAddress, keySize, valueSize);

    if (locationAddress == 0) {
      locationAddress = Uns.allocate(SIZE_OF_INT + SIZE_OF_ADDRESS);
    } else {
      locationAddress = Uns.reallocate(locationAddress, SIZE_OF_INT + SIZE_OF_ADDRESS * (entryCount + 1));
    }

    Uns.putInt(locationAddress, 0, entryCount + 1);
    Uns.putLong(locationAddress, SIZE_OF_INT + SIZE_OF_ADDRESS * entryCount, entryAddress);

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
    for (long locationOffset = 0; locationOffset < entryCount; locationOffset++) {
      long keyValueAddress = Uns.getLong(locationAddress, SIZE_OF_ADDRESS * locationOffset);

      boolean isEqual = true;
      for (int keyOffset = 0; keyOffset < keySize; keyOffset++) {
        if (key[keyOffset] != Uns.getByte(keyValueAddress, keyOffset)) {
          isEqual = false;
          break;
        }
      }

      if (isEqual) {
        byte[] value = new byte[valueSize];
        Uns.copyMemory(keyValueAddress, keySize, value, 0, valueSize);
        return value;
      }
    }

    return null;
  }

  private int bucketIndexFor(byte[] key) {
    return Arrays.hashCode(key) & bucketMask;
  }

  public static void main(String[] args) {
    OffheapHashMap offheapHashMap = new OffheapHashMap(16, 4, 2);
    byte[] key = new byte[]{10, 20, 30, 40};
    byte[] value = new byte[]{37, 55};
    offheapHashMap.put(key, value);
    System.out.println(Arrays.toString(offheapHashMap.get(key)));

    value = new byte[]{37, 56};
    offheapHashMap.put(key, value);
    System.out.println(Arrays.toString(offheapHashMap.get(key)));

    key = new byte[]{10, 20, 30, 88};
    System.out.println(Arrays.toString(offheapHashMap.get(key)));
  }
}