package com.alibabacloud.polar_race.engine.common.neoremind.offheap;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Uns {

  // offset of LRU replacement strategy next pointer (8 bytes, long)
  static final long ENTRY_OFF_LRU_NEXT = 0;
  // offset of LRU replacement strategy previous pointer (8 bytes, long)
  static final long ENTRY_OFF_LRU_PREV = 8;
  // offset of next hash entry in a hash bucket (8 bytes, long)
  static final long ENTRY_OFF_NEXT = 16;
  // offset of entry reference counter (4 bytes, int)
  static final long ENTRY_OFF_REFCOUNT = 24;
  // offset of entry sentinel (4 bytes, int)
  static final long ENTRY_OFF_SENTINEL = 28;
  // slot in which the entry resides (8 bytes, long)
  static final long ENTRY_OFF_EXPIRE_AT = 32;
  // LRU generation (4 bytes, int, only 2 distinct values)
  static final long ENTRY_OFF_GENERATION = 40;
  // bytes 44..47 unused
  // offset of serialized hash value (8 bytes, long)
  static final long ENTRY_OFF_HASH = 48;
  // offset of serialized value length (4 bytes, int)
  static final long ENTRY_OFF_VALUE_LENGTH = 56;
  // offset of serialized hash key length (4 bytes, int)
  static final long ENTRY_OFF_KEY_LENGTH = 60;
  // offset of data in first block
  static final long ENTRY_OFF_DATA = 64;

  private static final Unsafe unsafe;
  private static final IAllocator allocator;

  private static final class AllocInfo {
    final long size;
    final Throwable trace;

    AllocInfo(Long size, Throwable trace) {
      this.size = size;
      this.trace = trace;
    }
  }

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
      //if (unsafe.addressSize() > 8)
      //  throw new RuntimeException("Address size " + unsafe.addressSize() + " not supported yet (max 8 bytes)");

      //if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
      //  LOGGER.warn("Degraded performance due to off-heap memory allocations and access guarded by debug code enabled via system property " + "uns.debugOffHeapAccess=true");

      IAllocator alloc = new UnsafeAllocator();

      allocator = alloc;
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private Uns() {
  }

  public static long getLongFromByteArray(byte[] array, int offset) {
    if (offset < 0 || offset + 8 > array.length)
      throw new ArrayIndexOutOfBoundsException();
    return unsafe.getLong(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
  }

  public static int getIntFromByteArray(byte[] array, int offset) {
    if (offset < 0 || offset + 4 > array.length)
      throw new ArrayIndexOutOfBoundsException();
    return unsafe.getInt(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
  }

  public static short getShortFromByteArray(byte[] array, int offset) {
    if (offset < 0 || offset + 2 > array.length)
      throw new ArrayIndexOutOfBoundsException();
    return unsafe.getShort(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
  }

  public static long getAndPutLong(long address, long offset, long value) {
    return unsafe.getAndSetLong(null, address + offset, value);
  }

  public static void putLong(long address, long offset, long value) {
    unsafe.putLong(null, address + offset, value);
  }

  public static long getLong(long address, long offset) {
    return unsafe.getLong(null, address + offset);
  }

  public static void putInt(long address, long offset, int value) {
    unsafe.putInt(null, address + offset, value);
  }

  public static int getInt(long address, long offset) {
    return unsafe.getInt(null, address + offset);
  }

  public static void putShort(long address, long offset, short value) {
    unsafe.putShort(null, address + offset, value);
  }

  public static short getShort(long address, long offset) {
    return unsafe.getShort(null, address + offset);
  }

  public static void putByte(long address, long offset, byte value) {
    unsafe.putByte(null, address + offset, value);
  }

  public static byte getByte(long address, long offset) {
    return unsafe.getByte(null, address + offset);
  }

  public static boolean decrement(long address, long offset) {
    long v = unsafe.getAndAddInt(null, address + offset, -1);
    return v == 1;
  }

  public static void increment(long address, long offset) {
    unsafe.getAndAddInt(null, address + offset, 1);
  }

  public static void copyMemory(byte[] arr, int off, long address, long offset, long len) {
    unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address + offset, len);
  }

  public static void copyMemory(long address, long offset, byte[] arr, int off, long len) {
    unsafe.copyMemory(null, address + offset, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
  }

  public static void copyMemory(long src, long srcOffset, long dst, long dstOffset, long len) {
    unsafe.copyMemory(null, src + srcOffset, null, dst + dstOffset, len);
  }

  public static void setMemory(long address, long offset, long len, byte val) {
    unsafe.setMemory(address + offset, len, val);
  }

  public static long getTotalAllocated() {
    return allocator.getTotalAllocated();
  }

  static long reallocate(long address, long bytes) {
    return unsafe.reallocateMemory(address, bytes);
  }

  public static long allocate(long bytes) {
    return allocate(bytes, false);
  }

  public static long allocate(long bytes, boolean throwOOME) {
    long address = allocator.allocate(bytes);
    if (address == 0L) {
      if (throwOOME) {
        throw new OutOfMemoryError("unable to allocate " + bytes + " in off-heap");
      }
    }
    return address;
  }

  public static long allocateIOException(long bytes) throws IOException {
    return allocateIOException(bytes, false);
  }

  public static long allocateIOException(long bytes, boolean throwOOME) throws IOException {
    long address = allocate(bytes, throwOOME);
    if (address == 0L) {
      throw new IOException("unable to allocate " + bytes + " in off-heap");
    }
    return address;
  }

  public static void free(long address) {
    if (address == 0L) {
      return;
    }
    allocator.free(address);
  }

}
