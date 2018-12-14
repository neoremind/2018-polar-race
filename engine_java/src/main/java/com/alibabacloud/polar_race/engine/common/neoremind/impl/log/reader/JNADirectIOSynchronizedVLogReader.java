package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.reader;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogReader;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.io.FileNotFoundException;
import java.io.IOException;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_DIRECT;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_NOATIME;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_RDONLY;

/**
 * 直接利用jna direct io读文件，对于读取是pread从磁盘到内存，然后read内存，原子操作需要加锁。
 *
 * @author xu.zx
 */
public class JNADirectIOSynchronizedVLogReader implements VLogReader {

  public static final int BLOCK_SIZE = SIZE_OF_VALUE;

  public static ThreadLocal<byte[]> VALUE_THREAD_LOCAL = ThreadLocal.withInitial(() -> new byte[SIZE_OF_VALUE]);

  static {
    Native.register("c");
    //PAGE_SIZE = getpagesize();
  }

  public static NativeLong COUNT;

  //public static int PAGE_SIZE;

  private native int open(String pathname, int flags);

  //private native int read(int fd, Pointer buf, int count);

  private static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

  private native int posix_memalign(PointerByReference memptr, int alignment, int size);

  private native int close(int fd);

  //private static native int getpagesize();

  private int fd;

  private final String path;

  private Pointer bufPnt;

  private NativeLong mutableOffset;

  private long address;

  public JNADirectIOSynchronizedVLogReader(String path) {
    this.path = path;
  }

  @Override
  public void close() throws IOException {
    if (close(fd) < 0) {
      throw new IOException("Problems occurred while doing close()");
    }
  }

  public static void globalInit() {
    COUNT = new NativeLong(BLOCK_SIZE);
    //PAGE_SIZE = getpagesize();
  }

  @Override
  public void init() throws FileNotFoundException {
    fd = open(path, O_NOATIME | O_RDONLY | O_DIRECT);
    PointerByReference pntByRef = new PointerByReference();
    posix_memalign(pntByRef, BLOCK_SIZE, BLOCK_SIZE);
    bufPnt = pntByRef.getValue();
    address = Pointer.nativeValue(bufPnt);
    mutableOffset = new NativeLong(0);
  }

  @Override
  public byte[] readValue(long offset) {
    byte[] value = VALUE_THREAD_LOCAL.get();
    synchronized (this) {
      mutableOffset.setNumber(offset);
      pread(fd, bufPnt, COUNT, mutableOffset);
      Uns.copyMemory(address, 0, value, 0, SIZE_OF_VALUE);
      //bufPnt.read(0, value, 0, SIZE_OF_VALUE);
    }
    return value;
  }
}
