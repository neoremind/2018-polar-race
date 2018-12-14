package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_DIRECT;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_NOATIME;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_RDONLY;

/**
 * 使用file channel nio，底层利用pread并发load vlog，并且缓存在direct buffer的offheap中。
 * 由于{@link ByteBuffer}非线程安全，这里采用其内存地址，使用{@link Uns#copyMemory(long, long, byte[], int, long)}
 * 来直接拷贝堆外内存到heap中。使用{@link DirectByteBufferPool}来复用堆外内存。
 *
 * @author xu.zx
 */
public class ConcurrentJNADirectIOLoad2DirectOffheapUsingUnsafeVLogCache extends AbstractVLogCache implements VLogCache {

  private FileChannel fileChannel;

  private Pointer[] pointers;

  private long[] address;

  private DirectIOPointerPool directIOPointerPool;

  private NativeLong defaultNativeLong;

  static {
    Native.register("c");
  }

  private native int open(String pathname, int flags);

  private static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

  private native int close(int fd);

  //private static native int getpagesize();

  private int fd;

  private int totalRound;

  private int lastRoundSize;

  public ConcurrentJNADirectIOLoad2DirectOffheapUsingUnsafeVLogCache(File file, ExecutorService prepareLoadExecutor,
                                                                     ExecutorService cleanerExecutor) throws IOException {
    this.prepareLoadExecutor = prepareLoadExecutor;
    fd = open(file.getAbsolutePath(), O_NOATIME | O_RDONLY | O_DIRECT);
    this.fileChannel = new FileInputStream(file).getChannel();
    long fileSize = fileChannel.size();
    totalRound = (int) (fileSize / SEGMENT_SIZE) + 1;
    lastRoundSize = (int) fileSize & SEGMENT_SIZE_MASK;
    defaultNativeLong = new NativeLong(SEGMENT_SIZE);
    directIOPointerPool = DirectIOPointerPool.getInstance();
    directIOPointerPool.initIfPossible(Math.min(totalRound * 3, MAX_DIRECT_BUFFER_POLL_SIZE), SEGMENT_SIZE);
  }

  @Override
  public void load() {
    pointers = new Pointer[totalRound];
    address = new long[totalRound];
    int offset = 0;
    List<Future> futureList = new ArrayList<>(totalRound);
    for (int i = 0; i < totalRound - 1; i++) {
      futureList.add(prepareLoadExecutor.submit(new LoadTask(i, offset, SEGMENT_SIZE)));
      offset += SEGMENT_SIZE;
    }
    futureList.add(prepareLoadExecutor.submit(new LoadTask(totalRound - 1, offset, lastRoundSize)));
    waitFutures(totalRound, futureList);
  }

  @Override
  public byte[] read(int offset) {
    byte[] result = new byte[SIZE_OF_VALUE];
    Uns.copyMemory(address[offset / SEGMENT_SIZE], offset & SEGMENT_SIZE_MASK, result, 0, SIZE_OF_VALUE);
    return result;
  }

  @Override
  public void close() {
    if (close(fd) < 0) {
      throw new RuntimeException("Problems occurred while doing close()");
    }
  }

  @Override
  public void freeUp() {
    for (Pointer pointer : pointers) {
      directIOPointerPool.recycle(pointer);
    }
    pointers = null;
    address = null;
    if (fileChannel != null) {
      Closeables.closeQuietly(fileChannel);
    }
  }

  class LoadTask implements Runnable {

    private int sliceNumber;

    private int offset;

    private int size;

    public LoadTask(int sliceNumber, int offset, int size) {
      this.sliceNumber = sliceNumber;
      this.offset = offset;
      this.size = size;
    }

    @Override
    public void run() {
      Pointer pointer = directIOPointerPool.take();
      pread(fd, pointer, size == SEGMENT_SIZE ? defaultNativeLong : new NativeLong(size), new NativeLong(offset));
      pointers[sliceNumber] = pointer;
      address[sliceNumber] = Pointer.nativeValue(pointer);
    }
  }
}
