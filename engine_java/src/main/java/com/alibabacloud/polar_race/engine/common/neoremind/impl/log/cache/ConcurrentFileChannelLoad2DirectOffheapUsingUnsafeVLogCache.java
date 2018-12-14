package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;

import sun.nio.ch.DirectBuffer;

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

/**
 * 使用file channel nio，底层利用pread并发load vlog，并且缓存在direct buffer的offheap中。
 * 由于{@link ByteBuffer}非线程安全，这里采用其内存地址，使用{@link Uns#copyMemory(long, long, byte[], int, long)}
 * 来直接拷贝堆外内存到heap中。使用{@link DirectByteBufferPool}来复用堆外内存。
 *
 * @author xu.zx
 */
public class ConcurrentFileChannelLoad2DirectOffheapUsingUnsafeVLogCache extends AbstractVLogCache implements VLogCache {

  private FileChannel fileChannel;

  private ByteBuffer[] buffer;

  private long[] address;

  private long fileSize;

  private DirectByteBufferPool directByteBufferPool;

  public ConcurrentFileChannelLoad2DirectOffheapUsingUnsafeVLogCache(File file, ExecutorService prepareLoadExecutor,
                                                                     ExecutorService cleanerExecutor) throws IOException {
    this.prepareLoadExecutor = prepareLoadExecutor;
    this.fileChannel = new FileInputStream(file).getChannel();
    this.fileSize = fileChannel.size();
  }

  @Override
  public void load() {
    int totalRound = (int) (fileSize / SEGMENT_SIZE) + 1;
    directByteBufferPool = DirectByteBufferPool.getInstance();
    directByteBufferPool.initIfPossible(Math.min(totalRound * 3, MAX_DIRECT_BUFFER_POLL_SIZE), SEGMENT_SIZE);
    buffer = new ByteBuffer[totalRound];
    address = new long[totalRound];
    int offset = 0;
    List<Future> futureList = new ArrayList<>(totalRound);
    for (int i = 0; i < totalRound; i++) {
      futureList.add(prepareLoadExecutor.submit(new LoadTask(i, offset)));
      offset += SEGMENT_SIZE;
    }
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
    for (ByteBuffer byteBuffer : buffer) {
      directByteBufferPool.recycle(byteBuffer);
    }
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  class LoadTask implements Runnable {

    private int sliceNumber;

    private int offset;

    public LoadTask(int sliceNumber, int offset) {
      this.sliceNumber = sliceNumber;
      this.offset = offset;
    }

    @Override
    public void run() {
      try {
        ByteBuffer b = directByteBufferPool.take();
        b.clear();
        fileChannel.read(b, offset);
        // b.flip();
        buffer[sliceNumber] = b;
        address[sliceNumber] = ((DirectBuffer) b).address();
      } catch (IOException e) {
        e.printStackTrace();
        Throwables.propagate(e);
      }
    }
  }
}
