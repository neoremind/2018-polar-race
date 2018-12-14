package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
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
 * 使用file channel nio并发load vlog，并且缓存在direct buffer的offheap中，避免一次heap内转存。
 *
 * @author xu.zx
 */
public class ConcurrentFileChannelLoad2DirectOffheapVLogCache extends AbstractVLogCache implements VLogCache {

  private FileChannel fileChannel;

  private ByteBuffer[] buffer;

  private long fileSize;

  public ConcurrentFileChannelLoad2DirectOffheapVLogCache(File file, ExecutorService prepareLoadExecutor,
                                                          ExecutorService cleanerExecutor) throws IOException {
    this.prepareLoadExecutor = prepareLoadExecutor;
    this.fileChannel = new FileInputStream(file).getChannel();
    this.fileSize = fileChannel.size();
  }

  @Override
  public void load() {
    int totalRound = (int) (fileSize / SEGMENT_SIZE) + 1;
    int lastRoundSize = (int) fileSize & SEGMENT_SIZE_MASK;
    buffer = new ByteBuffer[totalRound];
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
    ByteBuffer b = buffer[offset / SEGMENT_SIZE];
    b.position(offset & SEGMENT_SIZE_MASK);
    b.get(result, 0, SIZE_OF_VALUE);
    return result;
  }

  @Override
  public void close() {
    for (ByteBuffer byteBuffer : buffer) {
      ((DirectBuffer) byteBuffer).cleaner().clean();
    }
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
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
      try {
        ByteBuffer b = ByteBuffer.allocateDirect(size);
        fileChannel.read(b, offset);
        b.flip();
        buffer[sliceNumber] = b;
      } catch (IOException e) {
        e.printStackTrace();
        Throwables.propagate(e);
      }
    }
  }
}
