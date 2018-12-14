package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MMapUtil;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MoreMappedByteBuffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 使用mmap并发load vlog，并且一次性调用load，直接使用MappedByteBuffer
 *
 * @author xu.zx
 */
public class ConcurrentMmapLoad2MappedByteBufferVLogCache2 extends AbstractVLogCache implements VLogCache {

  private FileChannel fileChannel;

  private long fileSize;

  private MappedByteBuffer[] mappedByteBuffers;

  private long[] mappedByteBufferAddress;

  public ConcurrentMmapLoad2MappedByteBufferVLogCache2(File file, ExecutorService prepareLoadExecutor,
                                                       ExecutorService cleanerExecutor) throws IOException {
    this.prepareLoadExecutor = prepareLoadExecutor;
    this.cleanerExecutor = cleanerExecutor;
    this.fileChannel = new FileInputStream(file).getChannel();
    this.fileSize = fileChannel.size();
  }

  @Override
  public void load() {
    int totalRound = (int) (fileSize / SEGMENT_SIZE) + 1;
    int lastRoundSize = (int) fileSize & SEGMENT_SIZE_MASK;
    mappedByteBuffers = new MappedByteBuffer[totalRound];
    mappedByteBufferAddress = new long[totalRound];
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
    Uns.copyMemory(mappedByteBufferAddress[offset / SEGMENT_SIZE], offset & SEGMENT_SIZE_MASK, result, 0, SIZE_OF_VALUE);
    return result;
  }

  @Override
  public void close() {
    for (MappedByteBuffer mappedByteBuffer : mappedByteBuffers) {
      MMapUtil.unmap(mappedByteBuffer);
    }
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  class LoadTask implements Callable<Integer> {

    private int sliceNumber;

    private int offset;

    private int size;

    public LoadTask(int sliceNumber, int offset, int size) {
      this.sliceNumber = sliceNumber;
      this.offset = offset;
      this.size = size;
    }

    @Override
    public Integer call() throws Exception {
      MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, size);
      mappedByteBuffer.load();
      mappedByteBuffers[sliceNumber] = mappedByteBuffer;
      mappedByteBufferAddress[sliceNumber] = MoreMappedByteBuffer.getInstance().getAddress(mappedByteBuffer);
      return sliceNumber;
    }
  }
}
