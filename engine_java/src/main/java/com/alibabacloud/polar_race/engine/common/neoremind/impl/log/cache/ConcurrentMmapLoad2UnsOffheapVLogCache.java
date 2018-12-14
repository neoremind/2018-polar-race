package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MMapUtil;

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
 * 使用mmap并发load vlog，并且缓存在{@link Uns}的offheap中
 *
 * @author xu.zx
 */
public class ConcurrentMmapLoad2UnsOffheapVLogCache extends AbstractVLogCache implements VLogCache {

  private FileChannel fileChannel;

  private long address;

  private long fileSize;

  public ConcurrentMmapLoad2UnsOffheapVLogCache(File file, ExecutorService prepareLoadExecutor,
                                                ExecutorService cleanerExecutor) throws IOException {
    this.prepareLoadExecutor = prepareLoadExecutor;
    this.cleanerExecutor = cleanerExecutor;
    this.fileChannel = new FileInputStream(file).getChannel();
    this.fileSize = fileChannel.size();
    address = Uns.allocate(fileSize);
  }

  @Override
  public void load() {
    int totalRound = (int) (fileSize / SEGMENT_SIZE) + 1;
    int lastRoundSize = (int) fileSize & SEGMENT_SIZE_MASK;
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
    Uns.copyMemory(address, offset, result, 0, SIZE_OF_VALUE);
    return result;
  }

  @Override
  public void close() {
    Uns.free(address);
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
      byte[] buffer = new byte[size];
      MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, size);
      mappedByteBuffer.get(buffer, 0, size);
      Uns.copyMemory(buffer, 0, address, offset, size);
      cleanerExecutor.submit(() -> MMapUtil.unmap(mappedByteBuffer));
      return sliceNumber;
    }
  }
}
