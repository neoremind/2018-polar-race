package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MMapUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 不load cache，只用mmap read
 *
 * @author xu.zx
 */
public class MmapVlogReader implements VLogCache {

  private FileChannel fileChannel;

  private MappedByteBuffer mappedByteBuffer;

  public MmapVlogReader(File file) throws IOException {
    this.fileChannel = new FileInputStream(file).getChannel();
    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
  }

  @Override
  public void load() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] read(int offset) {
    byte[] result = new byte[SIZE_OF_VALUE];
    mappedByteBuffer.position(offset);
    mappedByteBuffer.get(result, 0, SIZE_OF_VALUE);
    return result;
  }

  @Override
  public void close() {
    if (mappedByteBuffer != null) {
      MMapUtil.unmap(mappedByteBuffer);
      mappedByteBuffer = null;
    }
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  @Override
  public void freeUp() {

  }
}
