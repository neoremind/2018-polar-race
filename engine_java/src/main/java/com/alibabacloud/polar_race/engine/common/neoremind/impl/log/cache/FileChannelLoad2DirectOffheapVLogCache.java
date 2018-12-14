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

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 基于file channel load vlog，并且缓存在direct buffer的offheap中
 *
 * @author xu.zx
 */
public class FileChannelLoad2DirectOffheapVLogCache implements VLogCache {

  private FileChannel fileChannel;

  private ByteBuffer buffer;

  public FileChannelLoad2DirectOffheapVLogCache(File file) throws IOException {
    this.fileChannel = new FileInputStream(file).getChannel();
    this.buffer = ByteBuffer.allocateDirect((int) fileChannel.size());
  }

  @Override
  public void load() {
    try {
      fileChannel.read(buffer);
      buffer.flip();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public byte[] read(int offset) {
    byte[] result = new byte[SIZE_OF_VALUE];
    buffer.position(offset);
    buffer.get(result, 0, SIZE_OF_VALUE);
    return result;
  }

  @Override
  public void close() {
    ((DirectBuffer) buffer).cleaner().clean();
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  @Override
  public void freeUp() {

  }
}
