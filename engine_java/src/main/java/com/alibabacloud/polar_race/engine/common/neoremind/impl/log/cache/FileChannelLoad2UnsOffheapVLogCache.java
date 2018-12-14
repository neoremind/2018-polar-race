package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.cache;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogCache;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 基于file channel load vlog，并且缓存在{@link Uns}的offheap中
 *
 * @author xu.zx
 */
public class FileChannelLoad2UnsOffheapVLogCache implements VLogCache {

  public static final int PAGE_SIZE = 4096 * 256;

  private final FileChannel fileChannel;

  private long offset;

  private long address;

  private ByteBuffer buffer;

  private byte[] temp;

  public FileChannelLoad2UnsOffheapVLogCache(File file) throws IOException {
    this.fileChannel = new FileInputStream(file).getChannel();
    this.address = Uns.allocate(fileChannel.size());
    this.buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
    this.temp = new byte[PAGE_SIZE];
  }

  @Override
  public void load() {
    try {
      while (-1 != (fileChannel.read(buffer))) {
        buffer.flip();

//        while (buffer.hasRemaining()) {
//          Uns.copyMemory(buffer.get(), buffer.position(), address, offset, PAGE_SIZE);
//        }
        int size = buffer.remaining();
        buffer.get(temp, 0, size);
        Uns.copyMemory(temp, 0, address, offset, size);
        offset += size;
        buffer.clear();
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
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
  }

  @Override
  public void freeUp() {

  }
}
