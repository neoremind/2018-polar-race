package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.reader;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogReader;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 基于file channel的vlog reader
 *
 * @author xu.zx
 */
public class FileChannelVLogReader implements VLogReader {

  private final File file;

  private FileChannel[] fileChannel;

  private int nonAccurateCounter = 0;

  public FileChannelVLogReader(File file) {
    this.file = file;
  }

  @Override
  public void init() throws FileNotFoundException {
    this.fileChannel = new FileChannel[4];
    for (int i = 0; i < 4; i++) {
      this.fileChannel[i] = new FileInputStream(file).getChannel();
    }
  }

  @Override
  public byte[] readValue(long offset) {
    try {
      byte[] value = new byte[SIZE_OF_VALUE];
      //IOUtils.readBytes(value, fileChannel[nonAccurateCounter++ & 3], offset, SIZE_OF_VALUE);
      fileChannel[nonAccurateCounter++ & 3].read(ByteBuffer.wrap(value, 0, SIZE_OF_VALUE), offset);;
      return value;
    } catch (IOException e) {
      Throwables.propagate(e);
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    for (FileChannel channel : fileChannel) {
      Closeables.closeQuietly(channel);
    }
  }
}
