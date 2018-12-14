package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.writer;

import com.alibabacloud.polar_race.engine.common.neoremind.LogWriter;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
import static java.util.Objects.requireNonNull;

/**
 * 基于offheap直接内存的log writer
 *
 * @author xu.zx
 */
public class DirectBufferFileChannelLogWriter implements LogWriter {

  private final File file;

  private final FileChannel fileChannel;

  private ByteBuffer buffer;

  public DirectBufferFileChannelLogWriter(File file) throws FileNotFoundException {
    this(file, true);
  }

  public DirectBufferFileChannelLogWriter(File file, boolean append) throws FileNotFoundException {
    requireNonNull(file, "file is null");
    this.file = file;
    this.fileChannel = new FileOutputStream(file, append).getChannel();
    buffer = ByteBuffer.allocateDirect(SIZE_OF_VALUE);
  }

  @Override
  public void close() {
    Closeables.closeQuietly(fileChannel);
  }

  @Override
  public File getFile() {
    return file;
  }

  @Override
  public long getFileSize() throws IOException {
    return fileChannel.size();
  }

  @Override
  public void append(HeapSlice record)
      throws IOException {
    record.writeBytes(0, fileChannel, record.length());
  }

  @Override
  public void append(byte[] record) throws IOException {
    buffer.clear();
    buffer.put(record);
    buffer.flip();
    fileChannel.write(buffer);
  }

  @Override
  public void append(byte[] key, byte[] vlogSeq) throws IOException {
    throw new UnsupportedOperationException();
  }
}
