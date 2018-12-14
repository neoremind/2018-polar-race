package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.writer;

import com.alibabacloud.polar_race.engine.common.neoremind.LogWriter;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MMapUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
import static java.util.Objects.requireNonNull;

/**
 * 基于mmap的log writer
 *
 * @author xu.zx
 */
public class MmapLogWriter implements LogWriter {

  public static final int PAGE_SIZE = SIZE_OF_WAL_RECORD << 16;

  private final File file;

  private FileChannel fileChannel;

  private MappedByteBuffer mappedByteBuffer;

  private long fileOffset;

  public MmapLogWriter(File file) throws IOException {
    requireNonNull(file, "file is null");
    this.file = file;
    this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
    //TODO append position should be file size
    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
  }

  @Override
  public void close() throws IOException {
    fileOffset += mappedByteBuffer.position();
    MMapUtil.unmap(mappedByteBuffer);
    if (fileChannel.isOpen()) {
      fileChannel.truncate(fileOffset);
    }
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
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
    ensureCapacity(record.length());
    mappedByteBuffer.put(record.getRawArray(), 0, record.length());
  }

  @Override
  public void append(byte[] record) throws IOException {
    ensureCapacity(record.length);
    mappedByteBuffer.put(record, 0, record.length);
  }

  @Override
  public void append(byte[] key, byte[] vlogSeq) throws IOException {
    ensureCapacity(SIZE_OF_WAL_RECORD);
    mappedByteBuffer.put(key, 0, key.length);
    mappedByteBuffer.put(vlogSeq, 0, vlogSeq.length);
  }

  private void ensureCapacity(int bytes) throws IOException {
    if (mappedByteBuffer.remaining() < bytes) {
      fileOffset += mappedByteBuffer.position();
      MMapUtil.unmap(mappedByteBuffer);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
    }
  }

}
