package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.iterator;

import com.alibabacloud.polar_race.engine.common.neoremind.WalLogIterator;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MMapUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Consumer;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;

/**
 * MmapWalLogByteArrayIterator
 *
 * @author xu.zx
 */
public class MmapWalLogByteArrayIterator implements WalLogIterator<byte[]> {

  //TODO should set to 0
  public static final int CHECK_END_BEGINNING_INDEX = 50000;

  private FileChannel fileChannel;

  private MappedByteBuffer mappedByteBuffer = null;

  private int offset = 0;

  private long fileSize;

  private boolean end;

  private boolean checkEnd;

  public MmapWalLogByteArrayIterator(File file) throws FileNotFoundException {
    this(file, true);
  }

  public MmapWalLogByteArrayIterator(File file, boolean checkEnd) throws FileNotFoundException {
    this.checkEnd = checkEnd;
    fileChannel = new FileInputStream(file).getChannel();
    try {
      fileSize = fileChannel.size();
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
      mappedByteBuffer.order(ByteOrder.BIG_ENDIAN);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(fileChannel);
  }

  @Override
  public boolean hasNext() {
    return !end && offset < fileSize;
  }

  @Override
  public byte[] next() {
    byte[] keyAndVlogSeq = new byte[SIZE_OF_WAL_RECORD];
    mappedByteBuffer.get(keyAndVlogSeq, 0, SIZE_OF_WAL_RECORD);
    if (checkEnd && ending(keyAndVlogSeq)) {
      end = true;
      return keyAndVlogSeq;
    }
    offset += SIZE_OF_WAL_RECORD;
    return keyAndVlogSeq;
  }

  /**
   * mmap后会多余出N个空的key和vlog seq，检查是否到了最后一个
   */
  private boolean ending(byte[] keyAndVlogSeq) {
    return offset > CHECK_END_BEGINNING_INDEX && keyAndVlogSeq[8] == (byte) 0x00 && keyAndVlogSeq[9] == (byte) 0x00 &&
        keyAndVlogSeq[10] == (byte) 0x00 && keyAndVlogSeq[11] == (byte) 0x00;
  }

  public void foreach(Consumer<MappedByteBuffer> func) {
    while (offset < fileSize) {
      func.accept(mappedByteBuffer);
      offset += SIZE_OF_WAL_RECORD;
    }
  }

  @Override
  public long getFileSize() {
    return fileSize;
  }

  @Override
  public int getRecordSize() {
    return (int) (fileSize / SIZE_OF_WAL_RECORD);
  }

  @Override
  public int getActualRecordSize() {
    return offset / SIZE_OF_WAL_RECORD;
  }

  @Override
  public void freeUp() {
    if (mappedByteBuffer != null) {
      MMapUtil.unmap(mappedByteBuffer);
      mappedByteBuffer = null;
    }
  }
}
