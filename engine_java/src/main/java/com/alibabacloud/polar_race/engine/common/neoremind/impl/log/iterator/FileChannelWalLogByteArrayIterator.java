package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.iterator;

import com.alibabacloud.polar_race.engine.common.neoremind.WalLogIterator;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
import static com.google.common.base.Preconditions.checkState;

/**
 * MmapWalLogByteArrayIterator
 *
 * @author xu.zx
 */
public class FileChannelWalLogByteArrayIterator implements WalLogIterator<byte[]> {

  private FileChannel fileChannel;

  private ByteBuffer byteBuffer;

  private int offset = 0;

  private long fileSize;

  public FileChannelWalLogByteArrayIterator(File file) throws FileNotFoundException {
    fileChannel = new FileInputStream(file).getChannel();
    try {
      fileSize = fileChannel.size();
      checkState(fileSize % SIZE_OF_WAL_RECORD == 0, "wal is corrupt");
      byteBuffer = ByteBuffer.allocate((int) fileSize);
      fileChannel.read(byteBuffer);
      byteBuffer.flip();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  @Override
  public boolean hasNext() {
    return offset < fileSize;
  }

  @Override
  public byte[] next() {
    byte[] keyAndVlogSeq = new byte[SIZE_OF_WAL_RECORD];
    byteBuffer.get(keyAndVlogSeq, 0, SIZE_OF_WAL_RECORD);
    // BE
    //int vlogSeq = data.getInt();
    offset += SIZE_OF_WAL_RECORD;
    return keyAndVlogSeq;
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
    return offset;
  }

  @Override
  public void freeUp() {
    byteBuffer = null;
  }
}
