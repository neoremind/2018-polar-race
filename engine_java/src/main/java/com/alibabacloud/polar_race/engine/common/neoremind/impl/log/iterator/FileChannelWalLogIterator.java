package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.iterator;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.WalEntry;
import com.alibabacloud.polar_race.engine.common.neoremind.WalLogIterator;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_INT;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
import static com.google.common.base.Preconditions.checkState;

/**
 * FileChannelWalLogIterator
 *
 * @author xu.zx
 */
public class FileChannelWalLogIterator implements WalLogIterator<WalEntry> {

  private FileChannel fileChannel;

  private int offset = 0;

  private long fileSize;

  private HeapSlice slice;

  public FileChannelWalLogIterator(File file) throws FileNotFoundException {
    fileChannel = new FileInputStream(file).getChannel();
    try {
      fileSize = fileChannel.size();
      checkState(fileSize % SIZE_OF_WAL_RECORD == 0, "wal is corrupt");
      slice = HeapSlice.allocate((int) fileSize);
      try {
        slice.readBytes(0, fileChannel, 0, slice.length());
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    slice = null;
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  @Override
  public boolean hasNext() {
    return offset < fileSize;
  }

  @Override
  public WalEntry next() {
    byte[] key = slice.getBytes(offset, SIZE_OF_KEY);
    // BE
    int vlogSeq = fromBytes(slice.getBytes(offset + SIZE_OF_KEY, SIZE_OF_INT));
    offset += SIZE_OF_WAL_RECORD;
    return new WalEntry(key, vlogSeq);
  }

  public static int fromBytes(byte[] a) {
    return a[0] << 24 | (a[1] & 255) << 16 | (a[2] & 255) << 8 | a[3] & 255;
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

  }
}
