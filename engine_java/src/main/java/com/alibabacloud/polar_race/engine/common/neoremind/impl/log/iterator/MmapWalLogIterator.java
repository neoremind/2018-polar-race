package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.iterator;

import com.alibabacloud.polar_race.engine.common.neoremind.WalEntry;
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

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
import static com.google.common.base.Preconditions.checkState;

/**
 * MmapWalLogIterator
 *
 * @author xu.zx
 */
public class MmapWalLogIterator implements WalLogIterator<WalEntry> {

  private FileChannel fileChannel;

  private MappedByteBuffer data = null;

  private int offset = 0;

  private long fileSize;

  public MmapWalLogIterator(File file) throws FileNotFoundException {
    fileChannel = new FileInputStream(file).getChannel();
    try {
      fileSize = fileChannel.size();
      checkState(fileSize % SIZE_OF_WAL_RECORD == 0, "wal is corrupt");
      data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
      data.order(ByteOrder.BIG_ENDIAN);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    MMapUtil.unmap(data);
    data = null;
    Closeables.closeQuietly(fileChannel);
    fileChannel = null;
  }

  @Override
  public boolean hasNext() {
    return offset < fileSize;
  }

  @Override
  public WalEntry next() {
    byte[] key = new byte[SIZE_OF_KEY];
    data.get(key, 0, SIZE_OF_KEY);
    // BE
    int vlogSeq = data.getInt();
    offset += SIZE_OF_WAL_RECORD;
    return new WalEntry(key, vlogSeq);
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
