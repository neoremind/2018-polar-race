package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.iterator;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.WalLogIterator;
import com.alibabacloud.polar_race.engine.common.neoremind.offheap.Uns;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_DIRECT;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_NOATIME;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_RDONLY;

/**
 * DirectIOWalLogByteArrayIterator
 *
 * @author xu.zx
 */
public class DirectIOWalLogByteArrayIterator implements WalLogIterator<byte[]> {
//
//  static {
//    Native.register("c");
//  }
//
//  private native int open(String pathname, int flags);
//
//  private native int read(int fd, Pointer buf, int count);
//
//  private static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);
//
//  private native int posix_memalign(PointerByReference memptr, int alignment, int size);
//
//  private native int close(int fd);
//
//  private static native int getpagesize();

  private FileChannel fileChannel;
//
//  private int fd;
//
//  private long address;

  private int offset = 0;

  private long fileSize;

  private boolean end;

  DirectRandomAccessFile fin;

  public DirectIOWalLogByteArrayIterator(String path, File file) throws FileNotFoundException {
    fileChannel = new FileInputStream(file).getChannel();
    try {
      fileSize = fileChannel.size();
//      int blockSize = blockSize((int) fileSize);
//      fd = open(path, O_NOATIME | O_RDONLY | O_DIRECT);
//      PointerByReference pntByRef = new PointerByReference();
//      posix_memalign(pntByRef, 512, blockSize);
//      Pointer bufPnt = pntByRef.getValue();
//      address = Pointer.nativeValue(bufPnt);
//      NativeLong read = pread(fd, bufPnt, new NativeLong(fileSize), new NativeLong(0));
//      System.out.println(read.longValue());
      fin = new DirectRandomAccessFile(file, "r", blockSize((int) fileSize));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
//    if (close(fd) < 0) {
//      throw new IOException("Problems occurred while doing close()");
//    }
    fin.close();
    Closeables.closeQuietly(fileChannel);
  }

  @Override
  public boolean hasNext() {
    return !end && offset < fileSize;
  }

  @Override
  public byte[] next() {
    byte[] keyAndVlogSeq = new byte[SIZE_OF_WAL_RECORD];
    try {
      fin.read(keyAndVlogSeq);
//      Uns.copyMemory(address, offset, keyAndVlogSeq, 0, SIZE_OF_WAL_RECORD);
      if (ending(keyAndVlogSeq)) {
        end = true;
        return keyAndVlogSeq;
      }
      offset += SIZE_OF_WAL_RECORD;
      return keyAndVlogSeq;
    } catch (IOException e) {
      Throwables.propagate(e);
      return null;
    }
  }

  /**
   * mmap后会多余出N个空的key和vlog seq，检查是否到了最后一个
   */
  private boolean ending(byte[] keyAndVlogSeq) {
    return offset > 0 && keyAndVlogSeq[8] == (byte) 0x00 && keyAndVlogSeq[9] == (byte) 0x00 &&
        keyAndVlogSeq[10] == (byte) 0x00 && keyAndVlogSeq[11] == (byte) 0x00;
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

  }

  static int blockSize(int cap) {
    int a = cap / 4096;
    return (a + 1) * 4096;
  }

}
