package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.writer;

import com.alibabacloud.polar_race.engine.common.neoremind.LogWriter;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Filename;
import com.alibabacloud.polar_race.engine.common.neoremind.util.MMapUtil;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import net.smacke.jaydio.OpenFlags;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.Filename.VLOG_FILE_SUFFIX;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_CREAT;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_DIRECT;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_NOATIME;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.OpenFlags.O_RDWR;

/**
 * direct io log writer for vlog
 *
 * @author xu.zx
 */
public class JNADirectIOLogWriter implements LogWriter {

  public static final int BATCH_SIZE = 4;

  //public static final int PAGE_SIZE;

  public static final int BLOCK_SIZE = SIZE_OF_VALUE * BATCH_SIZE;

  private static NativeLong BLOCK_SIZE_NATIVE_LONG;

  static {
    Native.register("c");
    BLOCK_SIZE_NATIVE_LONG = new NativeLong(BLOCK_SIZE);
    //PAGE_SIZE = getpagesize();
  }

  private final String filePath;

  private final File file;

  private Pointer bufPnt;

  private FileChannel tmpFileChannel;

  private MappedByteBuffer mappedByteBuffer;

  private int fd;

  private long fileOffset;

  private int bufferOffset;

  public JNADirectIOLogWriter(String filePath) throws IOException {
    this.filePath = filePath;
    this.file = new File(filePath);
    fd = open(filePath, O_DIRECT | O_NOATIME | O_RDWR | O_CREAT, 00644);
    PointerByReference pntByRef = new PointerByReference();
    posix_memalign(pntByRef, BLOCK_SIZE, BLOCK_SIZE);
    bufPnt = pntByRef.getValue();
    this.tmpFileChannel = new RandomAccessFile(filePath + VLOG_FILE_SUFFIX, "rw").getChannel();
    mappedByteBuffer = tmpFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BLOCK_SIZE);
  }

  private static native int open(String pathname, int flags, int mode);

  private static native int getpagesize();

  private static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);

  private native int posix_memalign(PointerByReference memptr, int alignment, int size);

  private native int close(int fd);

  @Override
  public File getFile() {
    return file;
  }

  @Override
  public long getFileSize() throws IOException {
    return file.length();
  }

  @Override
  public void append(HeapSlice record) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void append(byte[] record) throws IOException {
    if (bufferOffset >= BLOCK_SIZE) {
      pwrite(fd, bufPnt, BLOCK_SIZE_NATIVE_LONG, new NativeLong(fileOffset));
      mappedByteBuffer.clear();
      fileOffset += BLOCK_SIZE;
      bufferOffset = 0;
    }
    mappedByteBuffer.put(record, 0, SIZE_OF_VALUE);
    bufPnt.write(bufferOffset, record, 0, SIZE_OF_VALUE);
    bufferOffset += SIZE_OF_VALUE;
  }

  @Override
  public void append(byte[] key, byte[] vlogSeq) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    // write left bytes
    if (bufferOffset > 0) {
      pwrite(fd, bufPnt, new NativeLong(bufferOffset), new NativeLong(fileOffset));
    }

    if (close(fd) < 0) {
      throw new IOException("Problems occurred while doing close()");
    }
    MMapUtil.unmap(mappedByteBuffer);
    Closeables.closeQuietly(tmpFileChannel);

    // safe deletion
    Files.delete(Paths.get(filePath + VLOG_FILE_SUFFIX));
  }
}
