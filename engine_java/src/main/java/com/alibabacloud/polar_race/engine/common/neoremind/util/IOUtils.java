package com.alibabacloud.polar_race.engine.common.neoremind.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

/**
 * IOUtils
 *
 * @author xu.zx
 */
public class IOUtils {

  public static int writeDirectBytes(byte[] data, GatheringByteChannel out) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
    buffer.put(data);
    buffer.flip();
    int writeBytes = out.write(buffer);
    return writeBytes;
  }

  public static int writeBytes(byte[] data, GatheringByteChannel out) throws IOException {
    return out.write(ByteBuffer.wrap(data, 0, data.length));
  }

  public static void closeQuietly(Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException ignored) {
      // omit
    }
  }

  public static int readBytes(byte[] data, FileChannel in, long position, int length)
      throws IOException {
    //checkPositionIndexes(index, index + length, this.length);
    //ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
//    int readBytes = 0;

//    System.out.println(position);
    return in.read(ByteBuffer.wrap(data, 0, length), position);
//    do {
//      int localReadBytes = in.read(buf, position);
//      if (localReadBytes < 0) {
//        if (readBytes == 0) {
//          return -1;
//        } else {
//          break;
//        }
//      } else if (localReadBytes == 0) {
//        break;
//      }
//      readBytes += localReadBytes;
//    } while (readBytes < length);
//
//    return readBytes;
  }
}
