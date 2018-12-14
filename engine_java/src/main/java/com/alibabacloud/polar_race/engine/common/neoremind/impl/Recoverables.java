package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.util.Filename;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 在写入阶段对于异常的退出，例如kill- 9，没有close db，需要回复临时的vlog，因为tmp vlog没write through，都在page cache里面，
 * 靠os去做sync，所以再次启动数据库的时候，需要恢复数据。
 *
 * @author xu.zx
 */
public class Recoverables {

  public static final int BUFFER_SIZE = SIZE_OF_VALUE * 4;

  public static void recoverVLog(String path, int shardId) {
    File vlogTmpFile = new File(path, Filename.vlogTmpFileName(shardId));
    if (!vlogTmpFile.exists()) {
      return;
    }
    try (FileChannel vlogTmpChannel = new FileInputStream(vlogTmpFile).getChannel();
         FileChannel vLogWriter = new FileOutputStream(new File(path, Filename.vlogFileName(shardId)), true).getChannel()) {
      ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
      while (vlogTmpChannel.read(buffer) > 0) {
        buffer.flip();
        vLogWriter.write(buffer);
        buffer.clear();
      }

      // safe deletion
      vlogTmpFile.delete();
    } catch (IOException e) {
      e.printStackTrace();
      Throwables.propagate(e);
    }
  }

}
