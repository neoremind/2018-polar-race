package com.alibabacloud.polar_race.engine.common.neoremind.util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * FileUtils
 *
 * @author xu.zx
 */
public final class FileUtils {

  public static ImmutableList<File> listFiles(File dir, FilenameFilter filter) {
    File[] files = dir.listFiles(filter);
    if (files == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(files);
  }

  /**
   * 写少量数据
   *
   * @param file  file
   * @param bytes bytes
   * @param sync  是否同步刷盘
   */
  public static void writeContent(File file, byte[] bytes, boolean sync) {
    try (FileOutputStream fis = new FileOutputStream(file);
         FileChannel channel = fis.getChannel()) {
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      if (sync) {
        channel.force(false);
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * 清理文件夹下有关db的文件
   *
   * @param path 路径
   */
  public static void clearAllFiles(String path) {
    ImmutableList<File> sstFiles = FileUtils.listFiles(new File(path), new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("LOCK") || name.startsWith("vlog") || name.endsWith("sst") || name.startsWith("wal")
            || name.startsWith("range") || name.startsWith("manifest") || name.startsWith("read");
      }
    });
    for (File sstFile : sstFiles) {
      sstFile.delete();
    }
  }

  /**
   * 清理文件夹下有关vlog的文件
   *
   * @param path    路径
   * @param shardId 分片号
   */
  public static void clearVlogFile(String path, int shardId) {
    ImmutableList<File> vlogFiles = FileUtils.listFiles(new File(path), new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals("vlog-" + shardId);
      }
    });
    long start = System.currentTimeMillis();
    for (File vlogFile : vlogFiles) {
      vlogFile.delete();
    }
    System.out.println(shardId + " delete vlog using " + (System.currentTimeMillis() - start) + "ms");
  }
}
