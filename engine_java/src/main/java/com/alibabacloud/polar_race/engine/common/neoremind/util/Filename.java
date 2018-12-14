package com.alibabacloud.polar_race.engine.common.neoremind.util;

import static java.util.Objects.requireNonNull;

/**
 * db用到的文件名生成器。
 *
 * @author xu.zx
 */
public final class Filename {

  public static final String SEPARATOR = "-";

  /**
   * 返回sst文件名称。
   *
   * @param shardId    shardId
   * @param fileNumber fileNumber
   * @return 文件名
   */
  public static String tableFileName(int shardId, int fileNumber) {
    return makeFileName(shardId, fileNumber, "sst");
  }

  /**
   * 存储sst文件信息的索引目录。
   *
   * @param shardId shardId
   * @return 文件名
   */
  public static String manifestFileName(int shardId) {
    return String.format("manifest-%d", shardId);
  }

  /**
   * write ahead log文件。
   *
   * @param shardId shardId
   * @return 文件名
   */
  public static String walFileName(int shardId) {
    return String.format("wal-%d", shardId);
  }

  /**
   * sorted write ahead log。
   *
   * @param shardId shardId
   * @return 文件名
   */
  public static String sortedWalFileName(int shardId) {
    return String.format("wal-%d.sort", shardId);
  }

  /**
   * value log。
   *
   * @param shardId shardId
   * @return 文件名
   */
  public static String vlogFileName(int shardId) {
    return String.format("vlog-%d", shardId);
  }

  public static final String VLOG_FILE_SUFFIX = ".tmp";

  /**
   * vlog用于临时直接反复擦写page cache的mmap缓冲文件
   */
  public static String vlogTmpFileName(int shardId) {
    return String.format("vlog-%d" + VLOG_FILE_SUFFIX, shardId);
  }

  /**
   * 返回数字的file number和后缀的文件名
   *
   * @param shardId    shard id
   * @param fileNumber file number
   * @param suffix     后缀
   * @return 文件名
   */
  private static String makeFileName(int shardId, int fileNumber, String suffix) {
    requireNonNull(suffix, "suffix is null");
    return String.format("%d%s%02d.%s", shardId, SEPARATOR, fileNumber, suffix);
  }
}
