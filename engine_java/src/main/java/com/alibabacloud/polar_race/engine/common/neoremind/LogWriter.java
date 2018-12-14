package com.alibabacloud.polar_race.engine.common.neoremind;

import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;

import java.io.File;
import java.io.IOException;

/**
 * 通用的log writer。
 *
 * @author xu.zx
 */
public interface LogWriter {

  /**
   * 获取log file句柄
   *
   * @return file
   */
  File getFile();

  /**
   * 获取file size，可能会进行系统调用，少用
   *
   * @return file size
   * @throws IOException 抛出异常
   */
  long getFileSize() throws IOException;

  /**
   * 追加一条记录
   *
   * @param record 记录slice
   * @throws IOException 抛出异常
   */
  void append(HeapSlice record) throws IOException;

  /**
   * 追加一条记录
   *
   * @param record 记录slice
   * @throws IOException 抛出异常
   */
  void append(byte[] record) throws IOException;

  /**
   * 追加一条记录
   *
   * @param key     key
   * @param vlogSeq vlogSeq
   * @throws IOException 抛出异常
   */
  void append(byte[] key, byte[] vlogSeq) throws IOException;

  /**
   * 关闭
   *
   * @throws IOException 抛出异常
   */
  void close() throws IOException;

}
