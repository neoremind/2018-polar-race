package com.alibabacloud.polar_race.engine.common.neoremind;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * value log reader。
 *
 * @author xu.zx
 */
public interface VLogReader {

  void init() throws FileNotFoundException;

  /**
   * 根据offset读取一个记录
   *
   * @param offset 文件内的offset
   * @return 记录slice
   */
  byte[] readValue(long offset);

  /**
   * 关闭vlog reader
   */
  void close() throws IOException;

}
