package com.alibabacloud.polar_race.engine.common.neoremind;

import java.io.IOException;
import java.util.Iterator;

/**
 * wal log reader。
 *
 * @author xu.zx
 */
public interface WalLogIterator<E> extends Iterator<E> {

  void freeUp();

  /**
   * 关闭vlog reader
   */
  void close() throws IOException;

  long getFileSize();

  int getRecordSize();

  int getActualRecordSize();
}
