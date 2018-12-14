package com.alibabacloud.polar_race.engine.common.neoremind;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.io.Closeable;

/**
 * DB的接口定义
 *
 * @author xu.zx
 */
public interface DB extends Closeable {

  /**
   * 写一个kv
   *
   * @param key   key
   * @param value value
   */
  void write(byte[] key, byte[] value);

  /**
   * 根据某个key读value
   *
   * @param key key
   * @return value
   */
  byte[] read(byte[] key);

  /**
   * 遍历某个key范围
   *
   * @param lower   最小key
   * @param upper   最大key
   * @param visitor 迭代访问器
   */
  void range(byte[] lower, byte[] upper, AbstractVisitor visitor);

}
