package com.alibabacloud.polar_race.engine.common.neoremind.util;

import java.lang.reflect.Field;
import java.nio.Buffer;

/**
 * 获取mmap内存地址的工具类
 *
 * @author xu.zx
 */
public class MoreMappedByteBuffer {

  private static MoreMappedByteBuffer instance = new MoreMappedByteBuffer();

  private Field field;

  private MoreMappedByteBuffer() {
    try {
      this.field = Buffer.class.getDeclaredField("address");
      field.setAccessible(true);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
  }

  public static MoreMappedByteBuffer getInstance() {
    return instance;
  }

  public long getAddress(Buffer mappedByteBuffer) {
    try {
      return (long) (field.get(mappedByteBuffer));
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
