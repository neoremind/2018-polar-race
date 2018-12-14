package com.alibabacloud.polar_race.engine.common.neoremind.util;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_INT;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * byte array工具类。
 *
 * @author xu.zx
 */
public class BytewiseUtil {

  private final static char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  public static final byte[] SMALLEST_KEY = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};

  public static final byte[] LARGEST_KEY = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

  public static String bytesToHex(byte[] bytes) {
    checkArgument(bytes != null, "can not convert bytes to hex string since bytes is null");
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  public static byte[] hexStringToByteArray(String s) {
    s = s.replace(" ", "");
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
          + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static byte[] toBEIntByteArray(int value) {
    byte[] data = new byte[SIZE_OF_INT];
    data[0] = (byte) (value);
    data[1] = (byte) (value >>> 8);
    data[2] = (byte) (value >>> 16);
    data[3] = (byte) (value >>> 24);
    return data;
  }

  public static void setBEInt(byte[] data, int index, int value) {
    data[index] = (byte) (value);
    data[index + 1] = (byte) (value >>> 8);
    data[index + 2] = (byte) (value >>> 16);
    data[index + 3] = (byte) (value >>> 24);
  }

  public static int fromBEBytes(byte b1, byte b2, byte b3, byte b4) {
    return b4 << 24 | (b3 & 255) << 16 | (b2 & 255) << 8 | b1 & 255;
  }

  public static long fromBytes(byte[] b) {
    return ((long) b[0] & 255L) << 56 | ((long) b[1] & 255L) << 48 | ((long) b[2] & 255L) << 40 | ((long) b[3] & 255L) << 32
        | ((long) b[4] & 255L) << 24 | ((long) b[5] & 255L) << 16 | ((long) b[6] & 255L) << 8 | (long) b[7] & 255L;
  }

  public static byte[] getNonNullLowerKey(byte[] lower) {
    if (lower == null) {
      return SMALLEST_KEY;
    } else {
      return lower;
    }
  }

  public static byte[] getNonNullUpperKey(byte[] upper) {
    if (upper == null) {
      return LARGEST_KEY;
    } else {
      return upper;
    }
  }
}
