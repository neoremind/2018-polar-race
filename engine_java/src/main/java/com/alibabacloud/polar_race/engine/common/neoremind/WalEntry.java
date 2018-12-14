package com.alibabacloud.polar_race.engine.common.neoremind;

import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;

import java.util.Map.Entry;

/**
 * key，value组合。
 *
 * @author xu.zx
 */
public class WalEntry implements Entry<byte[], Integer> {

  private final byte[] key;

  private final Integer value;

  public WalEntry(byte[] key, Integer value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public byte[] getKey() {
    return key;
  }

  @Override
  public Integer getValue() {
    return value;
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public final Integer setValue(Integer value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WalEntry entry = (WalEntry) o;

    if (!key.equals(entry.key)) {
      return false;
    }
    if (!value.equals(entry.value)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("WalEntry");
    sb.append("{key=").append(BytewiseUtil.bytesToHex(key));
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}
