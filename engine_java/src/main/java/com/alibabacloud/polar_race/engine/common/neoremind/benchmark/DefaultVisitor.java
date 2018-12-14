package com.alibabacloud.polar_race.engine.common.neoremind.benchmark;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.util.concurrent.atomic.AtomicLong;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil.bytesToHex;

public class DefaultVisitor extends AbstractVisitor {

  public AtomicLong visitCount;

  byte[] beforeKey = null;

  byte[] beforeValue = null;

  public DefaultVisitor() {
    this.visitCount = new AtomicLong();
  }

  public void visit(byte[] key, byte[] value) {
    if (value == null || value.length != SIZE_OF_VALUE) {
      System.out.println("value null or length invalid ");
      System.exit(-1);
    }
    if (beforeKey == null) {
      beforeKey = key;
      if (!verify(key, value)) {
        System.out.println(visitCount.get() + "= value not match, no prekey, key = " + bytesToHex(key) +
            ", pre=" + bytesToHex(beforeKey) + ", value= " + bytesToHex(value));
        System.exit(-1);
      }
    } else {
      if (compareTo(beforeKey, key) > 0) {
        System.out.println(visitCount.get() + "= not in order, key = " + bytesToHex(key) +
            ", pre=" + bytesToHex(beforeKey) + ", value= " + bytesToHex(value));
        System.exit(-1);
      } else {
        if (!verify(key, value)) {
          System.out.println(visitCount.get() + "= value not match, key = " + bytesToHex(key) +
              ", pre=" + bytesToHex(beforeKey) + ", value= " + bytesToHex(value) + ", beforeValue=" +
              beforeValue);
          System.exit(-1);
        }
        beforeKey = key;
        beforeValue = value;
      }
    }
    visitCount.incrementAndGet();
  }

  public int compareTo(byte[] a, byte[] b) {
    for (int i = 0; i < SIZE_OF_KEY; i++) {
      int thisByte = 0xFF & a[i];
      int thatByte = 0xFF & b[i];
      if (thisByte != thatByte) {
        return (thisByte) - (thatByte);
      }
    }
    return a.length - b.length;
  }

  public boolean verify(byte[] key, byte[] value) {
    for (int i = 0; i < key.length; i++) {
      if (key[i] != value[i]) {
        return false;
      }
    }
    return true;
  }

  public void resetBeforeKey() {
    this.beforeKey = null;
  }

  public AtomicLong getVisitCount() {
    return visitCount;
  }

}
