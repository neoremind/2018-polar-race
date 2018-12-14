package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.google.common.base.Preconditions;

import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;

import java.util.Iterator;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;

/**
 * @author xu.zx
 */
public class SortedKeyAndVLogSeqs {

  private int index = 0;

  private byte[][] keys;

  private int[] vlogSequences;

  public SortedKeyAndVLogSeqs(int size) {
    keys = new byte[size][];
    vlogSequences = new int[size];
  }

  public void add(byte[] keyAndVlogSeq) {
    byte[] key = new byte[SIZE_OF_KEY];
    System.arraycopy(keyAndVlogSeq, 0, key, 0, SIZE_OF_KEY);
    keys[index] = key;
    vlogSequences[index] = BytewiseUtil.fromBEBytes(keyAndVlogSeq[8], keyAndVlogSeq[9], keyAndVlogSeq[10], keyAndVlogSeq[11]);
    index++;
  }

  public int getIndex() {
    return index;
  }

  public byte[][] getKeys() {
    return keys;
  }

  public int[] getVlogSequences() {
    return vlogSequences;
  }

  public void destroy() {
    keys = null;
    vlogSequences = null;
  }
}
