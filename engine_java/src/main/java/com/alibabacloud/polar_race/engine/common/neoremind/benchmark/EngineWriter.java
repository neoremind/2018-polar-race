package com.alibabacloud.polar_race.engine.common.neoremind.benchmark;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.Random;
import java.util.concurrent.Callable;

public class EngineWriter implements Callable<Integer> {

  public int writeCount;

  public AbstractEngine engine;

  public byte[] valueBuffer;

  public int valueLen = 4 * 1024;

  public int keyLen = 4;

  public int keyRange = 0;

  public long orginSeed = 0;

  public EngineWriter(int writeCount, AbstractEngine engine, int valueLen, int keyLen, int keyRange, long seed) {
    this.writeCount = writeCount;
    this.engine = engine;
    this.valueLen = valueLen;
    this.keyLen = keyLen;
    this.keyRange = keyRange;
    this.orginSeed = seed;
    valueBuffer = BenchMarkUtil.getRandomValue();
  }

  public Integer call() throws Exception {
    byte[] key = null;
    byte[] value = new byte[valueLen];
    Random random = new Random(this.orginSeed);
    for (int i = 0; i < this.writeCount; i++) {
      key = BenchMarkUtil.getRandomKey(keyLen, random);
      System.arraycopy(valueBuffer, i, value, 0, valueLen);
      System.arraycopy(key, 0, value, 0, keyLen);
      try {
        this.engine.write(key, value);
      } catch (EngineException e) {
        e.printStackTrace();
        throw e;
      }
    }
    return 1;
  }

}
