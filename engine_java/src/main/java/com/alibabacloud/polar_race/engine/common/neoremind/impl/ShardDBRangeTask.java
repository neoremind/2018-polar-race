package com.alibabacloud.polar_race.engine.common.neoremind.impl;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.neoremind.util.AccumulativeRunner;

/**
 * ShardDBRangeTask
 *
 * @author xu.zx
 */
public class ShardDBRangeTask extends AccumulativeRunner.Task {

  byte[] lowerKey;

  byte[] upperKey;

  AbstractVisitor visitor;

  public ShardDBRangeTask(byte[] lowerKey, byte[] upperKey, AbstractVisitor visitor) {
    this.lowerKey = lowerKey;
    this.upperKey = upperKey;
    this.visitor = visitor;
  }

  public AbstractVisitor getVisitor() {
    return visitor;
  }

  public void setVisitor(AbstractVisitor visitor) {
    this.visitor = visitor;
  }

  public byte[] getLowerKey() {
    return lowerKey;
  }

  public void setLowerKey(byte[] lowerKey) {
    this.lowerKey = lowerKey;
  }

  public byte[] getUpperKey() {
    return upperKey;
  }

  public void setUpperKey(byte[] upperKey) {
    this.upperKey = upperKey;
  }
}
