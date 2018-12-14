package com.alibabacloud.polar_race.engine.common.neoremind.benchmark;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.util.concurrent.Callable;

public class EngineRanger implements Callable<Integer> {

  public AbstractEngine engine;

  public AbstractVisitor visitor;

  private int round = 2;

  public EngineRanger(AbstractEngine engine, AbstractVisitor visitor, int round) {
    this.engine = engine;
    this.visitor = visitor;
    this.round = round;
  }

  public Integer call() throws Exception {
    for (int i = 0; i < round; i++) {
      engine.range(null, null, visitor);
      ((DefaultVisitor) visitor).resetBeforeKey();
    }
    return 1;
  }

}
