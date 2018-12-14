package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.neoremind.DB;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.ShardDBImpl;

import java.io.IOException;

public class EngineRace extends AbstractEngine {

  private DB db;

  @Override
  public void open(String path) throws EngineException {
    try {
      db = new ShardDBImpl(path, 1024);
    } catch (IOException e) {
      e.printStackTrace();
      throw new EngineException(RetCodeEnum.IO_ERROR, "init db failed");
    }
  }

  @Override
  public void write(byte[] key, byte[] value) throws EngineException {
    try {
      db.write(key, value);
    } catch (Exception e) {
      e.printStackTrace();
      throw new EngineException(RetCodeEnum.IO_ERROR, "write failed");
    }
  }

  @Override
  public byte[] read(byte[] key) throws EngineException {
    try {
      return db.read(key);
    } catch (Exception e) {
      e.printStackTrace();
      throw new EngineException(RetCodeEnum.IO_ERROR, "read failed");
    }
  }

  @Override
  public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    try {
      db.range(lower, upper, visitor);
    } catch (Exception e) {
      e.printStackTrace();
      throw new EngineException(RetCodeEnum.IO_ERROR, "range failed");
    }
  }

  @Override
  public void close() {
    try {
      db.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
