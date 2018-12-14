package com.alibabacloud.polar_race.engine.common.neoremind.util;

/**
 * TaskStatus，在{@link AsyncExecutor}中可以使用。
 *
 * @author xu.zx
 */
public class TaskStatus {

  public static TaskStatus SUCCESS = TaskStatus.create();

  public static TaskStatus FAIL = TaskStatus.create().markFail(null);

  private boolean success = true;

  private Throwable t;

  public static TaskStatus create() {
    return new TaskStatus();
  }

  public boolean success() {
    return success;
  }

  public TaskStatus markSuccess() {
    this.success = true;
    return this;
  }

  public TaskStatus markFail(Throwable t) {
    this.t = t;
    this.success = false;
    return this;
  }

  public Throwable getThrowable() {
    return t;
  }
}
