package org.apache.pinot.spi.query;

import com.google.common.base.Preconditions;


public abstract class MseThreadContext {
  private static final ThreadLocal<MseThreadContext> THREAD_LOCAL = new ThreadLocal<>();
  private int _stageId = -1;
  private int _workerId = -1;


  public static MseThreadContext get() {
    return Preconditions.checkNotNull(THREAD_LOCAL.get(), "MseThreadContext is not initialized");
  }

  public static boolean isInitialized() {
    return THREAD_LOCAL.get() != null;
  }

  public static void initialize() {
    THREAD_LOCAL.set(new MseThreadContext());
  }

  public static void clear() {
    THREAD_LOCAL.remove();
  }

  public int getStageId() {
    return _stageId;
  }

  public void setStageId(int stageId) {
    Preconditions.checkState(_stageId == -1, "Stage id already set to %s, cannot set again", _stageId);
    _stageId = stageId;
  }

  public int getWorkerId() {
    return _workerId;
  }

  public void setWorkerId(int workerId) {
    Preconditions.checkState(_workerId == -1, "Worker id already set to %s, cannot set again", _workerId);
    _workerId = workerId;
  }
}
