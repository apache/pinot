package org.apache.pinot.minion.executor;

import org.apache.pinot.core.minion.PinotTaskConfig;

public class NoopTaskExecutor implements PinotTaskExecutor{
  protected boolean _cancelled = false;

  @Override
  public void cancel() {
    _cancelled = true;
  }

  @Override
  public String executeTask (PinotTaskConfig pinotTaskConfig) {
    return "";
  }
}
