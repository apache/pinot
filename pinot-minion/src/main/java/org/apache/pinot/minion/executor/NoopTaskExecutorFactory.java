package org.apache.pinot.minion.executor;

import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.annotations.minion.TaskExecutorFactory;

@TaskExecutorFactory
public class NoopTaskExecutorFactory implements PinotTaskExecutorFactory{

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager) {
  }

  @Override
  public String getTaskType() {
    return MinionConstants.NoopTask.TASK_TYPE;
  }

  @Override
  public PinotTaskExecutor create() {
    return new NoopTaskExecutor();
  }
}
