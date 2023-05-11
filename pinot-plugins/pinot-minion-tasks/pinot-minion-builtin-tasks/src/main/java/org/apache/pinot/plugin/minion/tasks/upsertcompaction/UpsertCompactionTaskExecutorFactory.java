package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.spi.annotations.minion.TaskExecutorFactory;


@TaskExecutorFactory
public class UpsertCompactionTaskExecutorFactory implements PinotTaskExecutorFactory {

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager) {
  }

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager, MinionConf minionConf) {
  }

  @Override
  public String getTaskType() { return MinionConstants.UpsertCompactionTask.TASK_TYPE; }

  @Override
  public PinotTaskExecutor create() { return new UpsertCompactionTaskExecutor(); }
}
