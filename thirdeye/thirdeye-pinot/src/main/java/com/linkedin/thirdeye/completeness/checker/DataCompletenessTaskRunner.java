package com.linkedin.thirdeye.completeness.checker;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;

public class DataCompletenessTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessTaskRunner.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private JobManager jobDAO;
  private TaskManager taskDAO;

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {

    jobDAO = DAO_REGISTRY.getJobDAO();
    taskDAO = DAO_REGISTRY.getTaskDAO();

    DataCompletenessTaskInfo dataCompletenessTaskInfo = (DataCompletenessTaskInfo) taskInfo;
    DataCompletenessType dataCompletenessType = dataCompletenessTaskInfo.getDataCompletenessType();
    if (dataCompletenessType.equals(DataCompletenessType.CHECKER)) {
      executeCheckerTask(dataCompletenessTaskInfo);
    } else if (dataCompletenessType.equals(DataCompletenessType.CLEANUP)) {
      executeCleanupTask(dataCompletenessTaskInfo);
    } else {
      throw new UnsupportedOperationException("DataCompleteness task must be of type CHECKER/CLEANUP, found "
          + dataCompletenessType);
    }
    return null;
  }

  private void executeCheckerTask(DataCompletenessTaskInfo dataCompletenessTaskInfo) {
    LOG.info("Execute data completeness checker task {}", dataCompletenessTaskInfo);
    try {

    } catch (Exception e) {
      LOG.error("Exception in data completeness checker task", e);
    }
  }

  private void executeCleanupTask(DataCompletenessTaskInfo dataCompletenessTaskInfo) {
    LOG.info("Execute data completeness cleanup {}", dataCompletenessTaskInfo);
    try {

    } catch (Exception e) {
      LOG.error("Exception data completeness cleanup task", e);
    }
  }
}
