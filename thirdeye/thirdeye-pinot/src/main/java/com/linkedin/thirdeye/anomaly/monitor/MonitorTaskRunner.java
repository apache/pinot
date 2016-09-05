package com.linkedin.thirdeye.anomaly.monitor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;

public class MonitorTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);

  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {

    anomalyJobDAO = taskContext.getAnomalyJobDAO();
    anomalyTaskDAO = taskContext.getAnomalyTaskDAO();

    MonitorTaskInfo monitorTaskInfo = (MonitorTaskInfo) taskInfo;
    MonitorType monitorType = monitorTaskInfo.getMonitorType();
    if (monitorType.equals(MonitorType.UPDATE)) {
      executeMonitorUpdate(monitorTaskInfo);
    } else if (monitorType.equals(MonitorType.EXPIRE)) {
      executeMonitorExpire(monitorTaskInfo);
    } else {
      throw new UnsupportedOperationException("Monitor task must be of type UPDATE/EXPIRE, found " + monitorType);
    }
    return null;
  }

  private void executeMonitorUpdate(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor udpate {}", monitorTaskInfo);
    try {

      Long jobExecutionId = monitorTaskInfo.getJobExecutionId();
      List<TaskDTO> anomalyTaskSpecs = anomalyTaskDAO
          .findByJobIdStatusNotIn(jobExecutionId, TaskStatus.COMPLETED);
      if (anomalyTaskSpecs.isEmpty()) {
        anomalyJobDAO.updateStatusAndJobEndTime(jobExecutionId, JobStatus.COMPLETED, System.currentTimeMillis());
        LOG.info("COMPLETED anomaly job {}", jobExecutionId);
      }
    } catch (Exception e) {
      LOG.error("Exception in monitor update task", e);
    }
  }

  private void executeMonitorExpire(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor expire {}", monitorTaskInfo);
    try {
      int expireDaysAgo = monitorTaskInfo.getExpireDaysAgo();
      // fist delete tasks then jobs, as task has a foreign key
      int numAnomalyTasksDeleted = anomalyTaskDAO.deleteRecordsOlderThanDaysWithStatus(expireDaysAgo, TaskStatus.COMPLETED);
      int numAnomalyJobsDeleted = anomalyJobDAO.deleteRecordsOlderThanDaysWithStatus(expireDaysAgo, JobStatus.COMPLETED);
      LOG.info("Deleted {} anomaly jobs and {} anomaly tasks", numAnomalyJobsDeleted, numAnomalyTasksDeleted);
    } catch (Exception e) {
      LOG.error("Exception in monitor expire task", e);
    }
  }

}
