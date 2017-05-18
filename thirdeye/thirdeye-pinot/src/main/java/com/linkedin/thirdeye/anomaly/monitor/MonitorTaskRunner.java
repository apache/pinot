package com.linkedin.thirdeye.anomaly.monitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

public class MonitorTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);

  private DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {

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

      // All jobs with status SCHEDULED
      Set<Long> scheduledJobIds = findAllJobsWithStatusScheduled();

      // All incomplete jobs with status SCHEDULED
      Set<Long> incompleteScheduledJobIds = findIncompleteJobsWithStatusScheduled();

      // All finished jobs with status SCHEDULED
      scheduledJobIds.removeAll(incompleteScheduledJobIds);

      if (!scheduledJobIds.isEmpty()) {
        DAO_REGISTRY.getJobDAO().updateStatusAndJobEndTimeForJobIds(scheduledJobIds, JobStatus.COMPLETED, System.currentTimeMillis());
        LOG.info("COMPLETED jobs {}", scheduledJobIds);
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
      int numAnomalyTasksDeleted = DAO_REGISTRY.getTaskDAO().deleteRecordsOlderThanDaysWithStatus(expireDaysAgo, TaskStatus.COMPLETED);
      int numAnomalyJobsDeleted = DAO_REGISTRY.getJobDAO().deleteRecordsOlderThanDaysWithStatus(expireDaysAgo, JobStatus.COMPLETED);
      // delete rows from detection status table, older than
      int numDetectionStatusRowsDeleted = DAO_REGISTRY.getDetectionStatusDAO().deleteRecordsOlderThanDays(7);
      LOG.info("Deleted {} anomaly jobs and {} anomaly tasks", numAnomalyJobsDeleted, numAnomalyTasksDeleted);
      LOG.info("Deleted {} rows from detectionStatus table", numDetectionStatusRowsDeleted);
    } catch (Exception e) {
      LOG.error("Exception in monitor expire task", e);
    }
  }


  private Set<Long> findAllJobsWithStatusScheduled() {
    Set<Long> scheduledJobIds = new HashSet<>();
    try {
      List<JobDTO> scheduledJobs = DAO_REGISTRY.getJobDAO().findByStatus(JobStatus.SCHEDULED);
      for (JobDTO job : scheduledJobs) {
        scheduledJobIds.add(job.getId());
      }
    } catch (Exception e) {
      LOG.error("Exception in finding jobs with status scheduled", e);
    }
    return scheduledJobIds;
  }

  private Set<Long> findIncompleteJobsWithStatusScheduled() {
    Set<Long> incompleteScheduledJobIds = new HashSet<>();
    List<TaskDTO> incompleteTasks = DAO_REGISTRY.getTaskDAO().findByStatusNotIn(TaskStatus.COMPLETED);
    for (TaskDTO task : incompleteTasks) {
      incompleteScheduledJobIds.add(task.getJobId());
    }
    return incompleteScheduledJobIds;
  }
}
