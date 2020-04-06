/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.anomaly.monitor;

import org.apache.pinot.thirdeye.anomaly.job.JobConstants.JobStatus;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.detection.health.DetectionHealth;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);
  private static final long MAX_TASK_TIME = TimeUnit.HOURS.toMillis(6);

  private DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) {

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
    LOG.info("Execute monitor update {}", monitorTaskInfo);
    int jobRetentionDays = monitorTaskInfo.getDefaultRetentionDays();
    try {
      // Mark expired tasks with RUNNING states as TIMEOUT
      List<TaskDTO> timeoutTasks = DAO_REGISTRY.getTaskDAO().findTimeoutTasksWithinDays(jobRetentionDays, MAX_TASK_TIME);
      if (!timeoutTasks.isEmpty()) {
        for (TaskDTO task : timeoutTasks) {
          DAO_REGISTRY.getTaskDAO().updateStatusAndTaskEndTime(task.getId(), TaskStatus.RUNNING, TaskStatus.TIMEOUT,
              System.currentTimeMillis(), "TIMEOUT status updated by MonitorTaskRunner");
        }
        LOG.warn("TIMEOUT tasks {}", timeoutTasks);
      }

      // Find all jobs in SCHEDULED status
      Map<Long, JobDTO> scheduledJobs = findScheduledJobsWithinDays(jobRetentionDays);

      // Remove SCHEDULED jobs that has WAITING tasks
      Set<Long> waitingJobs = findWaitingJobsWithinDays(jobRetentionDays);
      scheduledJobs.keySet().removeAll(waitingJobs);

      // Mark SCHEDULED jobs as TIMEOUT if it has any tasks that run for more than MAX_TASK_TIME or are marked as TIMEOUT
      Set<Long> timeoutJobs = findTimeoutJobsWithinDays(jobRetentionDays);
      if (!timeoutJobs.isEmpty()) {
        List<JobDTO> jobsToUpdate = extractJobDTO(scheduledJobs, timeoutJobs);
        if (!jobsToUpdate.isEmpty()) {
          DAO_REGISTRY.getJobDAO().updateJobStatusAndEndTime(jobsToUpdate, JobStatus.TIMEOUT, System.currentTimeMillis());
          scheduledJobs.keySet().removeAll(timeoutJobs);
          LOG.info("TIMEOUT jobs {}", timeoutJobs);
        }
      }

      // Mark SCHEDULED jobs as FAILED if it has any tasks are marked as FAILED
      Set<Long> failedJobs = findFailedJobsWithinDays(jobRetentionDays);
      if (!failedJobs.isEmpty()) {
        List<JobDTO> jobsToUpdate = extractJobDTO(scheduledJobs, failedJobs);
        if (!jobsToUpdate.isEmpty()) {
          DAO_REGISTRY.getJobDAO().updateJobStatusAndEndTime(jobsToUpdate, JobStatus.FAILED, System.currentTimeMillis());
          scheduledJobs.keySet().removeAll(failedJobs);
          LOG.info("FAILED jobs {}", timeoutJobs);
        }
      }

      // Mark the remaining jobs as COMPLETED
      if (!scheduledJobs.isEmpty()) {
        List<JobDTO> jobsToUpdate = new ArrayList<>(scheduledJobs.values());
        if (!jobsToUpdate.isEmpty()) {
          DAO_REGISTRY.getJobDAO().updateJobStatusAndEndTime(jobsToUpdate, JobStatus.COMPLETED, System.currentTimeMillis());
          LOG.info("COMPLETED jobs {}", scheduledJobs.keySet());
        }
      }

      // update detection health
      updateDetectionHealth();
    } catch (Exception e) {
      LOG.error("Exception in monitor update task", e);
    }
  }

  private void updateDetectionHealth() {
    DetectionConfigManager detectionDAO = DAO_REGISTRY.getDetectionConfigManager();
    List<DetectionConfigDTO> detectionConfigs = detectionDAO.findAllActive();
    for (DetectionConfigDTO config : detectionConfigs) {
      // update detection health status
      try {
        DateTime healthStatusWindowEnd = DateTime.now();
        DetectionHealth health = new DetectionHealth.Builder(config.getId(), healthStatusWindowEnd.minusDays(30).getMillis(),
            healthStatusWindowEnd.getMillis()).addRegressionStatus(DAO_REGISTRY.getEvaluationManager())
            .addAnomalyCoverageStatus(DAO_REGISTRY.getMergedAnomalyResultDAO())
            .addDetectionTaskStatus(DAO_REGISTRY.getTaskDAO())
            .addOverallHealth()
            .addOriginalDetectionHealth(config.getHealth())
            .build();
        // fetch the config again before saving to DB to avoid overriding config that is updated by other threads
        config = detectionDAO.findById(config.getId());
        config.setHealth(health);
        detectionDAO.update(config);
        LOG.info("Updated detection health for {}", config.getId());
      } catch (Exception e) {
        LOG.info("Update detection health for {} failed", config.getId(), e);
      }
    }
  }

  private void executeMonitorExpire(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor expire {}", monitorTaskInfo);

    // Delete completed jobs and tasks that are expired.
    try {
      // CAUTION: Fist delete tasks then jobs, as task has a foreign key.
      int completedJobRetentionDays = monitorTaskInfo.getCompletedJobRetentionDays();
      int deletedCompletedTasks = DAO_REGISTRY.getTaskDAO()
          .deleteRecordsOlderThanDaysWithStatus(completedJobRetentionDays, TaskStatus.COMPLETED);
      int deletedCompletedJobs =
          DAO_REGISTRY.getJobDAO().deleteRecordsOlderThanDaysWithStatus(completedJobRetentionDays, JobStatus.COMPLETED);
      LOG.info("Deleted {} completed jobs and {} completed tasks that are older than {} days.", deletedCompletedJobs,
          deletedCompletedTasks, completedJobRetentionDays);
    } catch (Exception e) {
      LOG.error("Exception when expiring jobs and tasks.", e);
    }

    // Delete all types of jobs and tasks that are expired.
    try {
      // CAUTION: Fist delete tasks then jobs, as task has a foreign key.
      int jobRetentionDays = monitorTaskInfo.getDefaultRetentionDays();
      int deletedTasks = DAO_REGISTRY.getTaskDAO().deleteRecordsOlderThanDays(jobRetentionDays);
      int deletedJobs = DAO_REGISTRY.getJobDAO().deleteRecordsOlderThanDays(jobRetentionDays);
      LOG.info("Deleted {} jobs and {} tasks that are older than {} days.", deletedTasks, deletedJobs,
          jobRetentionDays);
    } catch (Exception e) {
      LOG.error("Exception when expiring jobs and tasks.", e);
    }

    // Delete expired detection status.
    try {
      int deletedDetectionStatus = DAO_REGISTRY.getDetectionStatusDAO()
          .deleteRecordsOlderThanDays(monitorTaskInfo.getDetectionStatusRetentionDays());
      LOG.info("Deleted {} detection status that are older than {} days.", deletedDetectionStatus,
          monitorTaskInfo.getDetectionStatusRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when expiring detection status.", e);
    }

    // Delete expired data completeness entries.
    try {
      int deletedDetectionStatus = DAO_REGISTRY.getDataCompletenessConfigDAO()
          .deleteRecordsOlderThanDays(monitorTaskInfo.getDefaultRetentionDays());
      LOG.info("Deleted {} data completeness entries that are older than {} days.", deletedDetectionStatus,
          monitorTaskInfo.getDefaultRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when expiring data completeness entries.", e);
    }

    // Delete expired raw anomalies.
    try {
      int deletedRawAnomalies = DAO_REGISTRY.getRawAnomalyResultDAO()
          .deleteRecordsOlderThanDays(monitorTaskInfo.getRawAnomalyRetentionDays());
      LOG.info("Deleted {} raw anomalies that are older than {} days.", deletedRawAnomalies,
          monitorTaskInfo.getRawAnomalyRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when expiring raw anomalies.", e);
    }

    // Delete old evaluations.
    try {
      int deletedEvaluations = DAO_REGISTRY.getEvaluationManager().deleteRecordsOlderThanDays(monitorTaskInfo.getDefaultRetentionDays());
      LOG.info("Deleted {} evaluations that are older than {} days.", deletedEvaluations, monitorTaskInfo.getDefaultRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when deleting old evaluations.", e);
    }
  }

  private Map<Long, JobDTO> findScheduledJobsWithinDays(int days) {
    Map<Long, JobDTO> jobs = new HashMap<>();
    List<JobDTO> jobList = DAO_REGISTRY.getJobDAO().findByStatusWithinDays(JobStatus.SCHEDULED, days);
    if (CollectionUtils.isNotEmpty(jobList)) {
      for (JobDTO jobDTO : jobList) {
        jobs.put(jobDTO.getId(), jobDTO);
      }
    }
    return jobs;
  }

  private Set<Long> findWaitingJobsWithinDays(int days) {
    Set<Long> waitingJobIds = new HashSet<>();
    List<TaskDTO> waitingTasks = DAO_REGISTRY.getTaskDAO().findByStatusWithinDays(TaskStatus.WAITING, days);
    for (TaskDTO task : waitingTasks) {
      waitingJobIds.add(task.getJobId());
    }
    return waitingJobIds;
  }

  private Set<Long> findTimeoutJobsWithinDays(int days) {
    Set<Long> timeoutJobs = new HashSet<>();
    List<TaskDTO> timeoutTasks = DAO_REGISTRY.getTaskDAO().findByStatusWithinDays(TaskStatus.TIMEOUT, days);
    for (TaskDTO task : timeoutTasks) {
      timeoutJobs.add(task.getJobId());
    }
    return timeoutJobs;
  }

  private Set<Long> findFailedJobsWithinDays(int days) {
    Set<Long> failedJobIds = new HashSet<>();
    List<TaskDTO> failedTasks = DAO_REGISTRY.getTaskDAO().findByStatusWithinDays(TaskStatus.FAILED, days);
    for (TaskDTO task : failedTasks) {
      failedJobIds.add(task.getJobId());
    }
    return failedJobIds;
  }

  private List<JobDTO> extractJobDTO(Map<Long, JobDTO> allJobs, Set<Long> jobIdToExtract) {
    List<JobDTO> jobsToUpdate = new ArrayList<>();
    for (Long jobId : jobIdToExtract) {
      JobDTO jobDTO = allJobs.get(jobId);
      if (jobDTO != null) {
        jobsToUpdate.add(jobDTO);
      }
    }
    return jobsToUpdate;
  }
}
