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

package org.apache.pinot.thirdeye.anomaly.alert.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.thirdeye.anomaly.alert.AlertJobContext;
import org.apache.pinot.thirdeye.anomaly.alert.AlertTaskInfo;
import org.apache.pinot.thirdeye.anomaly.job.JobConstants;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.task.TaskGenerator;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.JobManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class AlertJobRunnerV2 implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(AlertJobRunnerV2.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String ALERT_JOB_CONTEXT_V2 = "ALERT_JOB_CONTEXT_V2";
  public static final String ALERT_JOB_MONITORING_WINDOW_START_TIME = "ALERT_JOB_MONITORING_WINDOW_START_TIME";
  public static final String ALERT_JOB_MONITORING_WINDOW_END_TIME = "ALERT_JOB_MONITORING_WINDOW_END_TIME";

  private JobManager jobDAO = DAORegistry.getInstance().getJobDAO();
  private TaskManager taskDAO = DAORegistry.getInstance().getTaskDAO();
  private AlertConfigManager alertConfigDAO = DAORegistry.getInstance().getAlertConfigDAO();

  private TaskGenerator taskGenerator;
  private AlertJobContext alertJobContext;

  public AlertJobRunnerV2() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());
    alertJobContext = (AlertJobContext) jobExecutionContext.getJobDetail().getJobDataMap().get(ALERT_JOB_CONTEXT_V2);
    long alertConfigId = alertJobContext.getAlertConfigId();

    AlertConfigDTO alertConfig = alertConfigDAO.findById(alertConfigId);
    if (alertConfig == null) {
      LOG.error("Alert config with id {} does not exist", alertConfigId);
    } else {
      alertJobContext.setAlertConfigDTO(alertConfig);

      DateTime monitoringWindowStartTime =
          (DateTime) jobExecutionContext.getJobDetail().getJobDataMap().get(ALERT_JOB_MONITORING_WINDOW_START_TIME);

      DateTime monitoringWindowEndTime =
          (DateTime) jobExecutionContext.getJobDetail().getJobDataMap().get(ALERT_JOB_MONITORING_WINDOW_END_TIME);

      // TODO :remove this monitoring window logic; alert v2 is not based on monitoring window.
       // Compute window end
      if (monitoringWindowEndTime == null) {
        Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
        monitoringWindowEndTime = new DateTime(scheduledFireTime);
      }
      // Compute window start according to window end
      if (monitoringWindowStartTime == null) {
        monitoringWindowStartTime = monitoringWindowEndTime;
      }

      // write to alert_jobs
      Long jobExecutionId = createJob(monitoringWindowStartTime, monitoringWindowEndTime);
      alertJobContext.setJobExecutionId(jobExecutionId);

      // write to alert_tasks
      List<Long> taskIds = createTasks(monitoringWindowStartTime, monitoringWindowEndTime);
      LOG.info("Alert V2 tasks created : {}", taskIds);
    }
  }

  private long createJob(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    Long jobExecutionId = null;
    try {
      JobDTO jobSpec = new JobDTO();
      jobSpec.setJobName(alertJobContext.getJobName());
      jobSpec.setConfigId(alertJobContext.getAlertConfigId());
      jobSpec.setWindowStartTime(monitoringWindowStartTime.getMillis());
      jobSpec.setWindowEndTime(monitoringWindowEndTime.getMillis());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
      jobSpec.setTaskType(TaskConstants.TaskType.ALERT2);
      jobExecutionId = jobDAO.save(jobSpec);
      LOG.info("Created alert job {} with jobExecutionId {}", jobSpec, jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating alert job", e);
    }

    return jobExecutionId;
  }

  private List<Long> createTasks(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    List<Long> taskIds = new ArrayList<>();
    try {

      List<AlertTaskInfo> tasks = taskGenerator
          .createAlertTasksV2(alertJobContext, monitoringWindowStartTime, monitoringWindowEndTime);

      for (AlertTaskInfo taskInfo : tasks) {

        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
        }
        TaskDTO taskSpec = new TaskDTO();
        taskSpec.setTaskType(TaskConstants.TaskType.ALERT2);
        taskSpec.setJobName(alertJobContext.getJobName());
        taskSpec.setStatus(TaskConstants.TaskStatus.WAITING);
        taskSpec.setTaskInfo(taskInfoJson);
        taskSpec.setJobId(alertJobContext.getJobExecutionId());
        long taskId = taskDAO.save(taskSpec);
        taskIds.add(taskId);
        LOG.info("Created alert task {} with taskId {}", taskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating alert tasks", e);
    }
    return taskIds;
  }
}
