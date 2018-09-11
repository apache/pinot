/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.classification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.job.JobRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dashboard.resources.EntityManagerResource.OBJECT_MAPPER;

public class ClassificationJobRunner implements JobRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ClassificationJobRunner.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final JobManager jobDAO = DAO_REGISTRY.getJobDAO();
  private ClassificationJobContext classificationJobContext;
  private TaskGenerator taskGenerator = new TaskGenerator();

  public ClassificationJobRunner(ClassificationJobContext classificationJobContext) {
    this.classificationJobContext = classificationJobContext;
  }

  @Override
  public Long createJob() {
    try {
      JobDTO jobSpec = new JobDTO();
      String jobName = createJobName(classificationJobContext);
      jobSpec.setJobName(jobName);
      jobSpec.setConfigId(classificationJobContext.getConfigDTO().getId());
      jobSpec.setWindowStartTime(classificationJobContext.getWindowStartTime());
      jobSpec.setWindowEndTime(classificationJobContext.getWindowEndTime());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
      jobSpec.setTaskType(TaskConstants.TaskType.CLASSIFICATION);
      Long jobExecutionId = jobDAO.save(jobSpec);
      classificationJobContext.setJobName(jobName);
      classificationJobContext.setJobExecutionId(jobExecutionId);
      LOG.info("Created classification job spec {} with jobExecutionId {}, window start {} and end {}", jobSpec,
          jobExecutionId, classificationJobContext.getWindowStartTime(), classificationJobContext.getWindowEndTime());
      return jobExecutionId;
    } catch (Exception e) {
      LOG.error("Exception in creating classification job", e);
    }
    return null;
  }

  @Override
  public List<Long> createTasks() {
    List<Long> taskIds = new ArrayList<>();

    try {
      List<ClassificationTaskInfo> taskInfos = taskGenerator
          .createGroupingTasks(classificationJobContext, classificationJobContext.getWindowStartTime(),
              classificationJobContext.getWindowEndTime());
      LOG.info("Creating classification tasks {}", taskInfos);
      for (ClassificationTaskInfo taskInfo : taskInfos) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting ClassificationTaskInfo {} to jsonString", taskInfo, e);
        }

        TaskDTO taskSpec = new TaskDTO();
        taskSpec.setTaskType(TaskConstants.TaskType.CLASSIFICATION);
        taskSpec.setJobName(classificationJobContext.getJobName());
        taskSpec.setStatus(TaskConstants.TaskStatus.WAITING);
        taskSpec.setStartTime(System.currentTimeMillis());
        taskSpec.setTaskInfo(taskInfoJson);
        taskSpec.setJobId(classificationJobContext.getJobExecutionId());
        long taskId = DAO_REGISTRY.getTaskDAO().save(taskSpec);

        taskIds.add(taskId);
        LOG.info("Created classification task {} with taskId {}", taskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating classification tasks", e);
    }

    return taskIds;
  }

  @Override
  public void run() {
    Long jobExecutionId = createJob();
    if (jobExecutionId != null) {
      List<Long> taskIds = createTasks();
    }
  }

  private static String createJobName(ClassificationJobContext jobContext) {
    long configId = jobContext.getConfigDTO().getId();
    String configName = jobContext.getConfigDTO().getName();
    long startTimes = jobContext.getWindowStartTime();
    long endTimes = jobContext.getWindowEndTime();

    return String.format("%s-%s-%s-%s", configId, configName, startTimes, endTimes);
  }
}
