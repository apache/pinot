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

package com.linkedin.thirdeye.completeness.checker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.job.JobRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** job runner for data completeness job
 *
 */
public class DataCompletenessJobRunner implements JobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessJobRunner.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DataCompletenessJobContext dataCompletenessJobContext;
  private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm");

  public DataCompletenessJobRunner(DataCompletenessJobContext dataCompletenessJobContext) {
    this.dataCompletenessJobContext = dataCompletenessJobContext;
  }

  @Override
  public void run() {
    DateTime now = new DateTime();
    long checkDurationEndTime = now.getMillis();
    long checkDurationStartTime = now.minus(TimeUnit.MILLISECONDS.convert(
        DataCompletenessConstants.LOOKBACK_TIME_DURATION, DataCompletenessConstants.LOOKBACK_TIMEUNIT)).getMillis();

    String checkerEndTime = dateTimeFormatter.print(checkDurationEndTime);
    String checkerStartTime = dateTimeFormatter.print(checkDurationStartTime);
    String jobName =
        String.format("%s-%s-%s", TaskType.DATA_COMPLETENESS.toString(), checkerStartTime, checkerEndTime);

    dataCompletenessJobContext.setCheckDurationStartTime(checkDurationStartTime);
    dataCompletenessJobContext.setCheckDurationEndTime(checkDurationEndTime);
    dataCompletenessJobContext.setJobName(jobName);

    Set<String> datasetsToCheck = new HashSet<>();
    for (DatasetConfigDTO datasetConfig : DAO_REGISTRY.getDatasetConfigDAO().findActiveRequiresCompletenessCheck()) {
      datasetsToCheck.add(datasetConfig.getDataset());
    }
    for (AnomalyFunctionDTO anomalyFunction : DAO_REGISTRY.getAnomalyFunctionDAO().findAllActiveFunctions()) {
      if (anomalyFunction.isRequiresCompletenessCheck()) {
        datasetsToCheck.add(anomalyFunction.getCollection());
      }
    }
    dataCompletenessJobContext.setDatasetsToCheck(Lists.newArrayList(datasetsToCheck));

    // create data completeness job
    long jobExecutionId = createJob();
    dataCompletenessJobContext.setJobExecutionId(jobExecutionId);

    // create data completeness tasks
    createTasks();

  }

  public Long createJob() {
    Long jobExecutionId = null;
    try {
      LOG.info("Creating data completeness job");
      JobDTO jobSpec = new JobDTO();
      jobSpec.setJobName(dataCompletenessJobContext.getJobName());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobStatus.SCHEDULED);
      jobSpec.setTaskType(TaskType.DATA_COMPLETENESS);
      jobSpec.setConfigId(0); // Data completeness job does not have a config id
      jobExecutionId = DAO_REGISTRY.getJobDAO().save(jobSpec);
      LOG.info("Created JobSpec {} with jobExecutionId {}", jobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating data completeness job", e);
    }
    return jobExecutionId;
  }

  protected List<DataCompletenessTaskInfo> createDataCompletenessTasks(DataCompletenessJobContext dataCompletenessJobContext) {
    List<DataCompletenessTaskInfo> tasks = new ArrayList<>();

    // create 1 task, which will get data and perform check
    DataCompletenessTaskInfo dataCompletenessCheck = new DataCompletenessTaskInfo();
    dataCompletenessCheck.setDataCompletenessType(DataCompletenessConstants.DataCompletenessType.CHECKER);
    dataCompletenessCheck.setDataCompletenessStartTime(dataCompletenessJobContext.getCheckDurationStartTime());
    dataCompletenessCheck.setDataCompletenessEndTime(dataCompletenessJobContext.getCheckDurationEndTime());
    dataCompletenessCheck.setDatasetsToCheck(dataCompletenessJobContext.getDatasetsToCheck());
    tasks.add(dataCompletenessCheck);

    // create 1 task, for cleanup
    DataCompletenessTaskInfo cleanup = new DataCompletenessTaskInfo();
    cleanup.setDataCompletenessType(DataCompletenessConstants.DataCompletenessType.CLEANUP);
    tasks.add(cleanup);

    return tasks;
  }

  public List<Long> createTasks() {
    List<Long> taskIds = new ArrayList<>();
    try {
      LOG.info("Creating data completeness checker tasks");
      List<DataCompletenessTaskInfo> dataCompletenessTasks =
          createDataCompletenessTasks(dataCompletenessJobContext);
      LOG.info("DataCompleteness tasks {}", dataCompletenessTasks);
      for (DataCompletenessTaskInfo taskInfo : dataCompletenessTasks) {

        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting DataCompletenessTaskInfo {} to jsonString", taskInfo, e);
        }
        TaskDTO taskSpec = new TaskDTO();
        taskSpec.setTaskType(TaskType.DATA_COMPLETENESS);
        taskSpec.setJobName(dataCompletenessJobContext.getJobName());
        taskSpec.setStatus(TaskStatus.WAITING);
        taskSpec.setStartTime(System.currentTimeMillis());
        taskSpec.setTaskInfo(taskInfoJson);
        taskSpec.setJobId(dataCompletenessJobContext.getJobExecutionId());
        long taskId = DAO_REGISTRY.getTaskDAO().save(taskSpec);
        taskIds.add(taskId);
        LOG.info("Created dataCompleteness task {} with taskId {}", taskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating data completeness tasks", e);
    }
    return taskIds;

  }


}
