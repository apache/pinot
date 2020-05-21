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

package org.apache.pinot.thirdeye.detection.dataquality;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.apache.pinot.thirdeye.detection.TaskUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The data quality job submitted to the scheduler. This job creates data quality tasks which
 * the runners will later pick and execute.
 */
public class DataQualityPipelineJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(DataQualityPipelineJob.class);

  private final TaskManager taskDAO = DAORegistry.getInstance().getTaskDAO();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long DATA_AVAILABILITY_TASK_TIMEOUT = TimeUnit.MINUTES.toMillis(15);

  @Override
  public void execute(JobExecutionContext jobExecutionContext) {
    DetectionPipelineTaskInfo taskInfo = TaskUtils.buildTaskInfo(jobExecutionContext);

    // if a task is pending and not time out yet, don't schedule more
    String jobName = String.format("%s_%d", TaskConstants.TaskType.DATA_QUALITY, taskInfo.getConfigId());
    if (TaskUtils.checkTaskAlreadyRun(jobName, taskInfo, DATA_AVAILABILITY_TASK_TIMEOUT)) {
      LOG.info("Skip scheduling {} task for {} with start time {}. Task is already in the queue.",
          TaskConstants.TaskType.DATA_QUALITY, jobName, taskInfo.getStart());
      return;
    }

    String taskInfoJson = null;
    try {
      taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
    } catch (JsonProcessingException e) {
      LOG.error("Exception when converting DetectionPipelineTaskInfo {} to jsonString", taskInfo, e);
    }

    TaskDTO taskDTO = TaskUtils.buildTask(taskInfo.getConfigId(), taskInfoJson, TaskConstants.TaskType.DATA_QUALITY);
    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created {} task {} with taskId {}", TaskConstants.TaskType.DATA_QUALITY, taskDTO, taskId);
  }
}


