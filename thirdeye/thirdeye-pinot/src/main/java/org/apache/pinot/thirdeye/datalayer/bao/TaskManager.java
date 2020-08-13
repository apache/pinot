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

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.List;

import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import java.util.Set;

public interface TaskManager extends AbstractManager<TaskDTO>{

  List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status);

  List<TaskDTO> findByNameOrderByCreateTime(String name, int fetchSize, boolean asc);

  List<TaskDTO> findByStatusNotIn(TaskStatus status);

  List<TaskDTO> findByStatusWithinDays(TaskStatus status, int days);

  List<TaskDTO> findByStatusesAndTypeWithinDays(List<TaskStatus> statuses, TaskConstants.TaskType type, int days);

  List<TaskDTO> findTimeoutTasksWithinDays(int days, long maxTaskTime);

  List<TaskDTO> findByStatusOrderByCreateTime(TaskStatus status, int fetchSize, boolean asc);

  List<TaskDTO> findByStatusAndTypeOrderByCreateTime(TaskStatus status, TaskConstants.TaskType type, int fetchSize, boolean asc);

  List<TaskDTO> findByStatusAndTypeNotInOrderByCreateTime(TaskStatus status, TaskConstants.TaskType type, int fetchSize, boolean asc);

  List<TaskDTO> findByStatusAndWorkerId(Long workerId, TaskStatus status);

  boolean updateStatusAndWorkerId(Long workerId, Long id, Set<TaskStatus> allowedOldStatus, int expectedVersion);

  void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime, String message);

  void updateTaskStartTime(Long id, Long taskStartTime);

  int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status);

  int countWaiting();

  void populateDetectionConfig(DetectionConfigDTO detectionConfigDTO, DetectionPipelineTaskInfo taskInfo);

  DetectionConfigDTO extractDetectionConfig(DetectionPipelineTaskInfo taskInfo);
}
