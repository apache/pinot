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

package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import java.util.Set;

public interface TaskManager extends AbstractManager<TaskDTO>{

  List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status);

  List<TaskDTO> findByStatusNotIn(TaskStatus status);

  List<TaskDTO> findByStatusWithinDays(TaskStatus status, int days);

  List<TaskDTO> findTimeoutTasksWithinDays(int days, long maxTaskTime);

  List<TaskDTO> findByStatusOrderByCreateTime(TaskStatus status, int fetchSize, boolean asc);

  boolean updateStatusAndWorkerId(Long workerId, Long id, Set<TaskStatus> allowedOldStatus,
      TaskStatus newStatus, int expectedVersion);

  void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime, String message);

  int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status);

}
