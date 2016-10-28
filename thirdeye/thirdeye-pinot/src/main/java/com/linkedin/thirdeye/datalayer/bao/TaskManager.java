package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import java.util.Set;

public interface TaskManager extends AbstractManager<TaskDTO>{

  List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status);

  List<TaskDTO> findByStatusNotIn(TaskStatus status);

  List<TaskDTO> findByStatusOrderByCreateTime(TaskStatus status, int fetchSize, boolean asc);

  boolean updateStatusAndWorkerId(Long workerId, Long id, Set<TaskStatus> allowedOldStatus,
      TaskStatus newStatus, int expectedVersion);

  void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime);

  int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status);

}
