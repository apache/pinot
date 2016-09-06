package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;


public interface TaskManager extends AbstractManager<TaskDTO>{

  List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status);

  List<TaskDTO> findByStatusOrderByCreateTimeAsc(TaskStatus status, int fetchSize);

  boolean updateStatusAndWorkerId(Long workerId, Long id, TaskStatus oldStatus,
      TaskStatus newStatus);

  void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime);

  int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status);

}
