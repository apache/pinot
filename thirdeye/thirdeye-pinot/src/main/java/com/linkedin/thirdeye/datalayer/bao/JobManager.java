package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;


public interface JobManager extends AbstractManager<JobDTO>{

  List<JobDTO> findByStatus(JobStatus status);

  void updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime);

  int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status);

}
