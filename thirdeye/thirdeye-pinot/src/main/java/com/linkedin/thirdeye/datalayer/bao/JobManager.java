package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.entity.Job;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import java.util.List;

public class JobManager extends AbstractManager<JobDTO, Job> {

  public List<AnomalyJobSpec> findByStatus(JobStatus status) {
    return null;
  }

  public void updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime) {
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    return 0;
  }
}
