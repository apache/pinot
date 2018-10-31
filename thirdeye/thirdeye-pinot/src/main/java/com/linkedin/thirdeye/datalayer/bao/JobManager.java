package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.util.List;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;


public interface JobManager extends AbstractManager<JobDTO> {

  List<JobDTO> findByStatus(JobStatus status);

  List<JobDTO> findByStatusWithinDays(JobStatus status, int days);

  void updateJobStatusAndEndTime(List<JobDTO> jobsToUpdate, JobStatus nweStatus, long newEndTime);

  int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status);

  List<JobDTO> findNRecentJobs(int n);

  String getJobNameByJobId(long id);

  JobDTO findLatestBackfillScheduledJobByFunctionId(long functionId, long backfillWindowStart, long backfillWindowEnd);

  List<JobDTO> findRecentScheduledJobByTypeAndConfigId(TaskConstants.TaskType taskType, long configId, long minScheduledTime);
}
