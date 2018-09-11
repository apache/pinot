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
