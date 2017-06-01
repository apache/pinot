package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.pojo.JobBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class JobManagerImpl extends AbstractManagerImpl<JobDTO> implements JobManager {

  private static final String FIND_RECENT_SCHEDULED_JOB_BY_TYPE_AND_CONFIG_ID =
      "where type=:type and configId=:configId and status!=:status and scheduleStartTime>=:scheduleStartTime order by scheduleStartTime desc";

  public JobManagerImpl() {
    super(JobDTO.class, JobBean.class);
  }

  @Override
  @Transactional
  public List<JobDTO> findByStatus(JobStatus status) {
    return super.findByParams(ImmutableMap.<String, Object>of("status", status.toString()));
  }

  @Override
  @Transactional
  public void updateStatusAndJobEndTimeForJobIds(Set<Long> ids, JobStatus status, Long jobEndTime) {
    for (Long id : ids) {
      JobDTO anomalyJobSpec = findById(id);
      anomalyJobSpec.setStatus(status);
      anomalyJobSpec.setScheduleEndTime(jobEndTime);
      update(anomalyJobSpec);
    }
  }

  @Override
  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    Predicate timestampPredicate = Predicate.LT("updateTime", expireTimestamp);
    List<JobBean> list =
        genericPojoDao.get(Predicate.AND(statusPredicate, timestampPredicate), JobBean.class);
    for (JobBean jobBean : list) {
      deleteById(jobBean.getId());
    }
    return list.size();
  }

  @Override
  public List<JobDTO> findNRecentJobs(int n) {
    String parameterizedSQL = "order by scheduleStartTime desc limit "+n;
    HashMap<String, Object> parameterMap = new HashMap<>();
    List<JobBean> list = genericPojoDao.executeParameterizedSQL(parameterizedSQL, parameterMap, JobBean.class);
    return convertBeanListToDTOList(list);
  }

  @Override
  public String getJobNameByJobId(long id) {
    JobDTO anomalyJobSpec = findById(id);
    if (anomalyJobSpec != null) {
      return anomalyJobSpec.getJobName();
    } else {
      return null;
    }
  }

  @Override
  public JobDTO findLatestBackfillScheduledJobByFunctionId(long functionId, long backfillWindowStart, long backfillWindowEnd) {
    String parameterizedSQL =
        "where name like \"" + DetectionTaskRunner.BACKFILL_PREFIX + "%" + functionId + "\" ";
    HashMap<String, Object> parameterMap = new HashMap<>();
    List<JobBean> list = genericPojoDao.executeParameterizedSQL(parameterizedSQL, parameterMap, JobBean.class);

    if (CollectionUtils.isNotEmpty(list)) {
      // Sort by start window time in descending order
      Collections.sort(list, new Comparator<JobBean>(){
        @Override
        public int compare(JobBean j1, JobBean j2) {
          return (int) ((j2.getWindowStartTime()/1000) - (j1.getWindowStartTime()/1000));
        }
      });
      // Find the latest job whose start time is located between backfill window
      for (JobBean job : list) {
        long jobStartTime = job.getWindowStartTime();
        if (backfillWindowStart <= jobStartTime && jobStartTime <= backfillWindowEnd) {
          return convertBean2DTO(job, JobDTO.class);
        }
      }
    }

    return null;
  }

  @Override
  public List<JobDTO> findRecentScheduledJobByTypeAndConfigId(TaskConstants.TaskType taskType, long configId,
      long minScheduledTime) {
    HashMap<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("type", taskType);
    parameterMap.put("configId", configId);
    parameterMap.put("status", JobStatus.FAILED);
    parameterMap.put("scheduleStartTime", minScheduledTime);
    List<JobBean> list = genericPojoDao
        .executeParameterizedSQL(FIND_RECENT_SCHEDULED_JOB_BY_TYPE_AND_CONFIG_ID, parameterMap, JobBean.class);

    if (CollectionUtils.isNotEmpty(list)) {
      // Sort by scheduleStartTime; most recent scheduled job at the beginning
      Collections.sort(list, Collections.reverseOrder(new Comparator<JobBean>() {
        @Override
        public int compare(JobBean o1, JobBean o2) {
          return Long.compare(o1.getScheduleStartTime(), o2.getScheduleStartTime());
        }
      }));
      return convertBeanListToDTOList(list);
    } else {
      return Collections.emptyList();
    }
  }
}
