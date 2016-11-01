package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.pojo.JobBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class JobManagerImpl extends AbstractManagerImpl<JobDTO> implements JobManager {

  public JobManagerImpl() {
    super(JobDTO.class, JobBean.class);
  }

  @Override
  @Transactional
  public List<JobDTO> findByStatus(JobStatus status) {
    return super.findByParams(ImmutableMap.of("status", status.toString()));
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
    List<JobDTO> ret = new ArrayList<>();
    for(JobBean bean:list){
      JobDTO dto = convertBean2DTO(bean, JobDTO.class);
      ret.add(dto);
    }
    return ret;
  }
}
