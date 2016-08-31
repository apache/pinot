package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;

import java.sql.Timestamp;
import java.util.List;

import org.joda.time.DateTime;

public class AnomalyJobDAO extends AbstractJpaDAO<AnomalyJobSpec> {

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE = "from AnomalyJobSpec aj "
      + "WHERE aj.status = :status AND aj.lastModified < :expireTimestamp";

//  private static final String DELETE_FROM_TASK_BY_JOB_ID = "delete from AnomalyTaskSpec t where t.job.id = :jobId";

  public AnomalyJobDAO() {
    super(AnomalyJobSpec.class);
  }

  @Transactional
  public List<AnomalyJobSpec> findByStatus(JobStatus status) {
    return super.findByParams(ImmutableMap.of("status", status));
  }

  @Transactional
  public void updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime) {
    AnomalyJobSpec anomalyJobSpec = findById(id);
    anomalyJobSpec.setStatus(status);
    anomalyJobSpec.setScheduleEndTime(jobEndTime);
    save(anomalyJobSpec);
  }

  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    List<AnomalyJobSpec> anomalyJobSpecs = getEntityManager()
        .createQuery(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, entityClass)
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status).getResultList();
    for (AnomalyJobSpec anomalyJobSpec : anomalyJobSpecs) {
      deleteById(anomalyJobSpec.getId());
    }
    return anomalyJobSpecs.size();
  }
}
