package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;

import java.sql.Timestamp;
import java.util.List;

import org.joda.time.DateTime;

public class AnomalyJobDAO extends AbstractJpaDAO<AnomalyJobSpec> {

  private static final String FIND_BY_STATUS = "SELECT aj FROM AnomalyJobSpec aj "
      + "WHERE aj.status = :status";

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE = "SELECT aj FROM AnomalyJobSpec aj "
      + "WHERE aj.status = :status AND aj.lastModified < :expireTimestamp";

  public AnomalyJobDAO() {
    super(AnomalyJobSpec.class);
  }

  public List<AnomalyJobSpec> findByStatus(JobStatus status) {
    return getEntityManager().createQuery(FIND_BY_STATUS, entityClass)
        .setParameter("status", status).getResultList();
  }

  public void updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime) {
    AnomalyJobSpec anomalyJobSpec = findById(id);
    anomalyJobSpec.setStatus(status);
    anomalyJobSpec.setScheduleEndTime(jobEndTime);
    save(anomalyJobSpec);
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    List<AnomalyJobSpec> anomalyJobSpecs = getEntityManager()
        .createQuery(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, entityClass)
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status).getResultList();

    for (AnomalyJobSpec anomalyJobSpec : anomalyJobSpecs) {
      delete(anomalyJobSpec);
    }
    return anomalyJobSpecs.size();
  }
}
