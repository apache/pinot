package com.linkedin.thirdeye.datalayer.bao;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;

import java.sql.Timestamp;
import java.util.List;

import org.joda.time.DateTime;

public class JobManager extends AbstractManager<JobDTO> {

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE = "from AnomalyJobSpec aj "
      + "WHERE aj.status = :status AND aj.lastModified < :expireTimestamp";

  public JobManager() {
    super(JobDTO.class);
  }

  @Transactional
  public List<JobDTO> findByStatus(JobStatus status) {
    return super.findByParams(ImmutableMap.of("status", status));
  }

  @Transactional
  public void updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime) {
    JobDTO anomalyJobSpec = findById(id);
    anomalyJobSpec.setStatus(status);
    anomalyJobSpec.setScheduleEndTime(jobEndTime);
    save(anomalyJobSpec);
  }

  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    List<JobDTO> anomalyJobSpecs = getEntityManager()
        .createQuery(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, entityClass)
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status).getResultList();
    for (JobDTO anomalyJobSpec : anomalyJobSpecs) {
      deleteById(anomalyJobSpec.getId());
    }
    return anomalyJobSpecs.size();
  }
}
