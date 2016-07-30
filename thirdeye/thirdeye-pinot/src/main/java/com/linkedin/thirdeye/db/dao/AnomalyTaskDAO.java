package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;

import java.sql.Timestamp;
import java.util.List;

import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

import org.hibernate.StaleObjectStateException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyTaskDAO extends AbstractJpaDAO<AnomalyTaskSpec> {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyTaskDAO.class);

  private static final String FIND_BY_JOB_ID_STATUS_NOT_IN = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE at.job.id = :jobId "
      + "AND at.status != :status";

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE at.status = :status "
      + "order by at.taskStartTime asc";

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE at.status = :status AND at.lastModified < :expireTimestamp";

  private static final String FIND_BY_STATUS_AND_ID = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE status = :status and id = :id";


  public AnomalyTaskDAO() {
    super(AnomalyTaskSpec.class);
  }

  public List<AnomalyTaskSpec> findByJobIdStatusNotIn(Long jobId, TaskStatus status) {
    return getEntityManager().createQuery(FIND_BY_JOB_ID_STATUS_NOT_IN, entityClass)
        .setParameter("jobId", jobId)
        .setParameter("status", status).getResultList();
  }

  @Transactional
  public List<AnomalyTaskSpec> findByStatusOrderByCreateTimeAscending(TaskStatus status) {
    return getEntityManager().createQuery(FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC, entityClass)
            .setParameter("status", status)
            .getResultList();
  }

  public List<AnomalyTaskSpec> findByIdAndStatus(Long id, TaskStatus status) {
    return getEntityManager().createQuery(FIND_BY_STATUS_AND_ID, entityClass)
            .setParameter("status", status)
            .setParameter("id", id)
            .getResultList();
  }

  public boolean updateStatusAndWorkerId(Long workerId, Long id, TaskStatus oldStatus, TaskStatus newStatus) {
    try {
      List<AnomalyTaskSpec> anomalyTaskSpecs = findByIdAndStatus(id, oldStatus);
      if (anomalyTaskSpecs.size() == 1) {
        AnomalyTaskSpec anomalyTaskSpec = anomalyTaskSpecs.get(0);
        anomalyTaskSpec.setStatus(newStatus);
        anomalyTaskSpec.setWorkerId(workerId);
        save(anomalyTaskSpec);
        return true;
      }
    } catch (OptimisticLockException | RollbackException | StaleObjectStateException e) {
      LOG.warn("OptimisticLockException while updating workerId {} and status {} for anomalyTaskId {}",
          workerId, newStatus, id);
    } catch (Exception e) {
      LOG.error("Exception in updateStatusAndWorkedId", e);
    }
    return false;

  }

  public void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus, Long taskEndTime) {
    List<AnomalyTaskSpec> anomalyTaskSpecs = findByIdAndStatus(id, oldStatus);
    if (anomalyTaskSpecs.size() == 1) {
      AnomalyTaskSpec anomalyTaskSpec = anomalyTaskSpecs.get(0);
      anomalyTaskSpec.setStatus(newStatus);
      anomalyTaskSpec.setTaskEndTime(taskEndTime);
      save(anomalyTaskSpec);
    }
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    List<AnomalyTaskSpec> anomalyTaskSpecs = getEntityManager()
        .createQuery(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, entityClass)
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status).getResultList();

    for (AnomalyTaskSpec anomalyTaskSpec : anomalyTaskSpecs) {
      delete(anomalyTaskSpec);
    }
    return anomalyTaskSpecs.size();
  }
}
