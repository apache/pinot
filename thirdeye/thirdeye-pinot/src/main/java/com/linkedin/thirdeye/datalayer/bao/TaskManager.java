package com.linkedin.thirdeye.datalayer.bao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import org.joda.time.DateTime;

public class TaskManager extends AbstractManager<TaskDTO> {

  private static final String FIND_BY_JOB_ID_STATUS_NOT_IN = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE at.job.id = :jobId "
      + "AND at.status != :status";

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE at.status = :status order by at.taskStartTime asc";

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE = "SELECT at FROM AnomalyTaskSpec at "
      + "WHERE at.status = :status AND at.lastModified < :expireTimestamp";

  public TaskManager() {
    super(TaskDTO.class);
  }

  @Transactional(rollbackOn = Exception.class)
  public List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status) {
    return getEntityManager().createQuery(FIND_BY_JOB_ID_STATUS_NOT_IN, entityClass)
        .setParameter("jobId", jobId)
        .setParameter("status", status).getResultList();
  }

  public List<TaskDTO> findByStatusOrderByCreateTimeAsc(TaskStatus status, int fetchSize) {
    EntityManager em = getEntityManager();
    EntityTransaction txn = em.getTransaction();
    List<TaskDTO> tasks = new ArrayList<>();
    try {
      txn.begin();
      tasks = em.createQuery(FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC, entityClass)
          .setMaxResults(fetchSize).setParameter("status", status).getResultList();
      txn.commit();
      return tasks;
    } catch (Exception e) {
      if (txn.isActive()) {
        txn.rollback();
      }
    }
    return tasks;
  }

  @Transactional(rollbackOn = Exception.class)
  public boolean updateStatusAndWorkerId(Long workerId,
      Long id, TaskStatus oldStatus, TaskStatus newStatus) {
    TaskDTO task = findById(id);
    if (task.getStatus().equals(oldStatus)) {
      task.setStatus(newStatus);
      task.setWorkerId(workerId);
      save(task);
      return true;
    } else {
      return false;
    }
  }

  @Transactional(rollbackOn = Exception.class)
  public void updateStatusAndTaskEndTime(Long id,
      TaskStatus oldStatus, TaskStatus newStatus, Long taskEndTime) {
    TaskDTO task = findById(id);
    if (task.getStatus().equals(oldStatus)) {
      task.setStatus(newStatus);
      task.setTaskEndTime(taskEndTime);
      save(task);
    }
  }

  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    List<TaskDTO> anomalyTaskSpecs = getEntityManager()
        .createQuery(FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE, entityClass)
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status).getResultList();

    for (TaskDTO anomalyTaskSpec : anomalyTaskSpecs) {
      deleteById(anomalyTaskSpec.getId());
    }
    return anomalyTaskSpecs.size();
  }
}
