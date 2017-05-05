package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datalayer.pojo.TaskBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class TaskManagerImpl extends AbstractManagerImpl<TaskDTO> implements TaskManager {

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC =
      " WHERE status = :status order by startTime asc";

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_DESC =
      " WHERE status = :status order by startTime desc";

  public TaskManagerImpl() {
    super(TaskDTO.class, TaskBean.class);
  }

  public Long save(TaskDTO entity) {
    if (entity.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(entity);
      return entity.getId();
    }
    TaskBean bean = (TaskBean) convertDTO2Bean(entity, TaskBean.class);
    Long id = genericPojoDao.put(bean);
    entity.setId(id);
    return id;

  }

  @Override
  public List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status) {
    Predicate jobIdPredicate = Predicate.EQ("jobId", jobId);
    Predicate statusPredicate = Predicate.NEQ("status", status.toString());
    Predicate predicate = Predicate.AND(statusPredicate, jobIdPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<TaskDTO> findByStatusOrderByCreateTime(TaskStatus status, int fetchSize, boolean asc) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("status", status.toString());
    List<TaskBean> list;
    String queryClause = (asc) ? FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC : FIND_BY_STATUS_ORDER_BY_CREATE_TIME_DESC;
    list = genericPojoDao.executeParameterizedSQL(queryClause, parameterMap, TaskBean.class);
    List<TaskDTO> result = new ArrayList<>();
    for (TaskBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, TaskDTO.class));
    }
    return result;
  }

  @Override
  public boolean updateStatusAndWorkerId(Long workerId, Long id, Set<TaskStatus> permittedOldStatus,
      TaskStatus newStatus, int expectedVersion) {
    TaskDTO task = findById(id);
    if (permittedOldStatus.contains(task.getStatus())) {
      task.setStatus(newStatus);
      task.setWorkerId(workerId);
      //increment the version
      task.setVersion(expectedVersion + 1);
      Predicate predicate = Predicate.AND(
          Predicate.EQ("id", id),
          Predicate.EQ("version", expectedVersion));
      int update = update(task, predicate);
      return update == 1;
    } else {
      return false;
    }
  }

  @Override
  public void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime) {
    TaskDTO task = findById(id);
    if (task.getStatus().equals(oldStatus)) {
      task.setStatus(newStatus);
      task.setEndTime(taskEndTime);
      save(task);
    }
  }

  @Override
  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());

    Predicate timestampPredicate = Predicate.LT("createTime", expireTimestamp);
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    List<TaskBean> list =
        genericPojoDao.get(Predicate.AND(statusPredicate, timestampPredicate), TaskBean.class);
    for (TaskBean bean : list) {
      deleteById(bean.getId());
    }
    return list.size();
  }

  @Override
  @Transactional
  public List<TaskDTO> findByStatusNotIn(TaskStatus status) {
    Predicate statusPredicate = Predicate.NEQ("status", status.toString());
    return findByPredicate(statusPredicate);
  }
}
