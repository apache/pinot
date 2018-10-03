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

package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
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
      Long taskEndTime, String message) {
    TaskDTO task = findById(id);
    if (task.getStatus().equals(oldStatus)) {
      task.setStatus(newStatus);
      task.setEndTime(taskEndTime);
      task.setMessage(message);
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
    return deleteByPredicate(Predicate.AND(statusPredicate, timestampPredicate));
  }

  @Override
  @Transactional
  public List<TaskDTO> findByStatusNotIn(TaskStatus status) {
    Predicate statusPredicate = Predicate.NEQ("status", status.toString());
    return findByPredicate(statusPredicate);
  }

  @Override
  public List<TaskDTO> findByStatusWithinDays(TaskStatus status, int days) {
    DateTime activeDate = new DateTime().minusDays(days);
    Timestamp activeTimestamp = new Timestamp(activeDate.getMillis());
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    Predicate timestampPredicate = Predicate.GE("createTime", activeTimestamp);
    return findByPredicate(Predicate.AND(statusPredicate, timestampPredicate));
  }

  @Override
  public List<TaskDTO> findTimeoutTasksWithinDays(int days, long maxTaskTime) {
    DateTime activeDate = new DateTime().minusDays(days);
    Timestamp activeTimestamp = new Timestamp(activeDate.getMillis());
    DateTime timeoutDate = new DateTime().minus(maxTaskTime);
    Timestamp timeoutTimestamp = new Timestamp(timeoutDate.getMillis());
    Predicate statusPredicate = Predicate.EQ("status", TaskStatus.RUNNING.toString());
    Predicate daysTimestampPredicate = Predicate.GE("createTime", activeTimestamp);
    Predicate timeoutTimestampPredicate = Predicate.LT("updateTime", timeoutTimestamp);
    return findByPredicate(Predicate.AND(statusPredicate, daysTimestampPredicate, timeoutTimestampPredicate));
  }
}
