/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.joda.time.DateTime;

import com.google.inject.persist.Transactional;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.TaskBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class TaskManagerImpl extends AbstractManagerImpl<TaskDTO> implements TaskManager {
  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC =
      " WHERE status = :status order by startTime asc limit 10";

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_DESC =
      " WHERE status = :status order by startTime desc limit 10";

  private static final String FIND_BY_STATUS_AND_TYPE_ORDER_BY_CREATE_TIME_ASC =
          " WHERE status = :status and type = :type order by startTime asc limit 10";

  private static final String FIND_BY_STATUS_AND_TYPE_ORDER_BY_CREATE_TIME_DESC =
          " WHERE status = :status and type = :type order by startTime desc limit 10";

  private static final String FIND_BY_STATUS_AND_TYPE_NOT_IN_ORDER_BY_CREATE_TIME_ASC =
      " WHERE status = :status and type != :type order by startTime asc limit 10";

  private static final String FIND_BY_STATUS_AND_TYPE_NOT_IN_ORDER_BY_CREATE_TIME_DESC =
      " WHERE status = :status and type != :type order by startTime desc limit 10";

  private static final String FIND_BY_NAME_ORDER_BY_CREATE_TIME_ASC =
      " WHERE name = :name order by createTime asc limit ";

  private static final String FIND_BY_NAME_ORDER_BY_CREATE_TIME_DESC =
      " WHERE name = :name order by createTime desc limit ";

  private static final String COUNT_WAITING_TASKS =
      "SELECT COUNT(*) FROM task_index WHERE status = 'WAITING'";

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerImpl.class);

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
  public List<TaskDTO> findByNameOrderByCreateTime(String name, int fetchSize, boolean asc) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    String queryClause = (asc) ? FIND_BY_NAME_ORDER_BY_CREATE_TIME_ASC + fetchSize
        : FIND_BY_NAME_ORDER_BY_CREATE_TIME_DESC + fetchSize;
    List<TaskBean>  list = genericPojoDao.executeParameterizedSQL(queryClause, parameterMap, TaskBean.class);
    List<TaskDTO> result = new ArrayList<>();
    for (TaskBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, TaskDTO.class));
    }
    return result;
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
  public List<TaskDTO> findByStatusAndTypeOrderByCreateTime(TaskStatus status, TaskConstants.TaskType type, int fetchSize, boolean asc) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("status", status.toString());
    parameterMap.put("type", type.toString());
    List<TaskBean> list;
    String queryClause = (asc) ? FIND_BY_STATUS_AND_TYPE_ORDER_BY_CREATE_TIME_ASC
            : FIND_BY_STATUS_AND_TYPE_ORDER_BY_CREATE_TIME_DESC;
    list = genericPojoDao.executeParameterizedSQL(queryClause, parameterMap, TaskBean.class);
    List<TaskDTO> result = new ArrayList<>();
    for (TaskBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, TaskDTO.class));
    }
    return result;
  }

  @Override
  public List<TaskDTO> findByStatusAndTypeNotInOrderByCreateTime(TaskStatus status,
      TaskConstants.TaskType type, int fetchSize, boolean asc) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("status", status.toString());
    parameterMap.put("type", type.toString());
    List<TaskBean> list;
    String queryClause = (asc) ? FIND_BY_STATUS_AND_TYPE_NOT_IN_ORDER_BY_CREATE_TIME_ASC
        : FIND_BY_STATUS_AND_TYPE_NOT_IN_ORDER_BY_CREATE_TIME_DESC;
    list = genericPojoDao.executeParameterizedSQL(queryClause, parameterMap, TaskBean.class);
    List<TaskDTO> result = new ArrayList<>();
    for (TaskBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, TaskDTO.class));
    }
    return result;
  }

  @Override
  public boolean updateStatusAndWorkerId(Long workerId, Long id, Set<TaskStatus> permittedOldStatus,
      int expectedVersion) {
    TaskDTO task = findById(id);
    if (permittedOldStatus.contains(task.getStatus())) {
      task.setStatus(TaskStatus.RUNNING);
      task.setWorkerId(workerId);
      task.setStartTime(System.currentTimeMillis());
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
  public void updateTaskStartTime(Long id, Long taskStartTime) {
    TaskDTO task = findById(id);
    task.setStartTime(taskStartTime);
    save(task);
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
  public List<TaskDTO> findByStatusesAndTypeWithinDays(List<TaskStatus> statuses,
      TaskConstants.TaskType type, int days) {
    DateTime activeDate = new DateTime().minusDays(days);
    Timestamp activeTimestamp = new Timestamp(activeDate.getMillis());
    Predicate statusPredicate = Predicate.IN("status", statuses.stream().map(Enum::toString).toArray());
    Predicate typePredicate = Predicate.EQ("type", type.toString());
    Predicate timestampPredicate = Predicate.GE("createTime", activeTimestamp);
    return findByPredicate(Predicate.AND(statusPredicate, typePredicate, timestampPredicate));
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

  @Override
  public List<TaskDTO> findByStatusAndWorkerId(Long workerId, TaskStatus status) {
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    Predicate workerIdPredicate = Predicate.EQ("workerId", workerId);
    return findByPredicate(Predicate.AND(statusPredicate, workerIdPredicate));
  }

  @Override
  public int countWaiting() {
    // NOTE: this aggregation should be supported by genericPojoDAO directly
    // ensure each resource is closed at the end of the statement
    try (Connection connection = this.genericPojoDao.getConnection();
        PreparedStatement statement = connection.prepareStatement(COUNT_WAITING_TASKS);
        ResultSet rs = statement.executeQuery()){
      rs.next();
      return rs.getInt(1);
    } catch (Exception e) {
      LOG.warn("Could not retrieve task backlog size. Defaulting to -1.", e);
      return -1;
    }
  }

  @Override
  public void populateDetectionConfig(DetectionConfigDTO detectionConfigDTO, DetectionPipelineTaskInfo taskInfo) {
    DetectionConfigBean bean = convertDTO2Bean(detectionConfigDTO, DetectionConfigBean.class);
    taskInfo.setDetectionConfigBean(bean);
  }

  @Override
  public DetectionConfigDTO extractDetectionConfig(DetectionPipelineTaskInfo taskInfo) {
    DetectionConfigBean detectionConfigBean = taskInfo.getDetectionConfigBean();
    return MODEL_MAPPER.map(detectionConfigBean, DetectionConfigDTO.class);
  }
}
