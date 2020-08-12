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

package org.apache.pinot.thirdeye.datalayer.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractEntity;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractIndexEntity;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractJsonEntity;
import org.apache.pinot.thirdeye.datalayer.entity.AlertConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AlertSnapshotIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AnomalyFeedbackIndex;
import org.apache.pinot.thirdeye.datalayer.entity.AnomalyFunctionIndex;
import org.apache.pinot.thirdeye.datalayer.entity.ApplicationIndex;
import org.apache.pinot.thirdeye.datalayer.entity.ClassificationConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.ConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DataCompletenessConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DatasetConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DetectionAlertConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DetectionConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.DetectionStatusIndex;
import org.apache.pinot.thirdeye.datalayer.entity.EntityToEntityMappingIndex;
import org.apache.pinot.thirdeye.datalayer.entity.EvaluationIndex;
import org.apache.pinot.thirdeye.datalayer.entity.EventIndex;
import org.apache.pinot.thirdeye.datalayer.entity.GenericJsonEntity;
import org.apache.pinot.thirdeye.datalayer.entity.GroupedAnomalyResultsIndex;
import org.apache.pinot.thirdeye.datalayer.entity.JobIndex;
import org.apache.pinot.thirdeye.datalayer.entity.MergedAnomalyResultIndex;
import org.apache.pinot.thirdeye.datalayer.entity.MetricConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.OnboardDatasetMetricIndex;
import org.apache.pinot.thirdeye.datalayer.entity.OnlineDetectionDataIndex;
import org.apache.pinot.thirdeye.datalayer.entity.OverrideConfigIndex;
import org.apache.pinot.thirdeye.datalayer.entity.RawAnomalyResultIndex;
import org.apache.pinot.thirdeye.datalayer.entity.RootcauseSessionIndex;
import org.apache.pinot.thirdeye.datalayer.entity.RootcauseTemplateIndex;
import org.apache.pinot.thirdeye.datalayer.entity.SessionIndex;
import org.apache.pinot.thirdeye.datalayer.entity.TaskIndex;
import org.apache.pinot.thirdeye.datalayer.pojo.AbstractBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertSnapshotBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import org.apache.pinot.thirdeye.datalayer.pojo.ApplicationBean;
import org.apache.pinot.thirdeye.datalayer.pojo.ClassificationConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.ConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DataCompletenessConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DatasetConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionAlertConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionStatusBean;
import org.apache.pinot.thirdeye.datalayer.pojo.EntityToEntityMappingBean;
import org.apache.pinot.thirdeye.datalayer.pojo.EvaluationBean;
import org.apache.pinot.thirdeye.datalayer.pojo.EventBean;
import org.apache.pinot.thirdeye.datalayer.pojo.GroupedAnomalyResultsBean;
import org.apache.pinot.thirdeye.datalayer.pojo.JobBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.OnboardDatasetMetricBean;
import org.apache.pinot.thirdeye.datalayer.pojo.OnlineDetectionDataBean;
import org.apache.pinot.thirdeye.datalayer.pojo.OverrideConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import org.apache.pinot.thirdeye.datalayer.pojo.RootcauseSessionBean;
import org.apache.pinot.thirdeye.datalayer.pojo.RootcauseTemplateBean;
import org.apache.pinot.thirdeye.datalayer.pojo.SessionBean;
import org.apache.pinot.thirdeye.datalayer.pojo.TaskBean;
import org.apache.pinot.thirdeye.datalayer.util.GenericResultSetMapper;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datalayer.util.SqlQueryBuilder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.collections4.CollectionUtils;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericPojoDao {
  private static final Logger LOG = LoggerFactory.getLogger(GenericPojoDao.class);
  private static boolean IS_DEBUG = LOG.isDebugEnabled();
  private static int MAX_BATCH_SIZE = 1000;

  static Map<Class<? extends AbstractBean>, PojoInfo> pojoInfoMap =
      new HashMap<Class<? extends AbstractBean>, GenericPojoDao.PojoInfo>();

  static String DEFAULT_BASE_TABLE_NAME = "GENERIC_JSON_ENTITY";

  static {
    pojoInfoMap.put(AnomalyFeedbackBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyFeedbackIndex.class));
    pojoInfoMap.put(AnomalyFunctionBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyFunctionIndex.class));
    pojoInfoMap.put(JobBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, JobIndex.class));
    pojoInfoMap.put(TaskBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, TaskIndex.class));
    pojoInfoMap.put(MergedAnomalyResultBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, MergedAnomalyResultIndex.class));
    pojoInfoMap.put(RawAnomalyResultBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, RawAnomalyResultIndex.class));
    pojoInfoMap.put(DatasetConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, DatasetConfigIndex.class));
    pojoInfoMap.put(MetricConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, MetricConfigIndex.class));
    pojoInfoMap.put(OverrideConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, OverrideConfigIndex.class));
    pojoInfoMap.put(EventBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, EventIndex.class));
    pojoInfoMap.put(AlertConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AlertConfigIndex.class));
    pojoInfoMap.put(DataCompletenessConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, DataCompletenessConfigIndex.class));
    pojoInfoMap.put(DetectionStatusBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, DetectionStatusIndex.class));
    pojoInfoMap.put(ClassificationConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, ClassificationConfigIndex.class));
    pojoInfoMap.put(EntityToEntityMappingBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, EntityToEntityMappingIndex.class));
    pojoInfoMap.put(GroupedAnomalyResultsBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, GroupedAnomalyResultsIndex.class));
    pojoInfoMap.put(OnboardDatasetMetricBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, OnboardDatasetMetricIndex.class));
    pojoInfoMap.put(ConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, ConfigIndex.class));
    pojoInfoMap.put(ApplicationBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, ApplicationIndex.class));
    pojoInfoMap.put(AlertSnapshotBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AlertSnapshotIndex.class));
    pojoInfoMap.put(RootcauseSessionBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, RootcauseSessionIndex.class));
    pojoInfoMap.put(SessionBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, SessionIndex.class));
    pojoInfoMap.put(DetectionConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, DetectionConfigIndex.class));
    pojoInfoMap.put(DetectionAlertConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, DetectionAlertConfigIndex.class));
    pojoInfoMap.put(EvaluationBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, EvaluationIndex.class));
    pojoInfoMap.put(RootcauseTemplateBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, RootcauseTemplateIndex.class));
    pojoInfoMap.put(OnlineDetectionDataBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, OnlineDetectionDataIndex.class));
  }

  private static PojoInfo newPojoInfo(String baseTableName,
      Class<? extends AbstractIndexEntity> indexEntityClass) {
    PojoInfo pojoInfo = new PojoInfo();
    pojoInfo.baseTableName = baseTableName;
    pojoInfo.indexEntityClass = indexEntityClass;
    return pojoInfo;
  }

  @Inject
  DataSource dataSource;

  @Inject
  SqlQueryBuilder sqlQueryBuilder;

  @Inject
  GenericResultSetMapper genericResultSetMapper;

  static ModelMapper MODEL_MAPPER = new ModelMapper();
  static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public GenericPojoDao() {
  }

  /**
   * Use at your own risk!!! Ensure to close the connection after using it or it can cause a leak.
   *
   * @return
   *
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    // ensure to close the connection
    return dataSource.getConnection();
  }

  private String getCurrentPrincipal() {
    // TODO use injection
    ThirdEyePrincipal principal = ThirdEyeAuthFilter.getCurrentPrincipal();
    if (principal != null) {
      return principal.getName();
    }
    return "no-auth-user";
  }

  public <E extends AbstractBean> Long put(final E pojo) {
    long tStart = System.nanoTime();
    String currentUser = getCurrentPrincipal();
    pojo.setCreatedBy(currentUser);
    pojo.setUpdatedBy(currentUser);
    try {
      //insert into its base table
      //get the generated id
      //update indexes
      return runTask(new QueryTask<Long>() {
        @Override
        public Long handle(Connection connection) throws Exception {
          PojoInfo pojoInfo = pojoInfoMap.get(pojo.getClass());
          AbstractJsonEntity genericJsonEntity = new GenericJsonEntity();
          genericJsonEntity.setCreateTime(new Timestamp(System.currentTimeMillis()));
          genericJsonEntity.setUpdateTime(new Timestamp(System.currentTimeMillis()));
          genericJsonEntity.setVersion(1);
          genericJsonEntity.setBeanClass(pojo.getClass().getName());
          String jsonVal = OBJECT_MAPPER.writeValueAsString(pojo);
          genericJsonEntity.setJsonVal(jsonVal);
          ThirdeyeMetricsUtil.dbWriteByteCounter.inc(jsonVal.length());

          try (PreparedStatement baseTableInsertStmt =
              sqlQueryBuilder.createInsertStatement(connection, genericJsonEntity)) {
            int affectedRows = baseTableInsertStmt.executeUpdate();
            if (affectedRows == 1) {
              try (ResultSet generatedKeys = baseTableInsertStmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                  pojo.setId(generatedKeys.getLong(1));
                }
              }
              if (pojoInfo.indexEntityClass != null) {
                AbstractIndexEntity abstractIndexEntity = pojoInfo.indexEntityClass.newInstance();
                MODEL_MAPPER.map(pojo, abstractIndexEntity);
                abstractIndexEntity.setBaseId(pojo.getId());
                abstractIndexEntity.setCreateTime(new Timestamp(System.currentTimeMillis()));
                abstractIndexEntity.setUpdateTime(new Timestamp(System.currentTimeMillis()));
                abstractIndexEntity.setVersion(1);
                int numRowsCreated;
                try (PreparedStatement indexTableInsertStatement = sqlQueryBuilder.createInsertStatement(connection,
                    abstractIndexEntity)) {
                  numRowsCreated = indexTableInsertStatement.executeUpdate();
                }
                if (numRowsCreated == 1) {
                  return pojo.getId();
                }
              } else {
                return pojo.getId();
              }
            }
          }
          return null;
        }

      }, null);
    } finally {
      ThirdeyeMetricsUtil.dbWriteCallCounter.inc();
      ThirdeyeMetricsUtil.dbWriteDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  /**
   * Update the list of pojos in transaction mode. Every transaction contains MAX_BATCH_SIZE of entries. By default,
   * this method update the entries in one transaction. If any entries cause an exception, this method will
   * update the entries one-by-one (i.e., in separated transactions) and skip the one that causes exceptions.
   *
   * @param pojos the pojo to be updated, whose ID cannot be null; otherwise, it will be ignored.
   *
   * @return the number of rows that are affected.
   */
  public <E extends AbstractBean> int update(final List<E> pojos) {
    if (CollectionUtils.isEmpty(pojos)) {
      return 0;
    }

    long tStart = System.nanoTime();
    String updateName = getCurrentPrincipal();
    for (E pojo : pojos) {
      pojo.setUpdatedBy(updateName);
    }
    try {
      return runTask(new QueryTask<Integer>() {
        @Override
        public Integer handle(Connection connection) throws Exception {
          if (CollectionUtils.isEmpty(pojos)) {
            return 0;
          }

          int updateCounter = 0;
          int minIdx = 0;
          int maxIdx = MAX_BATCH_SIZE;
          boolean isAutoCommit = connection.getAutoCommit();
          // Ensure that transaction mode is enabled
          connection.setAutoCommit(false);
          while (minIdx < pojos.size()) {
            List<E> subList = pojos.subList(minIdx, Math.min(maxIdx, pojos.size()));
            try {
              for (E pojo : subList) {
                Preconditions.checkNotNull(pojo.getId());
                addUpdateToConnection(pojo, Predicate.EQ("id", pojo.getId()), connection);
              }
              // Trigger commit() to ensure this batch of deletion is executed
              connection.commit();
              updateCounter += subList.size();
            } catch (Exception e) {
              // Error recovery: rollback previous changes.
              connection.rollback();
              // Unable to do batch because of exception; fall back to single row deletion mode.
              for (final E pojo : subList) {
                try {
                  int updateRow = addUpdateToConnection(pojo, Predicate.EQ("id", pojo.getId()), connection);
                  connection.commit();
                  updateCounter += updateRow;
                } catch (Exception e1) {
                  connection.rollback();
                  LOG.error("Exception while executing query task; skipping entity (id={})", pojo.getId(), e);
                }
              }
            }
            minIdx = maxIdx;
            maxIdx = maxIdx + MAX_BATCH_SIZE;
          }
          // Restore the original state of connection's auto commit
          connection.setAutoCommit(isAutoCommit);
          return updateCounter;
        }
      }, 0);
    } finally {
      ThirdeyeMetricsUtil.dbWriteCallCounter.inc();
      ThirdeyeMetricsUtil.dbWriteDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> int update(E pojo) {
    if (pojo.getId() == null) {
      throw new IllegalArgumentException(String.format("Need an ID to update the DB entity: %s", pojo.toString()));
    }
    return update(pojo, Predicate.EQ("id", pojo.getId()));
  }

  public <E extends AbstractBean> int update(final E pojo, final Predicate predicate) {
    long tStart = System.nanoTime();
    pojo.setUpdatedBy(getCurrentPrincipal());
    try {
      return runTask(new QueryTask<Integer>() {
        @Override
        public Integer handle(Connection connection) throws Exception {
          return addUpdateToConnection(pojo, predicate, connection);
        }
      }, 0);
    } finally {
      ThirdeyeMetricsUtil.dbWriteCallCounter.inc();
      ThirdeyeMetricsUtil.dbWriteDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  private <E extends AbstractBean> int addUpdateToConnection(final E pojo, final Predicate predicate,
      Connection connection) throws Exception {
    //update base table
    String jsonVal = OBJECT_MAPPER.writeValueAsString(pojo);
    AbstractJsonEntity genericJsonEntity = new GenericJsonEntity();
    genericJsonEntity.setUpdateTime(new Timestamp(System.currentTimeMillis()));
    genericJsonEntity.setJsonVal(jsonVal);
    genericJsonEntity.setId(pojo.getId());
    genericJsonEntity.setVersion(pojo.getVersion());
    PojoInfo pojoInfo = pojoInfoMap.get(pojo.getClass());
    dumpTable(connection, pojoInfo.indexEntityClass);
    Set<String> fieldsToUpdate = Sets.newHashSet("jsonVal", "updateTime", "version");
    int affectedRows;
    try (PreparedStatement baseTableInsertStmt =
        sqlQueryBuilder.createUpdateStatement(connection, genericJsonEntity, fieldsToUpdate, predicate)) {
      affectedRows = baseTableInsertStmt.executeUpdate();
    }

    //update indexes
    if (affectedRows == 1) {
      ThirdeyeMetricsUtil.dbWriteByteCounter.inc(jsonVal.length());
      if (pojoInfo.indexEntityClass != null) {
        AbstractIndexEntity abstractIndexEntity = pojoInfo.indexEntityClass.newInstance();
        MODEL_MAPPER.map(pojo, abstractIndexEntity);
        abstractIndexEntity.setBaseId(pojo.getId());
        abstractIndexEntity.setUpdateTime(new Timestamp(System.currentTimeMillis()));
        //updates all columns in the index table by default
        try (PreparedStatement indexTableInsertStatement =
            sqlQueryBuilder.createUpdateStatementForIndexTable(connection, abstractIndexEntity)) {
          int numRowsUpdated = indexTableInsertStatement.executeUpdate();
          LOG.debug("numRowsUpdated: {}", numRowsUpdated);
          return numRowsUpdated;
        }
      }
    }

    return affectedRows;
  }

  public <E extends AbstractBean> List<E> getAll(final Class<E> beanClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(new QueryTask<List<E>>() {
        @Override
        public List<E> handle(Connection connection) throws Exception {
          Predicate predicate = Predicate.EQ("beanClass", beanClass.getName());
          List<GenericJsonEntity> entities;
          try (PreparedStatement selectStatement = sqlQueryBuilder.createFindByParamsStatement(connection,
              GenericJsonEntity.class, predicate)) {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
              entities = genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
            }
          }
          List<E> ret = new ArrayList<>();
          if (CollectionUtils.isNotEmpty(entities)) {
            for (GenericJsonEntity entity : entities) {
              ThirdeyeMetricsUtil.dbReadByteCounter.inc(entity.getJsonVal().length());

              E e = OBJECT_MAPPER.readValue(entity.getJsonVal(), beanClass);
              e.setId(entity.getId());
              e.setUpdateTime(entity.getUpdateTime());
              ret.add(e);
            }
          }
          return ret;
        }
      }, Collections.<E>emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> List<E> list(final Class<E> beanClass, long limit, long offset) {
    long tStart = System.nanoTime();
    try {
      return runTask(connection -> {
        List<GenericJsonEntity> entities;
        Predicate predicate = Predicate.EQ("beanClass", beanClass.getName());
        try (PreparedStatement selectStatement =
            sqlQueryBuilder.createfindByParamsStatementWithLimit(connection, GenericJsonEntity.class, predicate, limit, offset)) {
          try (ResultSet resultSet = selectStatement.executeQuery()) {
            entities = genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
          }
        }
        List<E> result = new ArrayList<>();
        if (entities != null) {
          for (GenericJsonEntity entity : entities) {
            ThirdeyeMetricsUtil.dbReadByteCounter.inc(entity.getJsonVal().length());
            E e = OBJECT_MAPPER.readValue(entity.getJsonVal(), beanClass);
            e.setId(entity.getId());
            e.setUpdateTime(entity.getUpdateTime());
            result.add(e);
          }
        }
        return result;
      }, Collections.emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> List<E> getByPredicateJsonVal(Predicate predicate, final Class<E> beanClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(connection -> {
        List<GenericJsonEntity> entities;
        Predicate p = Predicate.AND(predicate, Predicate.EQ("beanClass", beanClass.getName()));
        try (PreparedStatement selectStatement =
            sqlQueryBuilder.createFindByParamsStatement(connection, GenericJsonEntity.class, p)) {
          try (ResultSet resultSet = selectStatement.executeQuery()) {
            entities = genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
          }
        }
        List<E> result = new ArrayList<>();
        if (entities != null) {
          for (GenericJsonEntity entity : entities) {
            ThirdeyeMetricsUtil.dbReadByteCounter.inc(entity.getJsonVal().length());
            E e = OBJECT_MAPPER.readValue(entity.getJsonVal(), beanClass);
            e.setId(entity.getId());
            e.setUpdateTime(entity.getUpdateTime());
            result.add(e);
          }
        }
        return result;
      }, Collections.emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> long count(final Class<E> beanClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(connection -> {
        PojoInfo pojoInfo = pojoInfoMap.get(beanClass);
        try (PreparedStatement selectStatement =
            sqlQueryBuilder.createCountStatement(connection, pojoInfo.indexEntityClass)) {
          try (ResultSet resultSet = selectStatement.executeQuery()) {
            if (resultSet.next()) {
              return resultSet.getInt(1);
            } else {
              throw new IllegalStateException("can't parse count query response");
            }
          }
        }
      }, -1);
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> E get(final Long id, final Class<E> pojoClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(new QueryTask<E>() {
        @Override
        public E handle(Connection connection) throws Exception {
          GenericJsonEntity genericJsonEntity;
          try (PreparedStatement selectStatement =
              sqlQueryBuilder.createFindByIdStatement(connection, GenericJsonEntity.class, id)) {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
              genericJsonEntity = genericResultSetMapper.mapSingle(resultSet, GenericJsonEntity.class);
            }
          }
          E e = null;
          if (genericJsonEntity != null) {
            ThirdeyeMetricsUtil.dbReadByteCounter.inc(genericJsonEntity.getJsonVal().length());

            e = OBJECT_MAPPER.readValue(genericJsonEntity.getJsonVal(), pojoClass);
            e.setId(genericJsonEntity.getId());
            e.setVersion(genericJsonEntity.getVersion());
            e.setUpdateTime(genericJsonEntity.getUpdateTime());
          }
          return e;
        }
      }, null);
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> List<E> get(final List<Long> idList, final Class<E> pojoClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(new QueryTask<List<E>>() {
        @Override
        public List<E> handle(Connection connection) throws Exception {
          List<GenericJsonEntity> genericJsonEntities;
          try (PreparedStatement selectStatement =
              sqlQueryBuilder.createFindByIdStatement(connection, GenericJsonEntity.class, idList)) {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
              genericJsonEntities = genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
            }
          }
          List<E> result = new ArrayList<>();
          if (CollectionUtils.isNotEmpty(genericJsonEntities)) {
            for (GenericJsonEntity genericJsonEntity : genericJsonEntities) {
              ThirdeyeMetricsUtil.dbReadByteCounter.inc(genericJsonEntity.getJsonVal().length());

              E e = OBJECT_MAPPER.readValue(genericJsonEntity.getJsonVal(), pojoClass);
              e.setId(genericJsonEntity.getId());
              e.setVersion(genericJsonEntity.getVersion());
              e.setUpdateTime(genericJsonEntity.getUpdateTime());
              result.add(e);
            }
          }
          return result;
        }
      }, Collections.<E>emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }
  /**
   *
   * @param parameterizedSQL second part of the sql (omit select from table section)
   * @param parameterMap
   * @return
   */
  public <E extends AbstractBean> List<E> executeParameterizedSQL(final String parameterizedSQL,
      final Map<String, Object> parameterMap, final Class<E> pojoClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(new QueryTask<List<E>>() {
        @Override
        public List<E> handle(Connection connection) throws Exception {
          PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
          dumpTable(connection, pojoInfo.indexEntityClass);
          List<? extends AbstractIndexEntity> indexEntities;
          try (PreparedStatement findMatchingIdsStatement = sqlQueryBuilder.createStatementFromSQL(
              connection, parameterizedSQL, parameterMap, pojoInfo.indexEntityClass)) {
            try (ResultSet rs = findMatchingIdsStatement.executeQuery()) {
              indexEntities = genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
            }
          }
          List<Long> idsToFind = new ArrayList<>();
          if (CollectionUtils.isNotEmpty(indexEntities)) {
            for (AbstractIndexEntity entity : indexEntities) {
              idsToFind.add(entity.getBaseId());
            }
          }
          List<E> ret = new ArrayList<>();
          //fetch the entities
          if (!idsToFind.isEmpty()) {
            List<GenericJsonEntity> entities;
            try (PreparedStatement selectStatement = sqlQueryBuilder.createFindByIdStatement(connection,
                GenericJsonEntity.class, idsToFind)) {
              try (ResultSet resultSet = selectStatement.executeQuery()) {
                entities = genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
              }
            }
            if (CollectionUtils.isNotEmpty(entities)) {
              for (GenericJsonEntity entity : entities) {
                ThirdeyeMetricsUtil.dbReadByteCounter.inc(entity.getJsonVal().length());

                E bean = OBJECT_MAPPER.readValue(entity.getJsonVal(), pojoClass);
                bean.setId(entity.getId());
                bean.setVersion(entity.getVersion());
                ret.add(bean);
              }
            }
          }
          return ret;

        }
      }, Collections.<E>emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }


  public <E extends AbstractBean> List<E> get(Map<String, Object> filterParams,
      Class<E> pojoClass) {
    Predicate[] childPredicates = new Predicate[filterParams.size()];
    int index = 0;
    for (Entry<String, Object> entry : filterParams.entrySet()) {
      childPredicates[index] = Predicate.EQ(entry.getKey(), entry.getValue());
      index = index + 1;
    }
    return get(Predicate.AND(childPredicates), pojoClass);
  }

  public <E extends AbstractBean> List<E> get(final Predicate predicate, final Class<E> pojoClass) {
    final List<Long> idsToFind = getIdsByPredicate(predicate, pojoClass);
    long tStart = System.nanoTime();
    try {
      //apply the predicates and fetch the primary key ids
      //look up the id and convert them to bean
      return runTask(new QueryTask<List<E>>() {
        @Override
        public List<E> handle(Connection connection) throws Exception {
          //fetch the entities
          List<E> ret = new ArrayList<>();
          if (!idsToFind.isEmpty()) {
            List<GenericJsonEntity> entities;
            try (PreparedStatement selectStatement = sqlQueryBuilder.createFindByIdStatement(connection,
                GenericJsonEntity.class, idsToFind)) {
              try (ResultSet resultSet = selectStatement.executeQuery()) {
                entities = genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
              }
              if (CollectionUtils.isNotEmpty(entities)) {
                for (GenericJsonEntity entity : entities) {
                  ThirdeyeMetricsUtil.dbReadByteCounter.inc(entity.getJsonVal().length());

                  E bean = OBJECT_MAPPER.readValue(entity.getJsonVal(), pojoClass);
                  bean.setId(entity.getId());
                  bean.setVersion(entity.getVersion());
                  bean.setUpdateTime(entity.getUpdateTime());
                  ret.add(bean);
                }
              }
            }
          }
          return ret;
        }

      }, Collections.<E>emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> List<Long> getIdsByPredicate(final Predicate predicate, final Class<E> pojoClass) {
    long tStart = System.nanoTime();
    try {
      //apply the predicates and fetch the primary key ids
      return runTask(new QueryTask<List<Long>>() {
        @Override
        public List<Long> handle(Connection connection) throws Exception {
          PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
          //find the matching ids
          List<? extends AbstractIndexEntity> indexEntities;
          try (PreparedStatement findByParamsStatement = sqlQueryBuilder
              .createFindByParamsStatement(connection, pojoInfo.indexEntityClass, predicate)) {
            try (ResultSet rs = findByParamsStatement.executeQuery()) {
              indexEntities = genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
            }
          }
          List<Long> idsToReturn = new ArrayList<>();
          if (CollectionUtils.isNotEmpty(indexEntities)) {
            for (AbstractIndexEntity entity : indexEntities) {
              idsToReturn.add(entity.getBaseId());
            }
          }
          dumpTable(connection, pojoInfo.indexEntityClass);
          return idsToReturn;
        }
      }, Collections.<Long>emptyList());
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  private void dumpTable(Connection connection, Class<? extends AbstractEntity> entityClass)
      throws Exception {
    long tStart = System.nanoTime();
    try {
      if (IS_DEBUG) {
        try (PreparedStatement findAllStatement =
            sqlQueryBuilder.createFindAllStatement(connection, entityClass)) {
          try (ResultSet resultSet = findAllStatement.executeQuery()) {
            List<? extends AbstractEntity> entities = genericResultSetMapper.mapAll(resultSet, entityClass);
            for (AbstractEntity entity : entities) {
              LOG.debug("{}", entity);
            }
          }
        }
      }
    } finally {
      ThirdeyeMetricsUtil.dbReadCallCounter.inc();
      ThirdeyeMetricsUtil.dbReadDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> int delete(final Long id, final Class<E> pojoClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(new QueryTask<Integer>() {
        @Override
        public Integer handle(Connection connection) throws Exception {
          PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
          Map<String, Object> filters = new HashMap<>();
          filters.put("id", id);
          try (PreparedStatement deleteStatement = sqlQueryBuilder.createDeleteByIdStatement(connection,
              GenericJsonEntity.class, filters)) {
            deleteStatement.executeUpdate();
          }
          filters.clear();
          filters.put("baseId", id);

          try (PreparedStatement deleteIndexStatement = sqlQueryBuilder.createDeleteByIdStatement(connection,
              pojoInfo.indexEntityClass, filters)) {
            return deleteIndexStatement.executeUpdate();
          }
        }
      }, 0);
    } finally {
      ThirdeyeMetricsUtil.dbWriteCallCounter.inc();
      ThirdeyeMetricsUtil.dbWriteDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  public <E extends AbstractBean> int delete(final List<Long> idsToDelete, final Class<E> pojoClass) {
    long tStart = System.nanoTime();
    try {
      return runTask(new QueryTask<Integer>() {
        @Override
        public Integer handle(Connection connection) throws Exception {
          if (CollectionUtils.isEmpty(idsToDelete)) {
            return 0;
          }

          boolean isAutoCommit = connection.getAutoCommit();
          // Ensure that transaction mode is enabled
          connection.setAutoCommit(false);
          Class<? extends AbstractIndexEntity> indexEntityClass = pojoInfoMap.get(pojoClass).indexEntityClass;
          int updateCounter = 0;
          int minIdx = 0;
          int maxIdx = MAX_BATCH_SIZE;
          while (minIdx < idsToDelete.size()) {
            List<Long> subList = idsToDelete.subList(minIdx, Math.min(maxIdx, idsToDelete.size()));
            try {
              int updatedBaseRow = addBatchDeletionToConnection(subList, indexEntityClass, connection);
              // Trigger commit() to ensure this batch of deletion is executed
              connection.commit();
              updateCounter += updatedBaseRow;
            } catch (Exception e) {
              // Error recovery: rollback previous changes.
              connection.rollback();
              // Unable to do batch because of exception; fall back to single row deletion mode.
              for (final Long pojoId : subList) {
                try {
                  int updatedBaseRow =
                      addBatchDeletionToConnection(Collections.singletonList(pojoId), indexEntityClass, connection);
                  connection.commit();
                  updateCounter += updatedBaseRow;
                } catch (Exception e1) {
                  connection.rollback();
                  LOG.error("Exception while executing query task; skipping entity (id={})", pojoId, e);
                }
              }
            }
            minIdx = Math.min(maxIdx, idsToDelete.size());
            maxIdx += MAX_BATCH_SIZE;
          }
          // Restore the original state of connection's auto commit
          connection.setAutoCommit(isAutoCommit);
          return updateCounter;
        }
      }, 0);
    } finally {
      ThirdeyeMetricsUtil.dbWriteCallCounter.inc();
      ThirdeyeMetricsUtil.dbWriteDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  /**
   * Delete the given ids from base table and index table.
   *
   * @param idsToDelete the IDs to be deleted
   * @param indexEntityClass the index entity of the entities in the ID list; this method assumes that these entities
   *                         to be deleted belong to the same index entity
   * @param connection the connection to the database.
   *
   * @return the number of base rows that are deleted.
   *
   * @throws Exception any exception from DB.
   */
  private <E extends AbstractBean> int addBatchDeletionToConnection(List<Long> idsToDelete,
      Class<? extends AbstractIndexEntity> indexEntityClass, Connection connection) throws Exception {
    try (PreparedStatement statement = sqlQueryBuilder.createDeleteStatement(connection, indexEntityClass, idsToDelete,
        true)) {
      statement.executeUpdate();
    }
    try (PreparedStatement baseTableDeleteStatement = sqlQueryBuilder.createDeleteStatement(connection,
        GenericJsonEntity.class, idsToDelete, false)) {
      return baseTableDeleteStatement.executeUpdate();
    }
  }

  public <E extends AbstractBean> int deleteByPredicate(final Predicate predicate, final Class<E> pojoClass) {
    List<Long> idsToDelete = getIdsByPredicate(predicate, pojoClass);
    return delete(idsToDelete, pojoClass);
  }


  private static interface QueryTask<T> {
    T handle(Connection connection) throws Exception;
  }

  <T> T runTask(QueryTask<T> task, T defaultReturnValue) {
    ThirdeyeMetricsUtil.dbCallCounter.inc();

    Connection connection = null;
    try {
      connection = getConnection();
      // Enable transaction
      connection.setAutoCommit(false);
      T t = task.handle(connection);
      // Commit this transaction
      connection.commit();
      return t;

    } catch (Exception e) {
      LOG.error("Exception while executing query task", e);
      ThirdeyeMetricsUtil.dbExceptionCounter.inc();

      // Rollback transaction in case json table is updated but index table isn't due to any errors (duplicate key, etc.)
      if (connection != null) {
        try {
          connection.rollback();
        } catch (SQLException e1) {
          LOG.error("Failed to rollback SQL execution", e);
        }
      }
      return defaultReturnValue;

    } finally {
      // Always close connection before leaving
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOG.error("Failed to close connection", e);
        }
      }
    }
  }

  static class PojoInfo {
    Class<? extends AbstractBean> pojoClass;
    String pojoName;
    String baseTableName;
    Class<AbstractJsonEntity> baseEntityClass;
    String indexTableName;
    Class<? extends AbstractIndexEntity> indexEntityClass;
    List<String> indexTableColumns;
  }

}
