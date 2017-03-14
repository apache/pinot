package com.linkedin.thirdeye.datalayer.dao;

import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricUtil;
import com.linkedin.thirdeye.datalayer.entity.AlertConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.EventIndex;
import com.linkedin.thirdeye.datalayer.entity.FunctionAutotuneConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.OverrideConfigIndex;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.EventBean;
import com.linkedin.thirdeye.datalayer.pojo.AutotuneConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.OverrideConfigBean;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections.CollectionUtils;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.linkedin.thirdeye.datalayer.entity.AbstractEntity;
import com.linkedin.thirdeye.datalayer.entity.AbstractIndexEntity;
import com.linkedin.thirdeye.datalayer.entity.AbstractJsonEntity;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFeedbackIndex;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunctionIndex;
import com.linkedin.thirdeye.datalayer.entity.DashboardConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.DataCompletenessConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.DatasetConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.DetectionStatusIndex;
import com.linkedin.thirdeye.datalayer.entity.EmailConfigurationIndex;
import com.linkedin.thirdeye.datalayer.entity.GenericJsonEntity;
import com.linkedin.thirdeye.datalayer.entity.IngraphDashboardConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.IngraphMetricConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.JobIndex;
import com.linkedin.thirdeye.datalayer.entity.MergedAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.MetricConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.RawAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.TaskIndex;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.DataCompletenessConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.DetectionStatusBean;
import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;
import com.linkedin.thirdeye.datalayer.pojo.IngraphDashboardConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.IngraphMetricConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.JobBean;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.pojo.TaskBean;
import com.linkedin.thirdeye.datalayer.util.GenericResultSetMapper;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datalayer.util.SqlQueryBuilder;

public class GenericPojoDao {
  private static final Logger LOG = LoggerFactory.getLogger(GenericPojoDao.class);
  private static boolean IS_DEBUG = LOG.isDebugEnabled();

  static Map<Class<? extends AbstractBean>, PojoInfo> pojoInfoMap =
      new HashMap<Class<? extends AbstractBean>, GenericPojoDao.PojoInfo>();

  static String DEFAULT_BASE_TABLE_NAME = "GENERIC_JSON_ENTITY";

  static {
    pojoInfoMap.put(AnomalyFeedbackBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyFeedbackIndex.class));
    pojoInfoMap.put(AnomalyFunctionBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyFunctionIndex.class));
    pojoInfoMap.put(EmailConfigurationBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, EmailConfigurationIndex.class));
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
    pojoInfoMap.put(DashboardConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, DashboardConfigIndex.class));
    pojoInfoMap.put(IngraphDashboardConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, IngraphDashboardConfigIndex.class));
    pojoInfoMap.put(IngraphMetricConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, IngraphMetricConfigIndex.class));
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
    pojoInfoMap.put(AutotuneConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, FunctionAutotuneConfigIndex.class));

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
    ThirdeyeMetricUtil.dbCallCounter.inc();
    // ensure to close the connection
    return dataSource.getConnection();
  }

  public <E extends AbstractBean> Long put(E pojo) {
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
  }

  public <E extends AbstractBean> int update(E pojo) {
    return update(pojo, Predicate.EQ("id", pojo.getId()));
  }

  public <E extends AbstractBean> int update(E pojo, final Predicate predicate) {
    //update base table
    //update indexes
    return runTask(new QueryTask<Integer>() {
      @Override
      public Integer handle(Connection connection) throws Exception {
        PojoInfo pojoInfo = pojoInfoMap.get(pojo.getClass());
        String jsonVal = OBJECT_MAPPER.writeValueAsString(pojo);
        AbstractJsonEntity genericJsonEntity = new GenericJsonEntity();
        genericJsonEntity.setUpdateTime(new Timestamp(System.currentTimeMillis()));
        genericJsonEntity.setJsonVal(jsonVal);
        genericJsonEntity.setId(pojo.getId());
        genericJsonEntity.setVersion(pojo.getVersion());
        dumpTable(connection, GenericJsonEntity.class);
        Set<String> fieldsToUpdate = Sets.newHashSet("jsonVal", "updateTime", "version");
        int affectedRows;
        try (PreparedStatement baseTableInsertStmt =
            sqlQueryBuilder.createUpdateStatement(connection, genericJsonEntity, fieldsToUpdate, predicate)) {
          affectedRows = baseTableInsertStmt.executeUpdate();
        }
        if (affectedRows == 1) {
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

    }, 0);
  }

  public <E extends AbstractBean> List<E> getAll(Class<E> beanClass) {
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
            E e = OBJECT_MAPPER.readValue(entity.getJsonVal(), beanClass);
            e.setId(entity.getId());
            ret.add(e);
          }
        }
        return ret;
      }
    }, Collections.emptyList());
  }

  public <E extends AbstractBean> E get(Long id, Class<E> pojoClass) {
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
          e = OBJECT_MAPPER.readValue(genericJsonEntity.getJsonVal(), pojoClass);
          e.setId(genericJsonEntity.getId());
          e.setVersion(genericJsonEntity.getVersion());
        }
        return e;
      }
    }, null);
  }

  public <E extends AbstractBean> List<E> get(List<Long> idList, Class<E> pojoClass) {
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
            E e = OBJECT_MAPPER.readValue(genericJsonEntity.getJsonVal(), pojoClass);
            e.setId(genericJsonEntity.getId());
            e.setVersion(genericJsonEntity.getVersion());
            result.add(e);
          }
        }
        return result;
      }
    }, Collections.emptyList());
  }
  /**
   *
   * @param parameterizedSQL second part of the sql (omit select from table section)
   * @param parameterMap
   * @return
   */
  public <E extends AbstractBean> List<E> executeParameterizedSQL(String parameterizedSQL,
      Map<String, Object> parameterMap, Class<E> pojoClass) {
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
              E bean = OBJECT_MAPPER.readValue(entity.getJsonVal(), pojoClass);
              bean.setId(entity.getId());
              bean.setVersion(entity.getVersion());
              ret.add(bean);
            }
          }
        }
        return ret;

      }
    }, Collections.emptyList());
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

  public <E extends AbstractBean> List<E> get(Predicate predicate, Class<E> pojoClass) {
    //apply the predicates and fetch the primary key ids
    //look up the id and convert them to bean
    return runTask(new QueryTask<List<E>>() {
      @Override
      public List<E> handle(Connection connection) throws Exception {
        PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
        //find the matching ids to delete
        List<? extends AbstractIndexEntity> indexEntities;
        try (PreparedStatement findByParamsStatement = sqlQueryBuilder
            .createFindByParamsStatement(connection, pojoInfo.indexEntityClass, predicate)) {
          try (ResultSet rs = findByParamsStatement.executeQuery()) {
            indexEntities = genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
          }
        }
        List<Long> idsToFind = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(indexEntities)) {
          for (AbstractIndexEntity entity : indexEntities) {
            idsToFind.add(entity.getBaseId());
          }
        }
        dumpTable(connection, pojoInfo.indexEntityClass);
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
                E bean = OBJECT_MAPPER.readValue(entity.getJsonVal(), pojoClass);
                bean.setId(entity.getId());
                bean.setVersion(entity.getVersion());
                ret.add(bean);
              }
            }
          }
        }
        return ret;
      }

    }, Collections.emptyList());
  }

  private void dumpTable(Connection connection, Class<? extends AbstractEntity> entityClass)
      throws Exception {
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
  }

  public <E extends AbstractBean> int delete(Long id, Class<E> pojoClass) {
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
  }

  public <E extends AbstractBean> int deleteByParams(Map<String, Object> filters,
      Class<E> pojoClass) {
    return runTask(new QueryTask<Integer>() {
      @Override
      public Integer handle(Connection connection) throws Exception {
        PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
        //find the matching ids to delete
        List<? extends AbstractIndexEntity> indexEntities;
        try (PreparedStatement findByParamsStatement = sqlQueryBuilder
            .createFindByParamsStatement(connection, pojoInfo.indexEntityClass, filters)) {
          try (ResultSet rs = findByParamsStatement.executeQuery()) {
            indexEntities = genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
          }
        }

        List<Long> idsToDelete = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(indexEntities)) {
          for (AbstractIndexEntity entity : indexEntities) {
            idsToDelete.add(entity.getBaseId());
          }
        }
        int baseRowsDeleted = 0;
        if (!idsToDelete.isEmpty()) {
          //delete the ids from both base table and index table
          int indexRowsDeleted = 0;
          try (PreparedStatement statement = sqlQueryBuilder.createDeleteStatement(connection,
              pojoInfo.indexEntityClass, idsToDelete)) {
            indexRowsDeleted = statement.executeUpdate();
          }
          try (PreparedStatement baseTableDeleteStatement = sqlQueryBuilder
              .createDeleteStatement(connection, GenericJsonEntity.class, idsToDelete)) {
            baseRowsDeleted = baseTableDeleteStatement.executeUpdate();
          }
          assert (baseRowsDeleted == indexRowsDeleted);
        }
        return baseRowsDeleted;

      }
    }, 0);
  }


  private static interface QueryTask<T> {
    T handle(Connection connection) throws Exception;
  }

  <T> T runTask(QueryTask<T> task, T defaultReturnValue) {
    try (Connection connection = getConnection()) {
      T t = task.handle(connection);
      connection.commit();
      return t;
    } catch (Exception e) {
      LOG.error("Exception while executing query task", e);
      return defaultReturnValue;
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
