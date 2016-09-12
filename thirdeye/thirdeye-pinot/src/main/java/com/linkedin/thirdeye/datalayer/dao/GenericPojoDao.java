package com.linkedin.thirdeye.datalayer.dao;

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

import org.codehaus.jackson.map.ObjectMapper;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.linkedin.thirdeye.anomaly.alert.AlertJobScheduler;
import com.linkedin.thirdeye.datalayer.entity.AbstractEntity;
import com.linkedin.thirdeye.datalayer.entity.AbstractIndexEntity;
import com.linkedin.thirdeye.datalayer.entity.AbstractJsonEntity;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFeedbackIndex;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunctionIndex;
import com.linkedin.thirdeye.datalayer.entity.EmailConfigurationIndex;
import com.linkedin.thirdeye.datalayer.entity.GenericJsonEntity;
import com.linkedin.thirdeye.datalayer.entity.JobIndex;
import com.linkedin.thirdeye.datalayer.entity.MergedAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.RawAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.WebappConfigIndex;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;
import com.linkedin.thirdeye.datalayer.pojo.JobBean;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.pojo.WebappConfigBean;
import com.linkedin.thirdeye.datalayer.util.GenericResultSetMapper;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datalayer.util.SqlQueryBuilder;

public class GenericPojoDao {
  private static final Logger LOG = LoggerFactory.getLogger(GenericPojoDao.class);

  static Map<Class<? extends AbstractBean>, PojoInfo> pojoInfoMap =
      new HashMap<Class<? extends AbstractBean>, GenericPojoDao.PojoInfo>();

  static String DEFAULT_BASE_TABLE_NAME = "GENERIC_JSON_ENTITY";

  static {
    pojoInfoMap.put(AnomalyFeedbackBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyFeedbackIndex.class));
    pojoInfoMap.put(AnomalyFunctionBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyFunctionIndex.class));

    //    pojoInfoMap.put(Anomalymergec.class,
    //        newPojoInfo(DEFAULT_BASE_TABLE_NAME, AnomalyMergeConfigIndex.class));
    pojoInfoMap.put(EmailConfigurationBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, EmailConfigurationIndex.class));
    pojoInfoMap.put(JobBean.class, newPojoInfo(DEFAULT_BASE_TABLE_NAME, JobIndex.class));
    pojoInfoMap.put(MergedAnomalyResultBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, MergedAnomalyResultIndex.class));
    pojoInfoMap.put(RawAnomalyResultBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, RawAnomalyResultIndex.class));
    pojoInfoMap.put(WebappConfigBean.class,
        newPojoInfo(DEFAULT_BASE_TABLE_NAME, WebappConfigIndex.class));
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


  public GenericPojoDao() {}

  /**
   * Use at your own risk!!! Ensure to close the connection after using it or it can cause a leak.
   *
   * @return
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
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

        PreparedStatement baseTableInsertStmt =
            sqlQueryBuilder.createInsertStatement(connection, genericJsonEntity);
        int affectedRows = baseTableInsertStmt.executeUpdate();
        if (affectedRows == 1) {
          ResultSet generatedKeys = baseTableInsertStmt.getGeneratedKeys();
          if (generatedKeys.next()) {
            pojo.setId(generatedKeys.getLong(1));
          }
          if (pojoInfo.indexEntityClass != null) {
            AbstractIndexEntity abstractIndexEntity = pojoInfo.indexEntityClass.newInstance();
            MODEL_MAPPER.map(pojo, abstractIndexEntity);
            abstractIndexEntity.setBaseId(pojo.getId());
            PreparedStatement indexTableInsertStatement =
                sqlQueryBuilder.createInsertStatement(connection, abstractIndexEntity);
            int numRowsCreated = indexTableInsertStatement.executeUpdate();
            if (numRowsCreated == 1) {
              return pojo.getId();
            }
          } else {
            return pojo.getId();
          }
        }
        return null;
      }

    }, null);
  }

  public <E extends AbstractBean> int update(E pojo) {
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
        Set<String> fieldsToUpdate = Sets.newHashSet("jsonVal", "updateTime");
        PreparedStatement baseTableInsertStmt =
            sqlQueryBuilder.createUpdateStatement(connection, genericJsonEntity, fieldsToUpdate);
        int affectedRows = baseTableInsertStmt.executeUpdate();
        if (affectedRows == 1) {
          if (pojoInfo.indexEntityClass != null) {
            AbstractIndexEntity abstractIndexEntity = pojoInfo.indexEntityClass.newInstance();
            MODEL_MAPPER.map(pojo, abstractIndexEntity);
            abstractIndexEntity.setBaseId(pojo.getId());
            //updates all columns in the index table by default
            PreparedStatement indexTableInsertStatement =
                sqlQueryBuilder.createUpdateStatementForIndexTable(connection, abstractIndexEntity);
            int numRowsUpdated = indexTableInsertStatement.executeUpdate();
            return numRowsUpdated;
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
        PreparedStatement selectStatement =
            sqlQueryBuilder.createFindByParamsStatement(connection, GenericJsonEntity.class, predicate);
        ResultSet resultSet = selectStatement.executeQuery();
        List<GenericJsonEntity> entities =
            genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
        List<E> ret = new ArrayList<>(entities.size());
        for (GenericJsonEntity entity : entities) {
          ret.add(OBJECT_MAPPER.readValue(entity.getJsonVal(), beanClass));
        }
        return ret;
      }
    }, Collections.emptyList());
  }

  public <E extends AbstractBean> E get(Long id, Class<E> pojoClass) {
    return runTask(new QueryTask<E>() {
      @Override
      public E handle(Connection connection) throws Exception {
        PreparedStatement selectStatement =
            sqlQueryBuilder.createFindByIdStatement(connection, GenericJsonEntity.class, id);
        ResultSet resultSet = selectStatement.executeQuery();
        GenericJsonEntity genericJsonEntity = (GenericJsonEntity) genericResultSetMapper
            .mapSingle(resultSet, GenericJsonEntity.class);
        E e = null;
        if (genericJsonEntity != null) {
          e = OBJECT_MAPPER.readValue(genericJsonEntity.getJsonVal(), pojoClass);
          e.setId(genericJsonEntity.getId());
        }
        return e;
      }
    }, null);
  }

  /**
   * 
   * @param parameterizedSQL second part of the sql (omit select from table section)
   * @param parameterMap
   * @return
   */
  @SuppressWarnings("unchecked")
  public <E extends AbstractBean> List<E> executeParameterizedSQL(String parameterizedSQL,
      Map<String, Object> parameterMap, Class<E> pojoClass) {
    return runTask(new QueryTask<List<E>>() {
      @Override
      public List<E> handle(Connection connection) throws Exception {
        PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
        PreparedStatement findMatchingIdsStatement = sqlQueryBuilder.createStatementFromSQL(
            connection, "select from " + pojoInfo.indexTableColumns + " " + parameterizedSQL,
            parameterMap, pojoInfo.indexEntityClass);
        ResultSet rs = findMatchingIdsStatement.executeQuery();
        List<? extends AbstractIndexEntity> indexEntities =
            genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
        List<Long> idsToFind = new ArrayList<>();
        for (AbstractEntity entity : indexEntities) {
          idsToFind.add(entity.getId());
        }

        //fetch the entities
        PreparedStatement selectStatement =
            sqlQueryBuilder.createFindByIdStatement(connection, GenericJsonEntity.class, idsToFind);
        ResultSet resultSet = selectStatement.executeQuery();
        List<GenericJsonEntity> entities =
            genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
        List<E> ret = new ArrayList<>(entities.size());
        for (GenericJsonEntity entity : entities) {
          ret.add(OBJECT_MAPPER.readValue(entity.getJsonVal(), pojoClass));
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
        PreparedStatement findByParamsStatement = sqlQueryBuilder
            .createFindByParamsStatement(connection, pojoInfo.indexEntityClass, predicate);
        ResultSet rs = findByParamsStatement.executeQuery();
        List<? extends AbstractIndexEntity> indexEntities =
            genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
        List<Long> idsToFind = new ArrayList<>();
        for (AbstractIndexEntity entity : indexEntities) {
          idsToFind.add(entity.getBaseId());
        }
        //fetch the entities
        PreparedStatement selectStatement =
            sqlQueryBuilder.createFindByIdStatement(connection, GenericJsonEntity.class, idsToFind);
        ResultSet resultSet = selectStatement.executeQuery();
        List<GenericJsonEntity> entities =
            genericResultSetMapper.mapAll(resultSet, GenericJsonEntity.class);
        List<E> ret = new ArrayList<>(entities.size());
        for (GenericJsonEntity entity : entities) {
          ret.add(OBJECT_MAPPER.readValue(entity.getJsonVal(), pojoClass));
        }
        return ret;
      }
    }, Collections.emptyList());
  }

  public <E extends AbstractBean> int delete(Long id, Class<E> pojoClass) {
    return runTask(new QueryTask<Integer>() {
      @Override
      public Integer handle(Connection connection) throws Exception {
        PojoInfo pojoInfo = pojoInfoMap.get(pojoClass);
        Map<String, Object> filters = new HashMap<>();
        filters.put("id", id);
        PreparedStatement deleteStatement =
            sqlQueryBuilder.createDeleteByIdStatement(connection, GenericJsonEntity.class, filters);
        deleteStatement.executeUpdate();
        filters.clear();
        filters.put("baseId", id);

        PreparedStatement deleteIndexStatement = sqlQueryBuilder
            .createDeleteByIdStatement(connection, pojoInfo.indexEntityClass, filters);
        return deleteIndexStatement.executeUpdate();
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
        PreparedStatement findByParamsStatement = sqlQueryBuilder
            .createFindByParamsStatement(connection, pojoInfo.indexEntityClass, filters);
        ResultSet rs = findByParamsStatement.executeQuery();
        List<? extends AbstractIndexEntity> indexEntities =
            genericResultSetMapper.mapAll(rs, pojoInfo.indexEntityClass);
        List<Long> idsToDelete = new ArrayList<>();
        for (AbstractIndexEntity entity : indexEntities) {
          idsToDelete.add(entity.getBaseId());
        }
        //delete the ids from both base table and index table
        PreparedStatement statement = sqlQueryBuilder.createDeleteStatement(connection,
            pojoInfo.indexEntityClass, idsToDelete);
        int indexRowsDeleted = statement.executeUpdate();
        PreparedStatement baseTableDeleteStatement =
            sqlQueryBuilder.createDeleteStatement(connection, GenericJsonEntity.class, idsToDelete);
        int baseRowsDeleted = baseTableDeleteStatement.executeUpdate();
        assert (baseRowsDeleted == indexRowsDeleted);
        return baseRowsDeleted;
      }
    }, 0);
  }


  private static interface QueryTask<T> {
    T handle(Connection connection) throws Exception;
  }

  <T> T runTask(QueryTask<T> task, T defaultReturnValue) {
    try (Connection connection = getConnection()) {
      return task.handle(connection);
    } catch (Exception e) {
      e.printStackTrace();
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
