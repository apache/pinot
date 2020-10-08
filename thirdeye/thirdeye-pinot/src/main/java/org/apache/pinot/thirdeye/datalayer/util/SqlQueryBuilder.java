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

package org.apache.pinot.thirdeye.datalayer.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;
import java.lang.reflect.Array;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractEntity;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractIndexEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SqlQueryBuilder {

  private final static Logger LOG = LoggerFactory.getLogger(SqlQueryBuilder.class);

  private static final String BASE_ID = "base_id";
  //insert sql per table
  private final Map<String, String> insertSqlMap = new HashMap<>();
  private static final String NAME_REGEX = "[a-z][_a-z0-9]*";

  private static final String PARAM_REGEX = ":(" + NAME_REGEX + ")";

  private static final Pattern PARAM_PATTERN =
      Pattern.compile(PARAM_REGEX, Pattern.CASE_INSENSITIVE);
  private static final Set<String> AUTO_UPDATE_COLUMN_SET =
      Sets.newHashSet("id", "last_modified");

  private final EntityMappingHolder entityMappingHolder;;

  @Inject
  public SqlQueryBuilder(EntityMappingHolder entityMappingHolder) {
    this.entityMappingHolder = entityMappingHolder;
  }

  public static String generateInsertSql(String tableName,
      LinkedHashMap<String, ColumnInfo> columnInfoMap) {

    StringBuilder values = new StringBuilder(" VALUES");
    StringBuilder names = new StringBuilder("");
    names.append("(");
    values.append("(");
    String delim = "";
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnName = columnInfo.columnNameInDB;
      if (columnInfo.field != null && !AUTO_UPDATE_COLUMN_SET.contains(columnName.toLowerCase())) {
        names.append(delim);
        names.append(columnName);
        values.append(delim);
        values.append("?");
        delim = ",";
      } else {
        LOG.debug("Skipping column " + columnName + " from insert");
      }
    }
    names.append(")");
    values.append(")");

    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(tableName).append(names.toString()).append(values.toString());
    return sb.toString();
  }

  public PreparedStatement createInsertStatement(Connection conn, AbstractEntity entity)
      throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    return createInsertStatement(conn, tableName, entity);
  }

  public PreparedStatement createInsertStatement(Connection conn, String tableName,
      AbstractEntity entity) throws Exception {
    if (!insertSqlMap.containsKey(tableName)) {
      String insertSql = generateInsertSql(tableName,
          entityMappingHolder.columnInfoPerTable.get(tableName.toLowerCase()));
      insertSqlMap.put(tableName, insertSql);
      LOG.debug(insertSql);
    }

    String sql = insertSqlMap.get(tableName);
    PreparedStatement preparedStatement =
        conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    int parameterIndex = 1;
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      if (columnInfo.field != null
          && !AUTO_UPDATE_COLUMN_SET.contains(columnInfo.columnNameInDB.toLowerCase())) {
        Object val = columnInfo.field.get(entity);
        if (val != null) {
          if (columnInfo.sqlType == Types.CLOB) {
            Clob clob = conn.createClob();
            clob.setString(1, val.toString());
            preparedStatement.setClob(parameterIndex++, clob);
          } else if (columnInfo.sqlType == Types.TIMESTAMP) {
            preparedStatement.setObject(parameterIndex++, val, columnInfo.sqlType);
          } else {
            preparedStatement.setObject(parameterIndex++, val.toString(), columnInfo.sqlType);
          }

        } else {
          preparedStatement.setNull(parameterIndex++, columnInfo.sqlType);
        }
      }
    }
    return preparedStatement;

  }

  public PreparedStatement createFindByIdStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Long id) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Select * from " + tableName + " where id=?";
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    prepareStatement.setLong(1, id);
    return prepareStatement;
  }

  public PreparedStatement createFindByIdStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, List<Long> ids) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    StringBuilder sql = new StringBuilder("Select * from " + tableName + " where id IN (");
    String delim = "";
    for (Long id : ids) {
      sql.append(delim).append(id);
      delim = ", ";
    }
    sql.append(")");
    LOG.debug("find by id(s) sql: {}", sql);
    PreparedStatement prepareStatement = connection.prepareStatement(sql.toString());
    return prepareStatement;
  }

  public PreparedStatement createFindByParamsStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Map<String, Object> filters) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);
    LinkedHashMap<String, Object> parametersMap = new LinkedHashMap<>();
    if (filters != null && !filters.isEmpty()) {
      StringBuilder whereClause = new StringBuilder(" WHERE ");
      String delim = "";
      for (String columnName : filters.keySet()) {
        String dbFieldName = entityNameToDBNameMapping.get(columnName);
        whereClause.append(delim).append(dbFieldName).append("=").append("?");
        parametersMap.put(dbFieldName, filters.get(columnName));
        delim = " AND ";
      }
      sqlBuilder.append(whereClause.toString());
    }
    LOG.debug("FIND BY SQL: {} " + sqlBuilder);
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Entry<String, Object> paramEntry : parametersMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      LOG.debug("Setting parameter: {} to {}", parameterIndex, paramEntry.getValue());
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue().toString(), info.sqlType);
    }
    return prepareStatement;
  }

  public PreparedStatement createUpdateStatement(Connection connection, AbstractEntity entity,
      Set<String> fieldsToUpdate, Predicate predicate) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);

    StringBuilder sqlBuilder = new StringBuilder("UPDATE " + tableName + " SET ");
    String delim = "";
    List<Pair<String, Object>> parametersList = new ArrayList<>();
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnNameInDB = columnInfo.columnNameInDB;
      if (!AUTO_UPDATE_COLUMN_SET.contains(columnNameInDB)
          && (fieldsToUpdate == null || fieldsToUpdate.contains(columnInfo.columnNameInEntity))) {
        Object val = columnInfo.field.get(entity);
        if (val != null) {
          if (Enum.class.isAssignableFrom(val.getClass())) {
            val = val.toString();
          }
          sqlBuilder.append(delim);
          sqlBuilder.append(columnNameInDB);
          sqlBuilder.append("=");
          sqlBuilder.append("?");
          delim = ",";
          parametersList.add(new ImmutablePair<String, Object>(columnNameInDB, val));
        }
      }
    }
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    generateWhereClause(entityNameToDBNameMapping, predicate, parametersList, whereClause);
    sqlBuilder.append(whereClause.toString());
    LOG.debug("Update statement:{}" + sqlBuilder);
    int parameterIndex = 1;
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    for (Pair<String, Object> paramEntry : parametersList) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);

    }
    return prepareStatement;
  }

  public PreparedStatement createDeleteStatement(Connection connection, Class<? extends AbstractEntity> entityClass,
      List<Long> ids, boolean useBaseId) throws Exception {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("ids to delete cannot be null/empty");
    }
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    StringBuilder sqlBuilder = new StringBuilder("DELETE FROM " + tableName);
    String whereString = " WHERE id IN( ";
    if (useBaseId) {
      whereString = " WHERE base_id IN( ";
    }
    StringBuilder whereClause = new StringBuilder(whereString);
    String delim = "";
    for (Long id : ids) {
      whereClause.append(delim).append(id);
      delim = ",";
    }
    whereClause.append(")");
    sqlBuilder.append(whereClause.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    return prepareStatement;
  }

  public PreparedStatement createDeleteByIdStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Map<String, Object> filters) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("DELETE FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    LinkedHashMap<String, Object> parametersMap = new LinkedHashMap<>();
    for (String columnName : filters.keySet()) {
      String dbFieldName = entityNameToDBNameMapping.get(columnName);
      whereClause.append(dbFieldName).append("=").append("?");
      parametersMap.put(dbFieldName, filters.get(columnName));
    }
    sqlBuilder.append(whereClause.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Entry<String, Object> paramEntry : parametersMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }



  public PreparedStatement createFindAllStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Select * from " + tableName;
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    return prepareStatement;
  }

  public PreparedStatement createFindByParamsStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Predicate predicate) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    List<Pair<String, Object>> parametersList = new ArrayList<>();
    generateWhereClause(entityNameToDBNameMapping, predicate, parametersList, whereClause);
    sqlBuilder.append(whereClause.toString());
    LOG.debug("createFindByParamsStatement Query: {}", sqlBuilder);
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Pair<String, Object> pair : parametersList) {
      String dbFieldName = pair.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      Preconditions.checkNotNull(info, String.format("Found field '%s' but expected %s", dbFieldName, columnInfoMap.keySet()));
      prepareStatement.setObject(parameterIndex++, pair.getValue(), info.sqlType);
      LOG.debug("Setting {} to {}", pair.getKey(), pair.getValue());
    }
    return prepareStatement;
  }

  public PreparedStatement createfindByParamsStatementWithLimit(Connection connection,
      Class<? extends AbstractEntity> entityClass, Predicate predicate, Long limit, Long offset) throws Exception {
    String tableName = entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    List<Pair<String, Object>> parametersList = new ArrayList<>();
    generateWhereClause(entityNameToDBNameMapping, predicate, parametersList, whereClause);
    sqlBuilder.append(whereClause.toString());
    sqlBuilder.append(" ORDER BY id DESC");
    if (limit != null) {
      sqlBuilder.append(" LIMIT ").append(limit);
    }
    if (offset != null) {
      sqlBuilder.append(" OFFSET ").append(offset);
    }
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Pair<String, Object> pair : parametersList) {
      String dbFieldName = pair.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      Preconditions.checkNotNull(info, String.format("Found field '%s' but expected %s", dbFieldName, columnInfoMap.keySet()));
      prepareStatement.setObject(parameterIndex++, pair.getValue(), info.sqlType);
      LOG.debug("Setting {} to {}", pair.getKey(), pair.getValue());
    }
    return prepareStatement;
  }

  public PreparedStatement createCountStatement(Connection connection,
      Class<? extends AbstractIndexEntity> indexEntityClass) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(indexEntityClass.getSimpleName());
    String sql = "Select count(*) from " + tableName;
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    return prepareStatement;
  }

  private void generateWhereClause(BiMap<String, String> entityNameToDBNameMapping,
      Predicate predicate, List<Pair<String, Object>> parametersList, StringBuilder whereClause) {
    String columnName = null;

    if (predicate.getLhs() != null) {
      columnName = entityNameToDBNameMapping.get(predicate.getLhs());
      Preconditions.checkNotNull(columnName, String.format("Found field '%s' but expected %s", predicate.getLhs(), entityNameToDBNameMapping.keySet()));
    }

    switch (predicate.getOper()) {
      case AND:
      case OR:
        whereClause.append("(");
        String delim = "";
        for (Predicate childPredicate : predicate.getChildPredicates()) {
          whereClause.append(delim);
          generateWhereClause(entityNameToDBNameMapping, childPredicate, parametersList,
              whereClause);
          delim = "  " + predicate.getOper().toString() + " ";
        }
        whereClause.append(")");
        break;
      case EQ:
      case LIKE:
      case GT:
      case LT:
      case NEQ:
      case LE:
      case GE:
        whereClause.append(columnName).append(" ").append(predicate.getOper().toString()).append(" ?");
        parametersList.add(ImmutablePair.of(columnName, predicate.getRhs()));
        break;
      case IN:
        Object rhs = predicate.getRhs();
        if (rhs != null && rhs.getClass().isArray()) {
          whereClause.append(columnName).append(" ").append(Predicate.OPER.IN.toString())
              .append("(");
          delim = "";
          int length = Array.getLength(rhs);
          if (length > 0) {
            for (int i = 0; i < length; i++) {
              whereClause.append(delim).append("?");
              parametersList.add(ImmutablePair.of(columnName, Array.get(rhs, i)));
              delim = ",";
            }
          } else {
            whereClause.append("null");
          }
          whereClause.append(")");
        }
        break;
      case BETWEEN:
        whereClause.append(columnName).append(predicate.getOper().toString()).append("? AND ?");
        ImmutablePair<Object, Object> pair = (ImmutablePair<Object, Object>) predicate.getRhs();
        parametersList.add(ImmutablePair.of(columnName, pair.getLeft()));
        parametersList.add(ImmutablePair.of(columnName, pair.getRight()));
        break;
      default:
        throw new RuntimeException("Unsupported predicate type:" + predicate.getOper());

    }
  }

  public PreparedStatement createStatementFromSQL(Connection connection, String parameterizedSQL,
      Map<String, Object> parameterMap, Class<? extends AbstractEntity> entityClass)
          throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    parameterizedSQL = "select * from " + tableName + " " + parameterizedSQL;
    parameterizedSQL = parameterizedSQL.replace(entityClass.getSimpleName(), tableName);
    StringBuilder psSql = new StringBuilder();
    List<String> paramNames = new ArrayList<String>();
    Matcher m = PARAM_PATTERN.matcher(parameterizedSQL);

    int index = 0;
    while (m.find(index)) {
      psSql.append(parameterizedSQL.substring(index, m.start()));
      String name = m.group(1);
      index = m.end();
      if (parameterMap.containsKey(name)) {
        psSql.append("?");
        paramNames.add(name);
      } else {
        throw new IllegalArgumentException(
            "Unknown parameter '" + name + "' at position " + m.start());
      }
    }

    // Any stragglers?
    psSql.append(parameterizedSQL.substring(index));
    String sql = psSql.toString();
    BiMap<String, String> dbNameToEntityNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName);
    for (Entry<String, String> entry : dbNameToEntityNameMapping.entrySet()) {
      String dbName = entry.getKey();
      String entityName = entry.getValue();
      sql = sql.toString().replaceAll(entityName, dbName);
    }
    LOG.debug("Generated SQL:{} ", sql);
    PreparedStatement ps = connection.prepareStatement(sql);
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfo =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (String entityFieldName : paramNames) {
      String[] entityFieldNameParts = entityFieldName.split("__", 2);
      if (entityFieldNameParts.length > 1)
        LOG.info("Using field name decomposition: '{}' to '{}'", entityFieldName, entityFieldNameParts[0]);
      String dbFieldName = dbNameToEntityNameMapping.inverse().get(entityFieldNameParts[0]);

      Object val = parameterMap.get(entityFieldName);
      if (Enum.class.isAssignableFrom(val.getClass())) {
        val = val.toString();
      }
      LOG.debug("Setting {} to {}", dbFieldName, val);
      ps.setObject(parameterIndex++, val, columnInfo.get(dbFieldName).sqlType);
    }

    return ps;
  }

  public PreparedStatement createUpdateStatementForIndexTable(Connection connection,
      AbstractIndexEntity entity) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);

    StringBuilder sqlBuilder = new StringBuilder("UPDATE " + tableName + " SET ");
    String delim = "";
    LinkedHashMap<String, Object> parameterMap = new LinkedHashMap<>();
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnNameInDB = columnInfo.columnNameInDB;
      if (!columnNameInDB.equalsIgnoreCase(BASE_ID)
          && !AUTO_UPDATE_COLUMN_SET.contains(columnNameInDB)) {
        Object val = columnInfo.field.get(entity);
        if (val != null) {
          if (Enum.class.isAssignableFrom(val.getClass())) {
            val = val.toString();
          }
          sqlBuilder.append(delim);
          sqlBuilder.append(columnNameInDB);
          sqlBuilder.append("=");
          sqlBuilder.append("?");
          delim = ",";
          parameterMap.put(columnNameInDB, val);
        }
      }
    }
    //ADD WHERE CLAUSE TO CHECK FOR ENTITY ID
    sqlBuilder.append(" WHERE base_id=?");
    parameterMap.put(BASE_ID, entity.getBaseId());
    LOG.debug("Update statement:{}" , sqlBuilder);
    int parameterIndex = 1;
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    for (Entry<String, Object> paramEntry : parameterMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      LOG.debug("Setting value: {} for {}", paramEntry.getValue(), dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }
}
