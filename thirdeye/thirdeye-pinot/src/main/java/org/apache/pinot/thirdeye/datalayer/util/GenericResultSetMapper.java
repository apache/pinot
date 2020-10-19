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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.entity.AbstractEntity;
import org.modelmapper.ModelMapper;
import org.modelmapper.config.Configuration.AccessLevel;
import org.modelmapper.convention.NameTokenizers;

@Singleton
public class GenericResultSetMapper {

  private final ModelMapper modelMapper = new ModelMapper();
  private final EntityMappingHolder entityMappingHolder;

  {
    modelMapper.getConfiguration().setSourceNameTokenizer(NameTokenizers.CAMEL_CASE)
        .setFieldMatchingEnabled(true).setFieldAccessLevel(AccessLevel.PRIVATE);
  }

  @Inject
  public GenericResultSetMapper(EntityMappingHolder entityMappingHolder) {
    this.entityMappingHolder = entityMappingHolder;
  }

  public <E extends AbstractEntity> E mapSingle(ResultSet rs, Class<? extends AbstractEntity> entityClass)
      throws Exception {
    List<E> resultMapList = (List<E>) toEntityList(rs, entityClass);
    if (resultMapList.size() > 0) {
      return resultMapList.get(0);
    }
    return null;
  }

  public <E extends AbstractEntity> List<E> mapAll(ResultSet rs,
      Class<E> entityClass) throws Exception {
    return toEntityList(rs, entityClass);
  }

  private <E extends AbstractEntity> List<E> toEntityList(ResultSet rs,
      Class<E> entityClass) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    List<E> entityList = new ArrayList<>();

    ObjectMapper mapper = new ObjectMapper();
    while (rs.next()) {
      AbstractEntity entityObj = entityClass.newInstance();
      ResultSetMetaData resultSetMetaData = rs.getMetaData();
      int numColumns = resultSetMetaData.getColumnCount();
      ObjectNode objectNode = mapper.createObjectNode();
      for (int i = 1; i <= numColumns; i++) {
        String dbColumnName = resultSetMetaData.getColumnLabel(i).toLowerCase();
        ColumnInfo columnInfo = columnInfoMap.get(dbColumnName);
        Field field = columnInfo.field;
        Object val;
        if (columnInfo.sqlType == Types.CLOB) {
          Clob clob = rs.getClob(i);
          val = clob.getSubString(1, (int) clob.length());
        } else {
          val = rs.getObject(i);
        }
        if (val == null) {
          continue;
        }
        if (field.getType().isAssignableFrom(Timestamp.class)) {
          objectNode.put(field.getName(), ((Timestamp) val).getTime());
        } else {
          objectNode.put(field.getName(), val.toString());
        }

        /*
         * if (Enum.class.isAssignableFrom(field.getType())) { field.set(entityObj,
         * Enum.valueOf(field.getType().asSubclass(Enum.class), val.toString())); } else if
         * (String.class.isAssignableFrom(field.getType())) { field.set(entityObj, val.toString());
         * } else if (Integer.class.isAssignableFrom(field.getType())) { field.set(entityObj,
         * Integer.valueOf(val.toString())); } else if
         * (Long.class.isAssignableFrom(field.getType())) { field.set(entityObj,
         * Long.valueOf(val.toString())); } else { field.set(entityObj, val); }
         */
      }
      entityObj = mapper.treeToValue(objectNode, entityClass);
      entityList.add((E) entityObj);
    }

    return entityList;

  }

  public AbstractEntity mapSingleOLD(ResultSet rs,
      Class<? extends AbstractEntity> entityClass) throws Exception {
    List<Map<String, Object>> resultMapList = toResultMapList(rs, entityClass);
    if (resultMapList.size() > 0) {
      Map<String, Object> map = resultMapList.get(0);
      if (map.get("workerId") != null) {
        ObjectOutputStream oos = new ObjectOutputStream(
            new FileOutputStream(new File("/tmp/map.out." + System.currentTimeMillis())));
        oos.writeObject(map);
        oos.close();
        System.out.println(map);
      }
      AbstractEntity entity = modelMapper.map(map, entityClass);
      System.out.println(entity);
      return entity;
    }
    return null;
  }

  public List<AbstractEntity> mapAllOLD(ResultSet rs,
      Class<? extends AbstractEntity> entityClass) throws Exception {
    List<Map<String, Object>> resultMapList = toResultMapList(rs, entityClass);
    List<AbstractEntity> resultEntityList = new ArrayList<>();
    if (resultMapList.size() > 0) {
      for (Map<String, Object> map : resultMapList) {
        AbstractEntity entity = modelMapper.map(map, entityClass);
        resultEntityList.add(entity);
      }
    }
    return resultEntityList;
  }

  List<Map<String, Object>> toResultMapList(ResultSet rs,
      Class<? extends AbstractEntity> entityClass) throws Exception {
    List<Map<String, Object>> resultMapList = new ArrayList<>();
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> dbNameToEntityNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName);
    while (rs.next()) {
      ResultSetMetaData resultSetMetaData = rs.getMetaData();
      int numColumns = resultSetMetaData.getColumnCount();
      HashMap<String, Object> map = new HashMap<>();
      for (int i = 1; i <= numColumns; i++) {
        String dbColumnName = resultSetMetaData.getColumnLabel(i).toLowerCase();
        String entityFieldName = dbNameToEntityNameMapping.get(dbColumnName);
        Object val = rs.getObject(i);
        if (val != null) {
          map.put(entityFieldName, val.toString());
        }
      }
      resultMapList.add(map);
    }
    System.out.println(resultMapList);
    return resultMapList;
  }

  public static void main(String[] args) throws Exception {
    ModelMapper mapper = new ModelMapper();
    Map<String, Object> result = new HashMap<>();
    //[{jobName=Test_Anomaly_Task, jobId=1, workerId=1, taskType=MONITOR, id=1, taskInfo=clob2: '{"jobExecutionId":1,"monitorType":"UPDATE","expireDaysAgo":0}', lastModified=2016-08-24 17:25:53.258, version=0, taskStartTime=1470356753227, status=RUNNING, taskEndTime=1471220753227}]

    result.put("jobName", "Test_Anomaly_Task");
    result.put("jobId", 1L);
    result.put("taskType", "MONITOR");
    result.put("id", 1L);
    result.put("taskInfo",
        "clob2: '{\"jobExecutionId\":1,\"monitorType\":\"UPDATE\",\"expireDaysAgo\":0}'");
    result.put("taskType", "MONITOR");
    result.put("lastModified", "2016-08-24 17:25:53.258");
    result.put("status", "RUNNING");
    result.put("lastModified", "2016-08-24 17:25:53.258");
    TaskDTO taskSpec1 = mapper.map(result, TaskDTO.class);
    System.out.println(taskSpec1);

    //INPUT 2
    ObjectInputStream ois =
        new ObjectInputStream(new FileInputStream(new File("/tmp/map.out.1472093046128")));
    Map<String, Object> inputMap = (Map<String, Object>) ois.readObject();
    TaskDTO taskSpec2 = mapper.map(inputMap, TaskDTO.class);
    System.out.println(taskSpec2);
  }
}
