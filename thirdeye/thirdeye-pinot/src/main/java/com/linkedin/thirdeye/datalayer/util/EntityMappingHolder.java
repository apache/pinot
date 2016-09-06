package com.linkedin.thirdeye.datalayer.util;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.thirdeye.datalayer.entity.AbstractEntity;

public class EntityMappingHolder {
  //Map<TableName,EntityName>
  BiMap<String, String> tableToEntityNameMap = HashBiMap.create();
  Map<String, LinkedHashMap<String, ColumnInfo>> columnInfoPerTable = new HashMap<>();
  //DB NAME to ENTITY NAME mapping
  Map<String, BiMap<String, String>> columnMappingPerTable = new HashMap<>();

  public void register(Connection connection, Class<? extends AbstractEntity> entityClass,
      String tableName) throws Exception {
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    String catalog = null;
    String schemaPattern = null;
    String tableNamePattern = tableName;
    String columnNamePattern = null;
    ResultSet rs =
        databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

    tableToEntityNameMap.put(tableName, entityClass.getSimpleName());
    columnMappingPerTable.put(tableName, HashBiMap.create());
    LinkedHashMap<String, ColumnInfo> columnInfoMap = new LinkedHashMap<>();

    while (rs.next()) {
      String columnName = rs.getString(4);
      ColumnInfo columnInfo = new ColumnInfo();
      columnInfo.columnNameInDB = columnName.toLowerCase();
      columnInfo.sqlType = rs.getInt(5);
      columnInfoMap.put(columnName.toLowerCase(), columnInfo);
    }
    List<Field> fields = new ArrayList<>();
    getAllFields(fields, entityClass);
    for (Field field : fields) {
      field.setAccessible(true);
      String entityColumn = field.getName();
      for (String dbColumn : columnInfoMap.keySet()) {
        boolean success = false;
        if (dbColumn.toLowerCase().equals(entityColumn.toLowerCase())) {
          success = true;
        }
        String dbColumnNormalized = dbColumn.replaceAll("_", "").toLowerCase();
        String entityColumnNormalized = entityColumn.replaceAll("_", "").toLowerCase();
        if (dbColumnNormalized.equals(entityColumnNormalized)) {
          success = true;
        }
        if (success) {
          columnInfoMap.get(dbColumn).columnNameInEntity = entityColumn;
          columnInfoMap.get(dbColumn).field = field;
          System.out.println("Mapped " + dbColumn + " to " + entityColumn);
          columnMappingPerTable.get(tableName).put(dbColumn, entityColumn);
        }
      }
    }
    columnInfoPerTable.put(tableName, columnInfoMap);
  }

  public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
    fields.addAll(Arrays.asList(type.getDeclaredFields()));
    if (type.getSuperclass() != null) {
      fields = getAllFields(fields, type.getSuperclass());
    }
    return fields;
  }

}

class ColumnInfo {
  String columnNameInDB;
  int sqlType;
  String columnNameInEntity;
  Field field;
}
