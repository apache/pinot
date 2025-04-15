/**
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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTableUtils {

  private LogicalTableUtils() {
    // Utility class
  }

  public static LogicalTable fromZNRecord(ZNRecord record)
      throws IOException {
    String tableName = record.getSimpleField(LogicalTable.TABLE_NAME_KEY);
    String physicalTableNamesString = record.getSimpleField(LogicalTable.PHYSICAL_TABLE_NAMES_KEY);

    LogicalTable logicalTable = new LogicalTable();
    logicalTable.setTableName(tableName);
    logicalTable.setPhysicalTableNames(JsonUtils.stringToObject(physicalTableNamesString, new TypeReference<>() {
    }));
    return logicalTable;
  }

  public static ZNRecord toZNRecord(LogicalTable table)
      throws JsonProcessingException {
    ZNRecord record = new ZNRecord(table.getTableName());
    record.setSimpleField(LogicalTable.TABLE_NAME_KEY, table.getTableName());
    record.setSimpleField(LogicalTable.PHYSICAL_TABLE_NAMES_KEY,
        JsonUtils.objectToString(table.getPhysicalTableNames()));
    return record;
  }

  public static void validateLogicalTableName(LogicalTable logicalTable, List<String> allPhysicalTables) {
    String tableName = logicalTable.getTableName();
    if (StringUtils.isEmpty(tableName)) {
      throw new IllegalArgumentException("Invalid logical table name. Reason: 'tableName' should not be null or empty");
    }

    if (TableNameBuilder.isOfflineTableResource(tableName) || TableNameBuilder.isRealtimeTableResource(tableName)) {
      throw new IllegalArgumentException(
          "Invalid logical table name. Reason: 'tableName' should not end with _OFFLINE or _REALTIME");
    }

    if (logicalTable.getPhysicalTableNames() == null || logicalTable.getPhysicalTableNames().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'physicalTableNames' should not be null or empty");
    }

    for (String physicalTableName : logicalTable.getPhysicalTableNames()) {
      if (!allPhysicalTables.contains(physicalTableName)) {
        throw new IllegalArgumentException(
            "Invalid logical table. Reason: '" + physicalTableName + "' should be one of the existing tables");
      }
    }
  }
}
