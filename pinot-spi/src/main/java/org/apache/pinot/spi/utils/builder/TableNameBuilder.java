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
package org.apache.pinot.spi.utils.builder;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableType;


public class TableNameBuilder {
  public static final TableNameBuilder OFFLINE = new TableNameBuilder(TableType.OFFLINE);
  public static final TableNameBuilder REALTIME = new TableNameBuilder(TableType.REALTIME);

  private static final String TYPE_SUFFIX_SEPARATOR = "_";

  private final String _typeSuffix;

  private TableNameBuilder(TableType tableType) {
    _typeSuffix = TYPE_SUFFIX_SEPARATOR + tableType.toString();
  }

  /**
   * Get the table name builder for the given table type.
   * @param tableType Table type
   * @return Table name builder for the given table type
   */
  public static TableNameBuilder forType(TableType tableType) {
    if (tableType == TableType.OFFLINE) {
      return OFFLINE;
    } else {
      return REALTIME;
    }
  }

  /**
   * Get the table name with type suffix.
   *
   * @param tableName Table name with or without type suffix
   * @return Table name with type suffix
   */
  public String tableNameWithType(String tableName) {
    if (tableName.endsWith(_typeSuffix)) {
      return tableName;
    } else {
      return tableName + _typeSuffix;
    }
  }

  /**
   * Return Whether the table has type suffix that matches the builder type.
   *
   * @param tableName Table name with or without type suffix
   * @return Whether the table has type suffix that matches the builder type
   */
  private boolean tableHasTypeSuffix(String tableName) {
    return tableName.endsWith(_typeSuffix);
  }

  /**
   * Get the table type based on the given table name with type suffix.
   *
   * @param tableName Table name with or without type suffix
   * @return Table type for the given table name, null if cannot be determined by table name
   */
  @Nullable
  public static TableType getTableTypeFromTableName(String tableName) {
    if (OFFLINE.tableHasTypeSuffix(tableName)) {
      return TableType.OFFLINE;
    }
    if (REALTIME.tableHasTypeSuffix(tableName)) {
      return TableType.REALTIME;
    }
    return null;
  }

  /**
   * Extract the raw table name from the given table name with type suffix.
   *
   * @param tableName Table name with or without type suffix
   * @return Table name without type suffix
   */
  public static String extractRawTableName(String tableName) {
    if (tableName == null) {
      return null;
    }
    if (OFFLINE.tableHasTypeSuffix(tableName)) {
      return tableName.substring(0, tableName.length() - OFFLINE._typeSuffix.length());
    }
    if (REALTIME.tableHasTypeSuffix(tableName)) {
      return tableName.substring(0, tableName.length() - REALTIME._typeSuffix.length());
    }
    return tableName;
  }

  /**
   * Return whether the given resource name represents a table resource.
   *
   * @param resourceName Resource name
   * @return Whether the resource name represents a table resource
   */
  public static boolean isTableResource(String resourceName) {
    return OFFLINE.tableHasTypeSuffix(resourceName) || REALTIME.tableHasTypeSuffix(resourceName);
  }

  /**
   * Return whether the given resource name represents an offline table resource.
   */
  public static boolean isOfflineTableResource(String resourceName) {
    return OFFLINE.tableHasTypeSuffix(resourceName);
  }

  /**
   * Return whether the given resource name represents a realtime table resource.
   */
  public static boolean isRealtimeTableResource(String resourceName) {
    return REALTIME.tableHasTypeSuffix(resourceName);
  }

  public static Set<String> getTableNameVariations(String tableName) {
    String rawTableName = extractRawTableName(tableName);
    String offlineTableName = OFFLINE.tableNameWithType(rawTableName);
    String realtimeTableName = REALTIME.tableNameWithType(rawTableName);
    return ImmutableSet.of(rawTableName, offlineTableName, realtimeTableName);
  }
}
