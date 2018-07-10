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
package com.linkedin.pinot.common.config;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.SegmentName;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class TableNameBuilder {
  public static final TableNameBuilder OFFLINE = new TableNameBuilder(TableType.OFFLINE);
  public static final TableNameBuilder REALTIME = new TableNameBuilder(TableType.REALTIME);

  private static final String TYPE_SUFFIX_SEPARATOR = "_";

  private final String _typeSuffix;

  private TableNameBuilder(@Nonnull TableType tableType) {
    _typeSuffix = TYPE_SUFFIX_SEPARATOR + tableType.toString();
  }

  /**
   * Get the table name builder for the given table type.
   * @param tableType Table type
   * @return Table name builder for the given table type
   */
  @Nonnull
  public static TableNameBuilder forType(@Nonnull TableType tableType) {
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
  @Nonnull
  public String tableNameWithType(@Nonnull String tableName) {
    Preconditions.checkArgument(!tableName.contains(SegmentName.SEPARATOR),
        "Table name: %s cannot contain two consecutive underscore characters", tableName);

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
  public boolean tableHasTypeSuffix(@Nonnull String tableName) {
    return tableName.endsWith(_typeSuffix);
  }

  /**
   * Get the table type based on the given table name with type suffix.
   *
   * @param tableName Table name with or without type suffix
   * @return Table type for the given table name, null if cannot be determined by table name
   */
  @Nullable
  public static TableType getTableTypeFromTableName(@Nonnull String tableName) {
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
  @Nonnull
  public static String extractRawTableName(@Nonnull String tableName) {
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
  public static boolean isTableResource(@Nonnull String resourceName) {
    return OFFLINE.tableHasTypeSuffix(resourceName) || REALTIME.tableHasTypeSuffix(resourceName);
  }
}
