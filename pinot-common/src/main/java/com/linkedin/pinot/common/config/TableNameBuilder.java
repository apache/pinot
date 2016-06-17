/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.StringUtil;


public class TableNameBuilder {

  public static final TableNameBuilder OFFLINE_TABLE_NAME_BUILDER = new TableNameBuilder(TableType.OFFLINE);
  public static final TableNameBuilder REALTIME_TABLE_NAME_BUILDER = new TableNameBuilder(TableType.REALTIME);

  TableType type;

  public TableNameBuilder(TableType type) {
    this.type = type;
  }

  public String forTable(String tableName) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkArgument(!tableName.contains("__"),
        "Table name cannot contain two consecutive underscore characters");

    if (needsPostfix(tableName)) {
      return StringUtil.join("_", tableName, type.toString().toUpperCase());
    }
    return tableName;
  }

  public boolean needsPostfix(String tableName) {
    return !tableName.endsWith(type.toString().toUpperCase());
  }

  public static TableType getTableTypeFromTableName(String tableName) {
    for (TableType tableType : TableType.values()) {
      if (tableName.endsWith("_" + tableType.toString())) {
        return tableType;
      }
    }
    return null;
  }

  public static String extractRawTableName(String tableName) {
    for (TableType tableType : TableType.values()) {
      if (tableName.endsWith("_" + tableType.toString())) {
        return tableName.substring(0, tableName.lastIndexOf("_" + tableType.toString()));
      }
    }
    return tableName;
  }

}
