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

import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Utility helpers shared by components that expose or consume the {@code system.query_log} table.
 */
public final class QueryLogSystemTableUtils {
  public static final String SYSTEM_SCHEMA = "system";
  public static final String SYSTEM_SCHEMA_ALIAS = "sys";
  public static final String TABLE_NAME = "query_log";
  public static final String FULL_TABLE_NAME = SYSTEM_SCHEMA + '.' + TABLE_NAME;

  private QueryLogSystemTableUtils() {
  }

  public static boolean isQueryLogSystemTableQuery(@Nullable SqlNode sqlNode) {
    if (sqlNode == null) {
      return false;
    }
    SqlNode unwrapped = unwrapOrderBy(sqlNode);
    if (!(unwrapped instanceof SqlSelect)) {
      return false;
    }
    SqlSelect select = (SqlSelect) unwrapped;
    SqlIdentifier tableIdentifier = extractTableIdentifier(select.getFrom());
    return isQueryLogSystemTableIdentifier(tableIdentifier);
  }

  private static SqlNode unwrapOrderBy(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy) {
      return ((SqlOrderBy) sqlNode).query;
    }
    return sqlNode;
  }

  @Nullable
  private static SqlIdentifier extractTableIdentifier(@Nullable SqlNode fromNode) {
    if (fromNode instanceof SqlIdentifier) {
      return (SqlIdentifier) fromNode;
    }
    if (fromNode instanceof SqlBasicCall && fromNode.getKind() == org.apache.calcite.sql.SqlKind.AS) {
      SqlNode aliased = ((SqlBasicCall) fromNode).operand(0);
      if (aliased instanceof SqlIdentifier) {
        return (SqlIdentifier) aliased;
      }
    }
    return null;
  }

  public static boolean isQueryLogSystemTableIdentifier(@Nullable SqlIdentifier identifier) {
    if (identifier == null) {
      return false;
    }
    List<String> names = identifier.names;
    if (names.isEmpty()) {
      return false;
    }
    String tableName = names.get(names.size() - 1);
    if (!TABLE_NAME.equalsIgnoreCase(tableName)) {
      return false;
    }
    if (names.size() == 1) {
      return true;
    }
    String schemaCandidate = names.get(names.size() - 2).toLowerCase(Locale.ROOT);
    return schemaCandidate.equals(SYSTEM_SCHEMA) || schemaCandidate.equals(SYSTEM_SCHEMA_ALIAS)
        || schemaCandidate.equals(CommonConstants.DEFAULT_DATABASE);
  }
}
