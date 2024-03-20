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

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.CommonConstants;


public class DatabaseUtils {
  private DatabaseUtils() {
  }

  /**
   * Construct the fully qualified table name i.e. {databaseName}.{tableName} from given table name and database name
   * @param tableName table/schema name
   * @param databaseName database name
   * @param ignoreCase whether to ignore case when comparing passed in database name against table name prefix if both
   *                   exist. For 'default' database, always compare it ignoring case.
   * @return translated table name. Throws {@link IllegalArgumentException} if {@code tableName} contains
   * more than 1 dot or if {@code tableName} has database prefix, and it does not match with {@code databaseName}
   */
  public static String translateTableName(String tableName, @Nullable String databaseName, boolean ignoreCase) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "'tableName' cannot be null or empty");
    String[] tableSplit = StringUtils.split(tableName, '.');
    switch (tableSplit.length) {
      case 1:
        // do not concat the database name prefix if it's a 'default' database
        if (StringUtils.isNotEmpty(databaseName) && !databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE)) {
          return databaseName + "." + tableName;
        }
        return tableName;
      case 2:
        Preconditions.checkArgument(!tableSplit[1].isEmpty(), "Invalid table name '%s'", tableName);
        String databasePrefix = tableSplit[0];
        Preconditions.checkArgument(
            StringUtils.isEmpty(databaseName) || (!ignoreCase && databaseName.equals(databasePrefix)) || (ignoreCase
                && databaseName.equalsIgnoreCase(databasePrefix)),
            "Database name '%s' from table prefix does not match database name '%s' from header", databasePrefix,
            databaseName);
        // skip database name prefix if it's a 'default' database
        return databasePrefix.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE) ? tableSplit[1] : tableName;
      default:
        throw new IllegalArgumentException(
            "Table name: '" + tableName + "' containing more than one '.' is not allowed");
    }
  }

  public static String translateTableName(String tableName, @Nullable String databaseName) {
    return translateTableName(tableName, databaseName, false);
  }

  /**
   * Utility to get fully qualified table name i.e. {databaseName}.{tableName} from given table name and http headers
   * @param tableName table/schema name
   * @param headers http headers
   * @param ignoreCase whether to ignore case when comparing database name in headers against table name prefix if both
   *                   exist. For 'default' database, always compare it ignoring case.
   * @return translated table name. Throws {@link IllegalStateException} if {@code tableName} contains more than 1 dot
   * or if {@code tableName} has database prefix, and it does not match with the 'database' header
   */
  public static String translateTableName(String tableName, HttpHeaders headers, boolean ignoreCase) {
    return translateTableName(tableName, headers.getHeaderString(CommonConstants.DATABASE), ignoreCase);
  }

  public static String translateTableName(String tableName, HttpHeaders headers) {
    return translateTableName(tableName, headers, false);
  }

  /**
   * Checks if the fully qualified {@code tableName} belongs to the provided {@code databaseName}
   * @param tableName fully qualified table name
   * @param databaseName database name
   * @return true if
   * <ul>
   *   <li>
   *     tableName is prefixed with "databaseName." or
   *   </li>
   *   <li>
   *     databaseName is null or "default" and tableName does not have a '.'
   *   </li>
   * </ul>
   * else false
   */
  public static boolean isPartOfDatabase(String tableName, @Nullable String databaseName) {
    // assumes tableName will not have default database prefix ('default.')
    if (StringUtils.isEmpty(databaseName) || databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE)) {
      return !tableName.contains(".");
    } else {
      return tableName.startsWith(databaseName + ".");
    }
  }

  /**
   * Extract database context from headers and query options
   * @param queryOptions Query option from request
   * @param headers http headers from request
   * @return extracted database name.
   * If queryOptions and headers have conflicting database context an {@link IllegalArgumentException} is thrown
   */
  public static @Nullable String extractDatabaseFromQueryRequest(
      Map<String, String> queryOptions, HttpHeaders headers) {
    String databaseFromOptions = queryOptions.get(CommonConstants.DATABASE);
    String databaseFromHeaders = headers.getHeaderString(CommonConstants.DATABASE);
    if (databaseFromHeaders != null && databaseFromOptions != null) {
      Preconditions.checkArgument(databaseFromOptions.equals(databaseFromHeaders), "Database context mismatch : "
          + "from headers %s, from query options %s", databaseFromHeaders, databaseFromOptions);
    }
    if (databaseFromHeaders != null) {
      return databaseFromHeaders;
    }
    return databaseFromOptions;
  }
}
