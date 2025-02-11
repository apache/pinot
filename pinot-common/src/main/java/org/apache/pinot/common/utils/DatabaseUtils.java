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
import java.util.Objects;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.utils.CommonConstants;


public class DatabaseUtils {
  private DatabaseUtils() {
  }

  /**
   * Returns the fully qualified table name. Do not prefix the database name if it is the default database.
   */
  public static String constructFullyQualifiedTableName(String databaseName, String tableName) {
    return databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE) ? tableName : databaseName + "." + tableName;
  }

  /**
   * Splits a fully qualified table name i.e. {databaseName}.{tableName} into different components.
   */
  public static String[] splitTableName(String tableName) {
    return StringUtils.split(tableName, '.');
  }

  /**
   * Construct the fully qualified table name i.e. {databaseName}.{tableName} from given table name and database name
   * @param tableName table/schema name
   * @param databaseName database name
   * @param ignoreCase whether to ignore case when comparing passed in database name against table name prefix if both
   *                   exist. For 'default' database, always compare it ignoring case.
   * @return translated table name.
   * <br>Throws {@link IllegalArgumentException} if {@code tableName} contains more than 1 dot
   * <br>Throws {@link DatabaseConflictException} if {@code tableName} has database prefix,
   * and it does not match with {@code databaseName}
   */
  public static String translateTableName(String tableName, @Nullable String databaseName, boolean ignoreCase) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "'tableName' cannot be null or empty");
    String[] tableSplit = splitTableName(tableName);
    switch (tableSplit.length) {
      case 1:
        return StringUtils.isEmpty(databaseName) ? tableName
            : constructFullyQualifiedTableName(databaseName, tableName);
      case 2:
        Preconditions.checkArgument(!tableSplit[1].isEmpty(), "Invalid table name '%s'", tableName);
        String databasePrefix = tableSplit[0];
        if (!StringUtils.isEmpty(databaseName) && (ignoreCase || !databaseName.equals(databasePrefix))
            && (!ignoreCase || !databaseName.equalsIgnoreCase(databasePrefix))) {
          throw new DatabaseConflictException("Database name '" + databasePrefix
              + "' from table prefix does not match database name '" + databaseName + "' from header");
        }
        // skip database name prefix if it's a 'default' database
        return databasePrefix.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE) ? tableSplit[1] : tableName;
      default:
        throw new IllegalArgumentException(
            "Table name: '" + tableName + "' containing more than one '.' is not allowed");
    }
  }

  /**
   * Construct the fully qualified table name i.e. {databaseName}.{tableName} from given table name and database name
   * @param tableName table/schema name
   * @param databaseName database name
   * @return translated table name.
   * <br>Throws {@link IllegalArgumentException} if {@code tableName} contains more than 1 dot
   * <br>Throws {@link DatabaseConflictException} if {@code tableName} has database prefix,
   * and it does not match with {@code databaseName}
   */
  public static String translateTableName(String tableName, @Nullable String databaseName) {
    return translateTableName(tableName, databaseName, false);
  }

  /**
   * Utility to get fully qualified table name i.e. {databaseName}.{tableName} from given table name and http headers
   * @param tableName table/schema name
   * @param headers http headers
   * @param ignoreCase whether to ignore case when comparing database name in headers against table name prefix if both
   *                   exist. For 'default' database, always compare it ignoring case.
   * @return translated table name.
   * <br>Throws {@link IllegalArgumentException} if {@code tableName} contains more than 1 dot
   * <br>Throws {@link DatabaseConflictException}  if {@code tableName} has database prefix,
   * and it does not match with the 'database' header
   */
  public static String translateTableName(String tableName, @Nullable HttpHeaders headers, boolean ignoreCase) {
    String database = headers != null ? headers.getHeaderString(CommonConstants.DATABASE) : null;
    return translateTableName(tableName, database, ignoreCase);
  }

  /**
   * Utility to get fully qualified table name i.e. {databaseName}.{tableName} from given table name and http headers
   * @param tableName table/schema name
   * @param headers http headers
   * @return translated table name.
   * <br>Throws {@link IllegalArgumentException} if {@code tableName} contains more than 1 dot
   * <br>Throws {@link DatabaseConflictException}  if {@code tableName} has database prefix,
   * and it does not match with the 'database' header
   */
  public static String translateTableName(String tableName, @Nullable HttpHeaders headers) {
    return translateTableName(tableName, headers, false);
  }

  /**
   * Checks if the databaseName refers to the default one.
   * A null or blank databaseName is considered as default.
   * @param databaseName database name
   * @return true if database refers to the default one
   */
  private static boolean isDefaultDatabase(@Nullable String databaseName) {
    return StringUtils.isBlank(databaseName) || databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE);
  }

  /**
   * Checks if the table belongs to the default database.
   * If table is not fully qualified (has no prefix) it is assumed to belong to the default database.
   * @param tableName the table name
   * @return the table belongs to the default database
   */
  private static boolean isPartOfDefaultDatabase(String tableName) {
    return !tableName.contains(".") || tableName.startsWith(CommonConstants.DEFAULT_DATABASE + ".");
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
    if (isDefaultDatabase(databaseName)) {
      return isPartOfDefaultDatabase(tableName);
    } else {
      return tableName.startsWith(databaseName + ".");
    }
  }

  /**
   * Removes the provided {@code databaseName} from the fully qualified {@code tableName}.
   * If table does not belong to the given database the tableName is returned as is.
   * @param tableName fully qualified table name
   * @param databaseName database name
   * @return The tableName without the database prefix.
   */
  public static String removeDatabasePrefix(String tableName, String databaseName) {
    return tableName.replaceFirst(databaseName + "\\.", "");
  }

  /**
   * Extract database context from headers and query options
   * @param queryOptions Query option from request
   * @param headers http headers from request
   * @return extracted database name.
   * <br>If database context is not provided at all return {@link CommonConstants#DEFAULT_DATABASE}.
   * <br>If queryOptions and headers have conflicting database context an {@link DatabaseConflictException} is thrown.
   */
  public static String extractDatabaseFromQueryRequest(
      @Nullable Map<String, String> queryOptions, @Nullable HttpHeaders headers) {
    String databaseFromOptions = queryOptions == null ? null : queryOptions.get(CommonConstants.DATABASE);
    String databaseFromHeaders = headers == null ? null : headers.getHeaderString(CommonConstants.DATABASE);
    if (databaseFromHeaders != null && databaseFromOptions != null
        && !databaseFromOptions.equals(databaseFromHeaders)) {
      throw new DatabaseConflictException("Database context mismatch : from headers '" + databaseFromHeaders
          + "', from query options '" + databaseFromOptions + "'");
    }
    String database = databaseFromHeaders != null ? databaseFromHeaders : databaseFromOptions;
    return Objects.requireNonNullElse(database, CommonConstants.DEFAULT_DATABASE);
  }

  /**
   * Extract the database name from the prefix of fully qualified table name.
   * If no prefix is present "default" database is returned
   */
  public static String extractDatabaseFromFullyQualifiedTableName(String fullyQualifiedTableName) {
    String[] split = StringUtils.split(fullyQualifiedTableName, '.');
    return split.length == 1 ? CommonConstants.DEFAULT_DATABASE : split[0];
  }

  public static String extractDatabaseFromHttpHeaders(HttpHeaders headers) {
    String databaseName = headers.getHeaderString(CommonConstants.DATABASE);
    return databaseName == null ? CommonConstants.DEFAULT_DATABASE : databaseName;
  }
}
