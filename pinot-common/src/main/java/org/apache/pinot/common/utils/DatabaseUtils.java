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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseUtils {
  private DatabaseUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseUtils.class);

  private static final List<String> TABLE_NAME_KEYS = List.of("tableName", "tableNameWithType", "schemaName");

  /**
   * Replace the table/schema name query params of the {@link ContainerRequestContext} requestContext with the
   * fully qualified table/schema name i.e. {databaseName}.{tableName}
   * @param requestContext requestContext to update
   */
  public static void translateTableNameQueryParam(ContainerRequestContext requestContext, TableCache tableCache) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    String uri = requestContext.getUriInfo().getRequestUri().toString();
    String databaseName = null;
    if (requestContext.getHeaders().containsKey(CommonConstants.DATABASE)) {
      databaseName = requestContext.getHeaderString(CommonConstants.DATABASE);
    }
    for (String key : TABLE_NAME_KEYS) {
      if (queryParams.containsKey(key)) {
        String tableName = queryParams.getFirst(key);
        String actualTableName = translateTableName(tableName, databaseName);
        // table is not part of default database
        if (!actualTableName.equals(tableName)) {
          uri = uri.replaceAll(String.format("%s=%s", key, tableName), String.format("%s=%s", key, actualTableName));
        }
      }
    }
    try {
      requestContext.setRequestUri(new URI(uri));
    } catch (URISyntaxException e) {
      LOGGER.error("Unable to translate the uri to {}", uri);
    }
  }

  /**
   * Construct the fully qualified table name i.e. {databaseName}.{tableName} from given table name and database name
   * If table name already has the database prefix then that takes precedence over the provided {@code databaseName}
   * @param tableName table/schema name
   * @param databaseName database name
   * @return translated table name
   */
  public static String translateTableName(String tableName, String databaseName) {
    if (tableName == null) {
      return null;
    }
    String[] tableSplit = StringUtils.split(tableName, '.');
    if (tableSplit.length > 2) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing more than one '.' is not allowed");
    } else if (tableSplit.length == 2) { // tableName already has database name prefix
      // if the database name prefix is of 'default' database then only return the table name part
      if (tableSplit[0].equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE)) {
        return tableSplit[1];
      }
      return tableName;
    }
    // do not concat the database name prefix if it's a 'default' database
    if (StringUtils.isNotEmpty(databaseName) && !databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE)) {
      return String.format("%s.%s", databaseName, tableName);
    }
    return tableName;
  }

  /**
   * Checks the logical table name equivalence. Usually needed when one of the table name is translated while other may
   * not but soft validation is performed before overwriting the translated name everywhere.
   * @param name1 table name
   * @param name2 another table name
   * @return {@code true} if both are null or both have the same logical table name.
   */
  public static boolean isTableNameEquivalent(String name1, String name2) {
    return Objects.equals(name1, name2)
        || (name1 != null && name2 != null && (name1.endsWith("." + name2) || name2.endsWith("." + name1)));
  }
}
