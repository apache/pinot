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
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseUtils {
  private DatabaseUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseUtils.class);

  private static final List<String> TABLE_NAME_KEYS = List.of("tableName", "tableNameWithType", "schemaName");

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
        String actualTableName = translateTableName(tableName, databaseName, tableCache);
        // table is not part of default database
        if (!actualTableName.equals(tableName)) {
          uri = uri.replaceAll(String.format("%s=%s", key, tableName),
              String.format("%s=%s", key, actualTableName));
          try {
            requestContext.setRequestUri(new URI(uri));
          } catch (URISyntaxException e) {
            LOGGER.error("Unable to translate the table name from {} to {}", tableName, actualTableName);
          }
        }
      }
    }
  }

  public static String translateTableName(String tableName, String databaseName, @Nullable TableCache tableCache) {
    if (tableName != null && databaseName != null) {
      String[] tableSplit = tableName.split("\\.");
      if (tableSplit.length > 2) {
        throw new IllegalStateException("Table name: '" + tableName + "' containing more than one '.' is not allowed");
      } else if (tableSplit.length == 2) {
        databaseName = tableSplit[0];
        tableName = tableSplit[1];
      }
      if (databaseName != null && !databaseName.isBlank()) {
        tableName = String.format("%s.%s", databaseName, tableName);
      }
    }
    String actualTableName = null;
    if (tableCache != null) {
      actualTableName = tableCache.getActualTableName(tableName);
    }
    return actualTableName != null ? actualTableName : tableName;
  }

  public static boolean isTableNameEquivalent(String logicalName, String physicalName) {
    return Objects.equals(logicalName, physicalName) || physicalName.endsWith("." + logicalName);
  }
}
