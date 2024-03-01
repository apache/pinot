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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.CommonConstants;


public class DatabaseUtils {
  private DatabaseUtils() {
  }

  /**
   * Construct the fully qualified table name i.e. {databaseName}.{tableName} from given table name and database name
   * @param tableName table/schema name
   * @param databaseName database name
   * @return translated table name. Throws {@link IllegalStateException} if {@code tableName} contains more than 1 dot
   * or if {@code tableName} has database prefix, and it does not match with {@code databaseName}
   */
  public static String translateTableName(String tableName, @Nullable String databaseName) {
    if (tableName == null) {
      throw new IllegalArgumentException("'tableName' cannot be null");
    }
    String[] tableSplit = StringUtils.split(tableName, '.');
    switch (tableSplit.length) {
      case 1:
        // do not concat the database name prefix if it's a 'default' database
        if (StringUtils.isNotEmpty(databaseName) && !databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE)) {
          return String.format("%s.%s", databaseName, tableName);
        }
        return tableName;
      case 2:
        String databasePrefix = tableSplit[0];
        if (StringUtils.isNotEmpty(databaseName) && !databaseName.equals(databasePrefix)) {
          throw new IllegalArgumentException("Database name '" + databasePrefix
              + "' from table prefix does not match database name '" + databaseName + "' from header");
        }
        // skip database name prefix if it's a 'default' database
        return databasePrefix.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE) ? tableSplit[1] : tableName;
      default:
      throw new IllegalArgumentException("Table name: '" + tableName + "' containing more than one '.' is not allowed");
    }
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
