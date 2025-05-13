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
package org.apache.pinot.tsdb.spi;

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Interface for accessing time series table metadata.
 */
public interface TimeSeriesMetadata {
  /**
   * Get the table config for a given table name
   * @param tableName name of the table
   * @return table config or null if not found
   */
  @Nullable
  TableConfig getTableConfig(String tableName);

  /**
   * Get the schema for a given raw table name
   * @param rawTableName raw table name without type suffix
   * @return schema or null if not found
   */
  @Nullable
  Schema getSchema(String rawTableName);

  /**
   * Get the actual table name for the given table name, handling case sensitivity
   * @param tableName table name to look up
   * @return actual table name or null if not found
   */
  @Nullable
  String getActualTableName(String tableName);
}
