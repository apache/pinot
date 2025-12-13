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
package org.apache.pinot.spi.systemtable;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * SPI for system table providers. Implementations live with the controller/broker path and serve virtual rows.
 */
public interface SystemTableProvider extends AutoCloseable {
  /**
   * Logical name, e.g. "system.tables".
   */
  String getTableName();

  /**
   * Schema for the virtual table.
   */
  Schema getSchema();

  /**
   * Lightweight virtual table config. Implementations can override for custom behavior.
   */
  default TableConfig getTableConfig() {
    return SystemTableConfigUtils.buildBasicConfig(getTableName(), getSchema());
  }

  /**
   * Shutdown hook.
   */
  @Override
  default void close()
      throws Exception {
  }

  /**
   * Fetch rows for a system table query. Implementations should apply projection/filter/offset/limit if feasible.
   */
  SystemTableResponse getRows(SystemTableRequest request)
      throws Exception;
}
