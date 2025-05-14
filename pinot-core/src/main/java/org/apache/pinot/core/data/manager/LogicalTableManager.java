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
package org.apache.pinot.core.data.manager;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;


public class LogicalTableManager {
  private final LogicalTableConfig _logicalTableConfig;
  private final Schema _logicalTableSchema;
  private final TableConfig _offlineTableConfig;
  private final TableConfig _realtimeTableConfig;

  public LogicalTableManager(LogicalTableConfig logicalTableConfig, Schema logicalTableSchema,
      TableConfig offlineTableConfig, TableConfig realtimeTableConfig) {
    _logicalTableConfig = logicalTableConfig;
    _logicalTableSchema = logicalTableSchema;
    _offlineTableConfig = offlineTableConfig;
    _realtimeTableConfig = realtimeTableConfig;
  }

  public LogicalTableConfig getLogicalTableConfig() {
    return _logicalTableConfig;
  }
  public Schema getLogicalTableSchema() {
    return _logicalTableSchema;
  }
  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }
  public TableConfig getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }
}
