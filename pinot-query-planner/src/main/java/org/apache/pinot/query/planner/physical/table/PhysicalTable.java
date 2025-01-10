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
package org.apache.pinot.query.planner.physical.table;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;


public class PhysicalTable {
  private final String _rawTableName;
  private final String _tableName;
  private final TableType _tableType;
  private TableConfig _tableConfig;
  private final boolean _isDisabled;

  public PhysicalTable(String rawTableName, String tableName, TableType tableType, TableConfig tableConfig,
      boolean isDisabled) {
    _rawTableName = rawTableName;
    _tableName = tableName;
    _tableType = tableType;
    _tableConfig = tableConfig;
    _isDisabled = isDisabled;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  public String getTableName() {
    return _tableName;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public boolean isDisabled() {
    return _isDisabled;
  }

  public void setTableConfig(TableConfig tableConfig) {
    _tableConfig = tableConfig;
  }
}
