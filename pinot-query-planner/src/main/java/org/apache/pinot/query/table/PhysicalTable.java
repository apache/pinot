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
package org.apache.pinot.query.table;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;


/**
 * The PhysicalTable class represents a OFFLINE or REALTIME table in the Pinot query planner.
 * It contains metadata about the table, such as its name, type, configuration, and status.
 */
public class PhysicalTable {
  public static final PhysicalTable EMPTY = new PhysicalTable(null, null, null, false, null, false);

  private final String _rawTableName;
  private final String _tableNameWithType;
  private final TableType _tableType;
  private final boolean _isRouteExists;
  private final TableConfig _tableConfig;
  private final boolean _isDisabled;

  /**
   * Constructs a PhysicalTable instance with the specified parameters.
   *
   * @param rawTableName the raw table name
   * @param tableNameWithType the table name
   * @param tableType the type of the table (offline or realtime)
   * @param isRouteExists whether the route exists for the table
   * @param tableConfig the configuration of the table
   * @param isDisabled whether the table is disabled
   */
  public PhysicalTable(String rawTableName, String tableNameWithType, TableType tableType, boolean isRouteExists,
      TableConfig tableConfig, boolean isDisabled) {
    _rawTableName = rawTableName;
    _tableNameWithType = tableNameWithType;
    _tableType = tableType;
    _isRouteExists = isRouteExists;
    _tableConfig = tableConfig;
    _isDisabled = isDisabled;
  }

  /**
   * Returns the raw table name.
   *
   * @return the raw table name
   */
  public String getRawTableName() {
    return _rawTableName;
  }

  /**
   * Returns the table name with type.
   *
   * @return the table name
   */
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  /**
   * Returns the type of the table.
   *
   * @return the table type
   */
  public TableType getTableType() {
    return _tableType;
  }

  /**
   * Returns the configuration of the table.
   *
   * @return the table configuration
   */
  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  /**
   * Returns whether the table is disabled.
   *
   * @return true if the table is disabled, false otherwise
   */
  public boolean isDisabled() {
    return _isDisabled;
  }

  /**
   * Returns whether the route exists for the table.
   *
   * @return true if the route exists, false otherwise
   */
  public boolean isRouteExists() {
    return _isRouteExists;
  }

  /**
   * Check if table config exists. If not, it is an EMPTY table and cannot be compared.
   * If table config exists, compare the table name with type.
   *
   * @param o The other physical table to compare with
   * @return true if the table name with type is the same, false otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PhysicalTable that = (PhysicalTable) o;

    if (_tableConfig == null || that._tableConfig == null) {
      return false;
    }

    return _tableNameWithType.equals(that._tableNameWithType);
  }

  @Override
  public int hashCode() {
    return _tableNameWithType.hashCode();
  }
}