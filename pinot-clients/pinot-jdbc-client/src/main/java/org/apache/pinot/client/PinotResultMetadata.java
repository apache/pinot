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
package org.apache.pinot.client;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.client.base.AbstractBaseResultSetMetadata;
import org.apache.pinot.client.utils.DriverUtils;


public class PinotResultMetadata extends AbstractBaseResultSetMetadata {
  private int _totalColumns;
  private Map<Integer, String> _columns = new HashMap<>();
  private Map<Integer, String> _columnDataTypes = new HashMap<>();

  public PinotResultMetadata(int totalColumns, Map<String, Integer> columnsNameToIndex,
      Map<Integer, String> columnDataTypes) {
    _totalColumns = totalColumns;
    _columnDataTypes = columnDataTypes;
    for (Map.Entry<String, Integer> entry : columnsNameToIndex.entrySet()) {
      _columns.put(entry.getValue(), entry.getKey());
    }
  }

  private void validateState(int column)
      throws SQLException {
    if (column > _totalColumns) {
      throw new SQLException("Column Index " + column + "is greater than total columns " + _totalColumns);
    }
  }

  @Override
  public int getColumnCount()
      throws SQLException {
    return _totalColumns;
  }

  @Override
  public String getColumnName(int column)
      throws SQLException {
    validateState(column);
    return _columns.getOrDefault(column, "");
  }

  @Override
  public String getColumnClassName(int column)
      throws SQLException {
    String columnTypeName = getColumnTypeName(column);
    return DriverUtils.getJavaClassName(columnTypeName);
  }

  @Override
  public int getColumnType(int column)
      throws SQLException {
    String columnTypeName = getColumnTypeName(column);
    return DriverUtils.getSQLDataType(columnTypeName);
  }

  @Override
  public String getColumnTypeName(int column)
      throws SQLException {
    validateState(column);
    return _columnDataTypes.get(column);
  }
}
