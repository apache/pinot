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
package org.apache.pinot.client.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.client.AbstractResultSet;
import org.apache.pinot.client.TextTable;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.utils.DataSchema;


/**
 * ResultSet which contains the ResultTable from the broker response of a sql query.
 */
public class GrpcResultSet extends AbstractResultSet {
  private final List<String> _columnNamesArray;
  private final List<String> _columnDataTypesArray;
  private final ArrayNode _currentBatchRows;

  public GrpcResultSet(DataSchema schema, Broker.BrokerResponse brokerResponse) {
    _columnNamesArray = new ArrayList<>(schema.size());
    _columnDataTypesArray = new ArrayList<>(schema.size());
    for (int i = 0; i < schema.size(); i++) {
      _columnNamesArray.add(schema.getColumnName(i));
      _columnDataTypesArray.add(schema.getColumnDataType(i).toString());
    }
    try {
      _currentBatchRows = GrpcUtils.extractRowsJson(brokerResponse);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getRowCount() {
    return _currentBatchRows.size();
  }

  @Override
  public int getColumnCount() {
    return _columnNamesArray.size();
  }

  @Override
  public String getColumnName(int columnIndex) {
    return _columnNamesArray.get(columnIndex);
  }

  @Override
  public String getColumnDataType(int columnIndex) {
    return _columnDataTypesArray.get(columnIndex);
  }

  @Override
  public String getString(int rowIndex, int columnIndex) {
    JsonNode jsonValue = _currentBatchRows.get(rowIndex).get(columnIndex);
    if (jsonValue.isTextual()) {
      return jsonValue.textValue();
    } else {
      return jsonValue.toString();
    }
  }

  public List<String> getAllColumns() {
    return _columnNamesArray;
  }

  public List<String> getAllColumnsDataTypes() {
    return _columnDataTypesArray;
  }

  @Override
  public int getGroupKeyLength() {
    return 0;
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    throw new AssertionError("No group key string for result table");
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    throw new AssertionError("No group key column name for result table");
  }

  @Override
  public String toString() {
    int numColumns = getColumnCount();
    TextTable table = new TextTable();
    String[] columnNames = new String[numColumns];
    String[] columnDataTypes = new String[numColumns];
    for (int c = 0; c < numColumns; c++) {
      columnNames[c] = _columnNamesArray.get(c);
      columnDataTypes[c] = _columnDataTypesArray.get(c);
    }
    table.addHeader(columnNames);
    table.addHeader(columnDataTypes);

    int numRows = getRowCount();
    for (int r = 0; r < numRows; r++) {
      String[] columnValues = new String[numColumns];
      for (int c = 0; c < numColumns; c++) {
        try {
          columnValues[c] = getString(r, c);
        } catch (Exception e) {
          columnNames[c] = "ERROR";
        }
      }
      table.addRow(columnValues);
    }
    return table.toString();
  }
}
