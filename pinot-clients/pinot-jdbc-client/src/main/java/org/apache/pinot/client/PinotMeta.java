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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PinotMeta {

  private List<List<Object>> _rows;
  private Map<String, List<String>> _dataSchema = new HashMap<>();

  public PinotMeta() {

  }

  public PinotMeta(List<List<Object>> rows, Map<String, List<String>> dataSchema) {
    _rows = rows;
    _dataSchema = dataSchema;
  }

  public List<List<Object>> getRows() {
    return _rows;
  }

  public void setRows(List<List<Object>> rows) {
    _rows = rows;
  }

  public void addRow(List<Object> row) {
    if (_rows == null) {
      _rows = new ArrayList<>();
    }
    _rows.add(row);
  }

  public Map<String, List<String>> getDataSchema() {
    return _dataSchema;
  }

  public void setDataSchema(Map<String, List<String>> dataSchema) {
    _dataSchema = dataSchema;
  }

  public void setColumnNames(String[] columnNames) {
    _dataSchema.put("columnNames", Arrays.asList(columnNames));
  }

  public void setColumnDataTypes(String[] columnDataTypes) {
    _dataSchema.put("columnDataTypes", Arrays.asList(columnDataTypes));
  }
}
