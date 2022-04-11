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
package org.apache.pinot.core.data.manager.offline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


class DimensionTable {

  private final Map<PrimaryKey, GenericRow> _lookupTable;
  private final Schema _tableSchema;
  private final List<String> _primaryKeyColumns;

  DimensionTable(Schema tableSchema, List<String> primaryKeyColumns) {
    this(tableSchema, primaryKeyColumns, new HashMap<>());
  }

  DimensionTable(Schema tableSchema, List<String> primaryKeyColumns, Map<PrimaryKey, GenericRow> lookupTable) {
    _lookupTable = lookupTable;
    _tableSchema = tableSchema;
    _primaryKeyColumns = primaryKeyColumns;
  }

  List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  GenericRow get(PrimaryKey pk) {
    return _lookupTable.get(pk);
  }

  boolean isEmpty() {
    return _lookupTable.isEmpty();
  }

  FieldSpec getFieldSpecFor(String columnName) {
    return _tableSchema.getFieldSpecFor(columnName);
  }
}
