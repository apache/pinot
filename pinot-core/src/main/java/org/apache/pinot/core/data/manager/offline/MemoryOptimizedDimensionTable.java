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


class MemoryOptimizedDimensionTable implements DimensionTable {

  private Map<PrimaryKey, LookupRecordLocation> _lookupTable;
  private final Schema _tableSchema;
  private final List<String> _primaryKeyColumns;

  MemoryOptimizedDimensionTable(Schema tableSchema, List<String> primaryKeyColumns) {
    this(tableSchema, primaryKeyColumns, new HashMap<>());
  }

  MemoryOptimizedDimensionTable(Schema tableSchema, List<String> primaryKeyColumns,
      Map<PrimaryKey, LookupRecordLocation> lookupTable) {
    _lookupTable = lookupTable;
    _tableSchema = tableSchema;
    _primaryKeyColumns = primaryKeyColumns;
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Override
  public GenericRow get(PrimaryKey pk) {
    LookupRecordLocation lookupRecordLocation = _lookupTable.get(pk);
    if (lookupRecordLocation == null) {
      return null;
    }
    return lookupRecordLocation.getIndexSegment().getRecord(lookupRecordLocation.getDocId(), new GenericRow());
  }

  @Override
  public boolean isEmpty() {
    return _lookupTable.isEmpty();
  }

  @Override
  public FieldSpec getFieldSpecFor(String columnName) {
    return _tableSchema.getFieldSpecFor(columnName);
  }
}
