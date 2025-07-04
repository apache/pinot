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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class FastLookupDimensionTable implements DimensionTable {

  private final Object2ObjectOpenCustomHashMap<Object[], Object[]> _lookupTable;
  private final Schema _tableSchema;
  private final List<String> _primaryKeyColumns;
  private final List<String> _valueColumns;
  private final int _keysNum;

  private final Object2IntOpenHashMap<String> _columnNamesToIdx;

  FastLookupDimensionTable(Schema tableSchema,
      List<String> primaryKeyColumns,
      List<String> valueColumns,
      Object2ObjectOpenCustomHashMap<Object[], Object[]> lookupTable) {
    _lookupTable = lookupTable;
    _tableSchema = tableSchema;
    _primaryKeyColumns = primaryKeyColumns;
    _keysNum = _primaryKeyColumns.size();
    _valueColumns = valueColumns;

    _columnNamesToIdx = new Object2IntOpenHashMap<>(_primaryKeyColumns.size() + valueColumns.size());
    _columnNamesToIdx.defaultReturnValue(Integer.MIN_VALUE);

    int idx = 0;
    for (String column : primaryKeyColumns) {
      _columnNamesToIdx.put(column, idx++);
    }
    for (String column : valueColumns) {
      _columnNamesToIdx.put(column, idx++);
    }
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Nullable
  @Override
  public FieldSpec getFieldSpecFor(String columnName) {
    return _tableSchema.getFieldSpecFor(columnName);
  }

  @Override
  public boolean isEmpty() {
    return _lookupTable.isEmpty();
  }

  @Override
  public boolean containsKey(PrimaryKey pk) {
    return _lookupTable.containsKey(pk.getValues());
  }

  /**
   * This method returns GenericRow, which has big memory and cpu overhead.
   */
  @Deprecated
  @Nullable
  @Override
  public GenericRow getRow(PrimaryKey pk) {
    Object[] rawPk = pk.getValues();
    Object[] value = _lookupTable.get(rawPk);
    if (value == null) {
      return null;
    }

    GenericRow row = new GenericRow();
    int pIdx = 0;
    for (String column : _primaryKeyColumns) {
      row.putValue(column, rawPk[pIdx++]);
    }

    int vIdx = 0;
    for (String column : _valueColumns) {
      row.putValue(column, value[vIdx++]);
    }

    return row;
  }

  @Nullable
  @Override
  public Object getValue(PrimaryKey pk, String columnName) {
    Object[] value = _lookupTable.get(pk.getValues());
    if (value == null) {
      return null;
    }

    return getValue(pk, columnName, value);
  }

  private Object getValue(PrimaryKey pk, String columnName, Object[] values) {
    int idx = _columnNamesToIdx.getInt(columnName);
    if (idx < 0) {
      return null;
    } else if (idx < _keysNum) {
      return pk.getValues()[idx];
    } else {
      return values[idx - _keysNum];
    }
  }

  @Nullable
  @Override
  public Object[] getValues(PrimaryKey pk, String[] columnNames) {
    Object[] rowValues = _lookupTable.get(pk.getValues());
    if (rowValues == null) {
      return null;
    }
    int numColumns = columnNames.length;
    Object[] result = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      result[i] = getValue(pk, columnNames[i], rowValues);
    }
    return result;
  }

  @Override
  public void close() {
  }
}
