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
package org.apache.pinot.core.common.datatable;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.datatable.DataTableImplV2;
import org.apache.pinot.common.datatable.DataTableImplV3;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * Kept for backward compatible. Things improved in the newer versions:
 * - Float size (should be 4 instead of 8)
 * - Store bytes as variable size data instead of String
 * - Use one dictionary for all columns (save space)
 * - Support setting nullRowIds
 */
@Deprecated
public class DataTableBuilderV2V3 extends BaseDataTableBuilder {
  private final Map<String, Map<String, Integer>> _dictionaryMap = new HashMap<>();
  private final Map<String, Map<Integer, String>> _reverseDictionaryMap = new HashMap<>();

  public DataTableBuilderV2V3(DataSchema dataSchema, int version) {
    super(dataSchema, version);
    Preconditions.checkArgument(version <= DataTableFactory.VERSION_3);
  }

  @Override
  public void setColumn(int colId, String value) {
    String columnName = _dataSchema.getColumnName(colId);
    Map<String, Integer> dictionary = _dictionaryMap.get(columnName);
    if (dictionary == null) {
      dictionary = new HashMap<>();
      _dictionaryMap.put(columnName, dictionary);
      _reverseDictionaryMap.put(columnName, new HashMap<>());
    }

    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    Integer dictId = dictionary.get(value);
    if (dictId == null) {
      dictId = dictionary.size();
      dictionary.put(value, dictId);
      _reverseDictionaryMap.get(columnName).put(dictId, value);
    }
    _currentRowDataByteBuffer.putInt(dictId);
  }

  @Override
  public void setColumn(int colId, ByteArray value)
      throws IOException {
    setColumn(colId, value.toHexString());
  }

  @Override
  public void setColumn(int colId, String[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);

    String columnName = _dataSchema.getColumnName(colId);
    Map<String, Integer> dictionary = _dictionaryMap.get(columnName);
    if (dictionary == null) {
      dictionary = new HashMap<>();
      _dictionaryMap.put(columnName, dictionary);
      _reverseDictionaryMap.put(columnName, new HashMap<>());
    }

    for (String value : values) {
      Integer dictId = dictionary.get(value);
      if (dictId == null) {
        dictId = dictionary.size();
        dictionary.put(value, dictId);
        _reverseDictionaryMap.get(columnName).put(dictId, value);
      }
      _variableSizeDataOutputStream.writeInt(dictId);
    }
  }

  @Override
  public void setNullRowIds(@Nullable RoaringBitmap nullRowIds)
      throws IOException {
    throw new UnsupportedOperationException("Not supported before DataTable V4");
  }

  @Override
  public DataTable build() {
    byte[] fixedSizeDataBytes = _fixedSizeDataByteArrayOutputStream.toByteArray();
    byte[] variableSizeDataBytes = _variableSizeDataByteArrayOutputStream.toByteArray();
    if (_version == DataTableFactory.VERSION_2) {
      return new DataTableImplV2(_numRows, _dataSchema, _reverseDictionaryMap, fixedSizeDataBytes,
          variableSizeDataBytes);
    } else {
      return new DataTableImplV3(_numRows, _dataSchema, _reverseDictionaryMap, fixedSizeDataBytes,
          variableSizeDataBytes);
    }
  }
}
