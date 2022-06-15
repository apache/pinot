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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.NullBitmapUtils;
import org.apache.pinot.core.common.DataTableBuilder;
import org.apache.pinot.core.common.DataTableFactory;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 *
 * Datatable that holds data in a matrix form. The purpose of this class is to
 * provide a way to construct a datatable and ability to serialize and
 * deserialize.<br>
 * Why can't we use existing serialization/deserialization mechanism. Most
 * existing techniques protocol buffer, thrift, avro are optimized for
 * transporting a single record but Pinot transfers quite a lot of data from
 * server to broker during the scatter/gather operation. The cost of
 * serialization and deserialization directly impacts the performance. Most
 * ser/deser requires us to convert the primitives data types in objects like
 * Integer etc. This is waste of cpu resource and increase the payload size. We
 * optimize the data format for Pinot usecase. We can also support lazy
 * construction of obejcts. Infact we retain the bytes as it is and will be able
 * to lookup the a field directly within a byte buffer.<br>
 *
 * USAGE:
 *
 * Datatable is initialized with the schema of the table. Schema describes the
 * columnnames, their order and data type for each column.<br>
 * Each row must follow the same convention. We don't support MultiValue columns
 * for now. Format,
 * |VERSION,DATA_START_OFFSET,DICTIONARY_START_OFFSET,INDEX_START_OFFSET
 * ,METADATA_START_OFFSET | |&lt;DATA&gt; |
 *
 * |&lt;DICTIONARY&gt;|
 *
 *
 * |&lt;METADATA&gt;| Data contains the actual values written by the application We
 * first write the entire data in its raw byte format. For example if you data
 * type is Int, it will write 4 bytes. For most data types that are fixed width,
 * we just write the raw data. For special cases like String, we create a
 * dictionary. Dictionary will be never exposed to the user. All conversions
 * will be done internally. In future, we might decide dynamically if dictionary
 * creation is needed, for now we will always create dictionaries for string
 * columns. During deserialization we will always load the dictionary
 * first.Overall having dictionary allow us to convert data table into a fixed
 * width matrix and thus allowing look up and easy traversal.
 *
 *
 */
// TODO: potential optimizations (DataTableV3 and below)
// TODO:   - Fix float size.
// TODO:     note:  (Fixed in V4, remove this comment once V3 is deprecated)
// TODO:   - Given a data schema, write all values one by one instead of using rowId and colId to position (save time).
// TODO:     note:  (Fixed in V4, remove this comment once V3 is deprecated)
// TODO:   - Store bytes as variable size data instead of String
// TODO:     note:  (Fixed in V4, remove this comment once V3 is deprecated)
// TODO:   - use builder factory pattern to for different version so that no version check per build.
// TODO:   - Use one dictionary for all columns (save space).
// TODO:     note:  (Fixed in V4, remove this comment once V3 is deprecated)

public class DataTableBuilderV2V3 implements DataTableBuilder {
  protected final DataSchema _dataSchema;
  protected final int[] _columnOffsets;
  protected final int _rowSizeInBytes;
  protected final Map<String, Map<String, Integer>> _dictionaryMap = new HashMap<>();
  protected final Map<String, Map<Integer, String>> _reverseDictionaryMap = new HashMap<>();
  protected final ByteArrayOutputStream _fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  protected final ByteArrayOutputStream _variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  protected final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  protected int _numRows;
  protected ByteBuffer _currentRowDataByteBuffer;

  public DataTableBuilderV2V3(DataSchema dataSchema) {
    _dataSchema = dataSchema;
    _columnOffsets = new int[dataSchema.size()];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets,
        DataTableFactory.getCurrentDataTableVersion());
  }

  public void startRow() {
    _numRows++;
    _currentRowDataByteBuffer = ByteBuffer.allocate(_rowSizeInBytes);
  }

  public void setNullRowIds(RoaringBitmap nullBitmap)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  public void setColumn(int colId, boolean value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    if (value) {
      _currentRowDataByteBuffer.put((byte) 1);
    } else {
      _currentRowDataByteBuffer.put((byte) 0);
    }
  }

  public void setColumn(int colId, byte value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.put(value);
  }

  public void setColumn(int colId, char value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putChar(value);
  }

  public void setColumn(int colId, short value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putShort(value);
  }

  public void setColumn(int colId, int value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(value);
  }

  public void setColumn(int colId, long value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putLong(value);
  }

  public void setColumn(int colId, float value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putFloat(value);
  }

  public void setColumn(int colId, double value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putDouble(value);
  }

  public void setColumn(int colId, BigDecimal value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = BigDecimalUtils.serialize(value);
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

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

  public void setColumn(int colId, ByteArray value)
      throws IOException {
    if (DataTableFactory.getCurrentDataTableVersion() >= 4) {
      _currentRowDataByteBuffer.position(_columnOffsets[colId]);
      _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
      byte[] bytes = value.getBytes();
      _currentRowDataByteBuffer.putInt(bytes.length);
      _variableSizeDataByteArrayOutputStream.write(bytes);
    } else {
      // NOTE: Use String to store bytes value in DataTable V2 for backward-compatibility
      setColumn(colId, value.toHexString());
    }
  }

  public void setColumn(int colId, Object value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    int objectTypeValue = ObjectSerDeUtils.ObjectType.getObjectType(value).getValue();
    if (objectTypeValue == ObjectSerDeUtils.ObjectType.Null.getValue()) {
      _currentRowDataByteBuffer.putInt(0);
      _variableSizeDataOutputStream.writeInt(objectTypeValue);
    } else {
      byte[] bytes = ObjectSerDeUtils.serialize(value, objectTypeValue);
      _currentRowDataByteBuffer.putInt(bytes.length);
      _variableSizeDataOutputStream.writeInt(objectTypeValue);
      _variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  public void setColumn(int colId, int[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (int value : values) {
      _variableSizeDataOutputStream.writeInt(value);
    }
  }

  public void setColumn(int colId, long[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (long value : values) {
      _variableSizeDataOutputStream.writeLong(value);
    }
  }

  public void setColumn(int colId, float[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (float value : values) {
      _variableSizeDataOutputStream.writeFloat(value);
    }
  }

  public void setColumn(int colId, double[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (double value : values) {
      _variableSizeDataOutputStream.writeDouble(value);
    }
  }

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

  public void finishRow()
      throws IOException {
    _fixedSizeDataByteArrayOutputStream.write(_currentRowDataByteBuffer.array());
  }

  public DataTable build() {
    int version = DataTableFactory.getCurrentDataTableVersion();
    switch (version) {
      case DataTableFactory.VERSION_2:
        return new DataTableImplV2(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
      case DataTableFactory.VERSION_3:
        return new DataTableImplV3(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
      default:
        throw new IllegalStateException("Unexpected value: " + version);
    }
  }
}
