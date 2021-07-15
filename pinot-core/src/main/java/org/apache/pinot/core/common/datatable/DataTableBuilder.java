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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.ByteArray;


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
public class DataTableBuilder {
  public static final int VERSION_2 = 2;
  public static final int VERSION_3 = 3;
  public static final int VERSION_4 = 4;
  private static int _version = VERSION_4;
  private final DataSchema _dataSchema;
  private final int[] _columnOffsets;
  private final int _rowSizeInBytes;
  // _dictionaryMap and _reverseDictionaryMap are used in V2 and V3, where each column use one dictionary to map String->Integer
  private final Map<String, Map<String, Integer>> _dictionaryMap = new HashMap<>();
  private final Map<String, Map<Integer, String>> _reverseDictionaryMap = new HashMap<>();
  // _dictionary and _reverseDictionary are used in V4, where all columns use a common dictionary to map String->Integer
  private final Map<String, Integer> _dictionary = new HashMap<>();
  private final Map<Integer, String> _reverseDictionary = new HashMap<>();
  private final ByteArrayOutputStream _fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  private final ByteArrayOutputStream _variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  private final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  private int _numRows;
  private ByteBuffer _currentRowDataByteBuffer;

  public DataTableBuilder(DataSchema dataSchema) {
    _dataSchema = dataSchema;
    _columnOffsets = new int[dataSchema.size()];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets, _version);
  }

  public static DataTable getEmptyDataTable() {
    switch (_version) {
      case VERSION_2:
        return new DataTableImplV2();
      case VERSION_3:
        return new DataTableImplV3();
      default:
        return new DataTableImplV4();
    }
  }

  public static void setCurrentDataTableVersion(int version) {
    if (version != VERSION_2 && version != VERSION_3 && version != VERSION_4) {
      throw new IllegalArgumentException("Unsupported version: " + version);
    }
    _version = version;
  }

  public void startRow() {
    _numRows++;
    _currentRowDataByteBuffer = ByteBuffer.allocate(_rowSizeInBytes);
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

  public void setColumn(int colId, String value) {
    if (_version == VERSION_2 || _version == VERSION_3) {
      String columnName = _dataSchema.getColumnName(colId);
      Map<String, Integer> dictionary = _dictionaryMap.get(columnName);

      if (dictionary == null) {
        dictionary = new HashMap<>();
        _dictionaryMap.put(columnName, dictionary);
        _reverseDictionaryMap.put(columnName, new HashMap<>());
      }
      Integer dictId = dictionary.get(value);
      if (dictId == null) {
        dictId = dictionary.size();
        dictionary.put(value, dictId);
        _reverseDictionaryMap.get(columnName).put(dictId, value);
      }

      _currentRowDataByteBuffer.position(_columnOffsets[colId]);
      _currentRowDataByteBuffer.putInt(dictId);
    } else {
      _dictionary.putIfAbsent(value, _dictionary.size());
      Integer dictId = _dictionary.get(value);
      _reverseDictionary.put(dictId, value);

      _currentRowDataByteBuffer.position(_columnOffsets[colId]);
      _currentRowDataByteBuffer.putInt(dictId);
    }
  }

  public void setColumn(int colId, ByteArray value)
      throws IOException {
    // NOTE: Use String to store bytes value in DataTable V2/V3 for backward-compatibility
    if (_version == VERSION_2 || _version == VERSION_3) {
      setColumn(colId, value.toHexString());
    } else {
      _currentRowDataByteBuffer.position(_columnOffsets[colId]);
      _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
      byte[] bytes = value.getBytes();
      _currentRowDataByteBuffer.putInt(bytes.length);
      _variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  public void setColumn(int colId, Object value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    int objectTypeValue = ObjectSerDeUtils.ObjectType.getObjectType(value).getValue();
    byte[] bytes = ObjectSerDeUtils.serialize(value, objectTypeValue);
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataOutputStream.writeInt(objectTypeValue);
    _variableSizeDataByteArrayOutputStream.write(bytes);
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

    if (_version == VERSION_2 || _version == VERSION_3) {
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
    } else {
      for (String value : values) {
        _dictionary.putIfAbsent(value, _dictionary.size());
        Integer dictId = _dictionary.get(value);
        _reverseDictionary.put(dictId, value);
        _variableSizeDataOutputStream.writeInt(dictId);
      }
    }
  }

  public void finishRow()
      throws IOException {
    _fixedSizeDataByteArrayOutputStream.write(_currentRowDataByteBuffer.array());
  }

  public DataTable build() {
    switch (_version) {
      case VERSION_2:
        return new DataTableImplV2(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
      case VERSION_3:
        return new DataTableImplV3(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
      default:
        return new DataTableImplV4(_numRows, _dataSchema, _reverseDictionary,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
    }
  }
}
