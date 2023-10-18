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
package org.apache.pinot.common.datatable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Base implementation of the DataTable interface.
 */
public abstract class BaseDataTable implements DataTable {
  protected int _numRows;
  protected int _numColumns;
  protected DataSchema _dataSchema;
  protected int[] _columnOffsets;
  protected int _rowSizeInBytes;
  protected Map<String, Map<Integer, String>> _dictionaryMap;
  protected byte[] _fixedSizeDataBytes;
  protected ByteBuffer _fixedSizeData;
  protected byte[] _variableSizeDataBytes;
  protected ByteBuffer _variableSizeData;
  protected Map<String, String> _metadata;

  public BaseDataTable(int numRows, DataSchema dataSchema, Map<String, Map<Integer, String>> dictionaryMap,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    _numRows = numRows;
    _numColumns = dataSchema.size();
    _dataSchema = dataSchema;
    _columnOffsets = new int[_numColumns];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets, getVersion());
    _dictionaryMap = dictionaryMap;
    _fixedSizeDataBytes = fixedSizeDataBytes;
    _fixedSizeData = ByteBuffer.wrap(fixedSizeDataBytes);
    _variableSizeDataBytes = variableSizeDataBytes;
    _variableSizeData = ByteBuffer.wrap(variableSizeDataBytes);
    _metadata = new HashMap<>();
  }

  /**
   * Construct empty data table. (Server side)
   */
  public BaseDataTable() {
    _numRows = 0;
    _numColumns = 0;
    _dataSchema = null;
    _columnOffsets = null;
    _rowSizeInBytes = 0;
    _dictionaryMap = null;
    _fixedSizeDataBytes = null;
    _fixedSizeData = null;
    _variableSizeDataBytes = null;
    _variableSizeData = null;
    _metadata = new HashMap<>();
  }

  /**
   * Helper method to serialize dictionary map.
   */
  protected byte[] serializeDictionaryMap()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_dictionaryMap.size());
    for (Map.Entry<String, Map<Integer, String>> dictionaryMapEntry : _dictionaryMap.entrySet()) {
      String columnName = dictionaryMapEntry.getKey();
      Map<Integer, String> dictionary = dictionaryMapEntry.getValue();
      byte[] bytes = columnName.getBytes(UTF_8);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
      dataOutputStream.writeInt(dictionary.size());

      for (Map.Entry<Integer, String> dictionaryEntry : dictionary.entrySet()) {
        dataOutputStream.writeInt(dictionaryEntry.getKey());
        byte[] valueBytes = dictionaryEntry.getValue().getBytes(UTF_8);
        dataOutputStream.writeInt(valueBytes.length);
        dataOutputStream.write(valueBytes);
      }
    }

    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Helper method to deserialize dictionary map.
   */
  protected Map<String, Map<Integer, String>> deserializeDictionaryMap(ByteBuffer buffer)
      throws IOException {
    int numDictionaries = buffer.getInt();
    Map<String, Map<Integer, String>> dictionaryMap = new HashMap<>(HashUtil.getHashMapCapacity(numDictionaries));

    for (int i = 0; i < numDictionaries; i++) {
      String column = DataTableUtils.decodeString(buffer);
      int dictionarySize = buffer.getInt();
      Map<Integer, String> dictionary = new HashMap<>(HashUtil.getHashMapCapacity(dictionarySize));
      for (int j = 0; j < dictionarySize; j++) {
        int key = buffer.getInt();
        String value = DataTableUtils.decodeString(buffer);
        dictionary.put(key, value);
      }
      dictionaryMap.put(column, dictionary);
    }

    return dictionaryMap;
  }

  @Override
  public Map<String, String> getMetadata() {
    return _metadata;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public int getNumberOfRows() {
    return _numRows;
  }

  @Override
  public int getInt(int rowId, int colId) {
    return _fixedSizeData.getInt(rowId * _rowSizeInBytes + _columnOffsets[colId]);
  }

  @Override
  public long getLong(int rowId, int colId) {
    return _fixedSizeData.getLong(rowId * _rowSizeInBytes + _columnOffsets[colId]);
  }

  @Override
  public float getFloat(int rowId, int colId) {
    return _fixedSizeData.getFloat(rowId * _rowSizeInBytes + _columnOffsets[colId]);
  }

  @Override
  public double getDouble(int rowId, int colId) {
    return _fixedSizeData.getDouble(rowId * _rowSizeInBytes + _columnOffsets[colId]);
  }

  @Override
  public BigDecimal getBigDecimal(int rowId, int colId) {
    int size = positionCursorInVariableBuffer(rowId, colId);
    ByteBuffer byteBuffer = _variableSizeData.slice();
    byteBuffer.limit(size);
    return BigDecimalUtils.deserialize(byteBuffer);
  }

  @Override
  public String getString(int rowId, int colId) {
    int dictId = _fixedSizeData.getInt(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    return _dictionaryMap.get(_dataSchema.getColumnName(colId)).get(dictId);
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    // NOTE: DataTable V2/V3 uses String to store BYTES value
    return BytesUtils.toByteArray(getString(rowId, colId));
  }

  @Override
  public int[] getIntArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    int[] ints = new int[length];
    for (int i = 0; i < length; i++) {
      ints[i] = _variableSizeData.getInt();
    }
    return ints;
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    long[] longs = new long[length];
    for (int i = 0; i < length; i++) {
      longs[i] = _variableSizeData.getLong();
    }
    return longs;
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    float[] floats = new float[length];
    for (int i = 0; i < length; i++) {
      floats[i] = _variableSizeData.getFloat();
    }
    return floats;
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    double[] doubles = new double[length];
    for (int i = 0; i < length; i++) {
      doubles[i] = _variableSizeData.getDouble();
    }
    return doubles;
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    String[] strings = new String[length];
    Map<Integer, String> dictionary = _dictionaryMap.get(_dataSchema.getColumnName(colId));
    for (int i = 0; i < length; i++) {
      strings[i] = dictionary.get(_variableSizeData.getInt());
    }
    return strings;
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    int size = positionCursorInVariableBuffer(rowId, colId);
    int type = _variableSizeData.getInt();
    if (size == 0) {
      assert type == CustomObject.NULL_TYPE_VALUE;
      return null;
    }
    ByteBuffer buffer = _variableSizeData.slice();
    buffer.limit(size);
    return new CustomObject(type, buffer);
  }

  @Nullable
  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    return null;
  }

  private int positionCursorInVariableBuffer(int rowId, int colId) {
    int offset = rowId * _rowSizeInBytes + _columnOffsets[colId];
    _variableSizeData.position(_fixedSizeData.getInt(offset));
    return _fixedSizeData.getInt(offset + 4);
  }

  @Override
  public String toString() {
    if (_dataSchema == null) {
      return _metadata.toString();
    } else {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("resultSchema:").append('\n');
      stringBuilder.append(_dataSchema).append('\n');
      stringBuilder.append("numRows: ").append(_numRows).append('\n');
      stringBuilder.append("metadata: ").append(_metadata.toString()).append('\n');
      return stringBuilder.toString();
    }
  }
}
