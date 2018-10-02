/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common.datatable;

import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.common.ObjectSerDeUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class DataTableImplV2 implements DataTable {
  private static final int VERSION = 2;

  // VERSION
  // NUM_ROWS
  // NUM_COLUMNS
  // DICTIONARY_MAP (START|SIZE)
  // METADATA (START|SIZE)
  // DATA_SCHEMA (START|SIZE)
  // FIXED_SIZE_DATA (START|SIZE)
  // VARIABLE_SIZE_DATA (START|SIZE)
  private static final int HEADER_SIZE = Integer.BYTES * 13;

  private final int _numRows;
  private final int _numColumns;
  private final DataSchema _dataSchema;
  private final int[] _columnOffsets;
  private final int _rowSizeInBytes;
  private final Map<String, Map<Integer, String>> _dictionaryMap;
  private final byte[] _fixedSizeDataBytes;
  private final ByteBuffer _fixedSizeData;
  private final byte[] _variableSizeDataBytes;
  private final ByteBuffer _variableSizeData;
  private final Map<String, String> _metadata;

  /**
   * Construct data table with results. (Server side)
   */
  public DataTableImplV2(int numRows, @Nonnull DataSchema dataSchema,
      @Nonnull Map<String, Map<Integer, String>> dictionaryMap, @Nonnull byte[] fixedSizeDataBytes,
      @Nonnull byte[] variableSizeDataBytes) {
    _numRows = numRows;
    _numColumns = dataSchema.size();
    _dataSchema = dataSchema;
    _columnOffsets = new int[_numColumns];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets);
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
  public DataTableImplV2() {
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
   * Construct data table from byte array. (broker side)
   */
  public DataTableImplV2(@Nonnull ByteBuffer byteBuffer) throws IOException {
    // Read header.
    _numRows = byteBuffer.getInt();
    _numColumns = byteBuffer.getInt();
    int dictionaryMapStart = byteBuffer.getInt();
    int dictionaryMapLength = byteBuffer.getInt();
    int metadataStart = byteBuffer.getInt();
    int metadataLength = byteBuffer.getInt();
    int dataSchemaStart = byteBuffer.getInt();
    int dataSchemaLength = byteBuffer.getInt();
    int fixedSizeDataStart = byteBuffer.getInt();
    int fixedSizeDataLength = byteBuffer.getInt();
    int variableSizeDataStart = byteBuffer.getInt();
    int variableSizeDataLength = byteBuffer.getInt();

    // Read dictionary.
    if (dictionaryMapLength != 0) {
      byte[] dictionaryMapBytes = new byte[dictionaryMapLength];
      byteBuffer.position(dictionaryMapStart);
      byteBuffer.get(dictionaryMapBytes);
      _dictionaryMap = deserializeDictionaryMap(dictionaryMapBytes);
    } else {
      _dictionaryMap = null;
    }

    // Read metadata.
    byte[] metadataBytes = new byte[metadataLength];
    byteBuffer.position(metadataStart);
    byteBuffer.get(metadataBytes);
    _metadata = deserializeMetadata(metadataBytes);

    // Read data schema.
    if (dataSchemaLength != 0) {
      byte[] schemaBytes = new byte[dataSchemaLength];
      byteBuffer.position(dataSchemaStart);
      byteBuffer.get(schemaBytes);
      _dataSchema = DataSchema.fromBytes(schemaBytes);
      _columnOffsets = new int[_dataSchema.size()];
      _rowSizeInBytes = DataTableUtils.computeColumnOffsets(_dataSchema, _columnOffsets);
    } else {
      _dataSchema = null;
      _columnOffsets = null;
      _rowSizeInBytes = 0;
    }

    // Read fixed size data.
    if (fixedSizeDataLength != 0) {
      _fixedSizeDataBytes = new byte[fixedSizeDataLength];
      byteBuffer.position(fixedSizeDataStart);
      byteBuffer.get(_fixedSizeDataBytes);
      _fixedSizeData = ByteBuffer.wrap(_fixedSizeDataBytes);
    } else {
      _fixedSizeDataBytes = null;
      _fixedSizeData = null;
    }

    // Read variable size data.
    if (variableSizeDataLength != 0) {
      _variableSizeDataBytes = new byte[variableSizeDataLength];
      byteBuffer.position(variableSizeDataStart);
      byteBuffer.get(_variableSizeDataBytes);
      _variableSizeData = ByteBuffer.wrap(_variableSizeDataBytes);
    } else {
      _variableSizeDataBytes = null;
      _variableSizeData = null;
    }
  }

  private Map<String, Map<Integer, String>> deserializeDictionaryMap(byte[] bytes) throws IOException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      int numDictionaries = dataInputStream.readInt();
      Map<String, Map<Integer, String>> dictionaryMap = new HashMap<>(numDictionaries);

      for (int i = 0; i < numDictionaries; i++) {
        String column = decodeString(dataInputStream);
        int dictionarySize = dataInputStream.readInt();
        Map<Integer, String> dictionary = new HashMap<>(dictionarySize);
        for (int j = 0; j < dictionarySize; j++) {
          int key = dataInputStream.readInt();
          String value = decodeString(dataInputStream);
          dictionary.put(key, value);
        }
        dictionaryMap.put(column, dictionary);
      }

      return dictionaryMap;
    }
  }

  private Map<String, String> deserializeMetadata(byte[] bytes) throws IOException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      int numEntries = dataInputStream.readInt();
      Map<String, String> metadata = new HashMap<>(numEntries);

      for (int i = 0; i < numEntries; i++) {
        String key = decodeString(dataInputStream);
        String value = decodeString(dataInputStream);
        metadata.put(key, value);
      }

      return metadata;
    }
  }

  private static String decodeString(DataInputStream dataInputStream) throws IOException {
    int length = dataInputStream.readInt();
    if (length == 0) {
      return StringUtils.EMPTY;
    } else {
      byte[] buffer = new byte[length];
      int numBytesRead = dataInputStream.read(buffer);
      assert numBytesRead == length;
      return StringUtil.decodeUtf8(buffer);
    }
  }

  @Override
  public void addException(@Nonnull ProcessingException processingException) {
    _metadata.put(EXCEPTION_METADATA_KEY + processingException.getErrorCode(), processingException.getMessage());
  }

  @Nonnull
  @Override
  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dataOutputStream.writeInt(VERSION);
    dataOutputStream.writeInt(_numRows);
    dataOutputStream.writeInt(_numColumns);
    int dataOffset = HEADER_SIZE;

    // Write dictionary.
    dataOutputStream.writeInt(dataOffset);
    byte[] dictionaryMapBytes = null;
    if (_dictionaryMap != null) {
      dictionaryMapBytes = serializeDictionaryMap();
      dataOutputStream.writeInt(dictionaryMapBytes.length);
      dataOffset += dictionaryMapBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write metadata.
    dataOutputStream.writeInt(dataOffset);
    byte[] metadataBytes = serializeMetadata();
    dataOutputStream.writeInt(metadataBytes.length);
    dataOffset += metadataBytes.length;

    // Write data schema.
    dataOutputStream.writeInt(dataOffset);
    byte[] dataSchemaBytes = null;
    if (_dataSchema != null) {
      dataSchemaBytes = _dataSchema.toBytes();
      dataOutputStream.writeInt(dataSchemaBytes.length);
      dataOffset += dataSchemaBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write fixed size data.
    dataOutputStream.writeInt(dataOffset);
    if (_fixedSizeDataBytes != null) {
      dataOutputStream.writeInt(_fixedSizeDataBytes.length);
      dataOffset += _fixedSizeDataBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write variable size data.
    dataOutputStream.writeInt(dataOffset);
    if (_variableSizeDataBytes != null) {
      dataOutputStream.writeInt(_variableSizeDataBytes.length);
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write actual data.
    if (dictionaryMapBytes != null) {
      dataOutputStream.write(dictionaryMapBytes);
    }
    dataOutputStream.write(metadataBytes);
    if (dataSchemaBytes != null) {
      dataOutputStream.write(dataSchemaBytes);
    }
    if (_fixedSizeDataBytes != null) {
      dataOutputStream.write(_fixedSizeDataBytes);
    }
    if (_variableSizeDataBytes != null) {
      dataOutputStream.write(_variableSizeDataBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  private byte[] serializeDictionaryMap() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_dictionaryMap.size());
    for (Entry<String, Map<Integer, String>> dictionaryMapEntry : _dictionaryMap.entrySet()) {
      String columnName = dictionaryMapEntry.getKey();
      Map<Integer, String> dictionary = dictionaryMapEntry.getValue();
      byte[] bytes = StringUtil.encodeUtf8(columnName);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
      dataOutputStream.writeInt(dictionary.size());

      for (Entry<Integer, String> dictionaryEntry : dictionary.entrySet()) {
        dataOutputStream.writeInt(dictionaryEntry.getKey());
        byte[] valueBytes = StringUtil.encodeUtf8(dictionaryEntry.getValue());
        dataOutputStream.writeInt(valueBytes.length);
        dataOutputStream.write(valueBytes);
      }
    }

    return byteArrayOutputStream.toByteArray();
  }

  private byte[] serializeMetadata() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_metadata.size());
    for (Entry<String, String> entry : _metadata.entrySet()) {
      byte[] keyBytes = StringUtil.encodeUtf8(entry.getKey());
      dataOutputStream.writeInt(keyBytes.length);
      dataOutputStream.write(keyBytes);

      byte[] valueBytes = StringUtil.encodeUtf8(entry.getValue());
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  @Nonnull
  @Override
  public Map<String, String> getMetadata() {
    return _metadata;
  }

  @Nullable
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
    _fixedSizeData.position(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    return _fixedSizeData.getInt();
  }

  @Override
  public long getLong(int rowId, int colId) {
    _fixedSizeData.position(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    return _fixedSizeData.getLong();
  }

  @Override
  public float getFloat(int rowId, int colId) {
    _fixedSizeData.position(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    return _fixedSizeData.getFloat();
  }

  @Override
  public double getDouble(int rowId, int colId) {
    _fixedSizeData.position(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    return _fixedSizeData.getDouble();
  }

  @Nonnull
  @Override
  public String getString(int rowId, int colId) {
    _fixedSizeData.position(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    int dictId = _fixedSizeData.getInt();
    return _dictionaryMap.get(_dataSchema.getColumnName(colId)).get(dictId);
  }

  @Nonnull
  @Override
  public <T> T getObject(int rowId, int colId) {
    int size = positionCursorInVariableBuffer(rowId, colId);
    int objectTypeValue = _variableSizeData.getInt();
    ByteBuffer byteBuffer = _variableSizeData.slice();
    byteBuffer.limit(size);
    return ObjectSerDeUtils.deserialize(byteBuffer, objectTypeValue);
  }

  @Nonnull
  @Override
  public int[] getIntArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    int[] ints = new int[length];
    for (int i = 0; i < length; i++) {
      ints[i] = _variableSizeData.getInt();
    }
    return ints;
  }

  @Nonnull
  @Override
  public long[] getLongArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    long[] longs = new long[length];
    for (int i = 0; i < length; i++) {
      longs[i] = _variableSizeData.getLong();
    }
    return longs;
  }

  @Nonnull
  @Override
  public float[] getFloatArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    float[] floats = new float[length];
    for (int i = 0; i < length; i++) {
      floats[i] = _variableSizeData.getFloat();
    }
    return floats;
  }

  @Nonnull
  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    int length = positionCursorInVariableBuffer(rowId, colId);
    double[] doubles = new double[length];
    for (int i = 0; i < length; i++) {
      doubles[i] = _variableSizeData.getDouble();
    }
    return doubles;
  }

  @Nonnull
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

  private int positionCursorInVariableBuffer(int rowId, int colId) {
    _fixedSizeData.position(rowId * _rowSizeInBytes + _columnOffsets[colId]);
    _variableSizeData.position(_fixedSizeData.getInt());
    return _fixedSizeData.getInt();
  }

  @Override
  public String toString() {
    if (_dataSchema == null) {
      return _metadata.toString();
    }

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(_dataSchema.toString()).append('\n');
    stringBuilder.append("numRows: ").append(_numRows).append('\n');

    _fixedSizeData.position(0);
    for (int rowId = 0; rowId < _numRows; rowId++) {
      for (int colId = 0; colId < _numColumns; colId++) {
        switch (_dataSchema.getColumnDataType(colId)) {
          case INT:
            stringBuilder.append(_fixedSizeData.getInt());
            break;
          case LONG:
            stringBuilder.append(_fixedSizeData.getLong());
            break;
          case FLOAT:
            stringBuilder.append(_fixedSizeData.getFloat());
            break;
          case DOUBLE:
            stringBuilder.append(_fixedSizeData.getDouble());
            break;
          case STRING:
            stringBuilder.append(_fixedSizeData.getInt());
            break;
          // Object and array.
          default:
            stringBuilder.append(String.format("(%s:%s)", _fixedSizeData.getInt(), _fixedSizeData.getInt()));
            break;
        }
        stringBuilder.append("\t");
      }
      stringBuilder.append("\n");
    }
    return stringBuilder.toString();
  }
}
