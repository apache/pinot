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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Datatable V4 Implementation is a wrapper around the Row-based data block.
 */
@InterfaceStability.Evolving
public class DataTableImplV4 implements DataTable {

  protected static final int HEADER_SIZE = Integer.BYTES * 13;
  // _errCodeToExceptionMap stores exceptions as a map of errorCode->errorMessage
  protected Map<Integer, String> _errCodeToExceptionMap;

  protected int _numRows;
  protected int _numColumns;
  protected int _fixDataSize;
  protected DataSchema _dataSchema;
  protected String[] _stringDictionary;
  protected byte[] _fixedSizeDataBytes;
  protected ByteBuffer _fixedSizeData;
  protected byte[] _variableSizeDataBytes;
  protected ByteBuffer _variableSizeData;
  protected Map<String, String> _metadata;
  protected int[] _columnOffsets;
  protected int _rowSizeInBytes;

  public DataTableImplV4() {
    _numRows = 0;
    _numColumns = 0;
    _fixDataSize = 0;
    _dataSchema = null;
    _stringDictionary = null;
    _fixedSizeDataBytes = null;
    _fixedSizeData = null;
    _variableSizeDataBytes = null;
    _variableSizeData = null;
    _rowSizeInBytes = 0;
    _columnOffsets = null;
    _metadata = new HashMap<>();
    _errCodeToExceptionMap = new HashMap<>();
  }

  public DataTableImplV4(int numRows, DataSchema dataSchema, String[] stringDictionary,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    _numRows = numRows;
    _dataSchema = dataSchema;
    _numColumns = dataSchema == null ? 0 : dataSchema.size();
    _fixDataSize = 0;
    _stringDictionary = stringDictionary;
    _fixedSizeDataBytes = fixedSizeDataBytes;
    _fixedSizeData = ByteBuffer.wrap(fixedSizeDataBytes);
    _variableSizeDataBytes = variableSizeDataBytes;
    _variableSizeData = ByteBuffer.wrap(variableSizeDataBytes);
    _metadata = new HashMap<>();
    _errCodeToExceptionMap = new HashMap<>();
    computeBlockObjectConstants();
  }

  public DataTableImplV4(ByteBuffer byteBuffer)
      throws IOException {
    // Read header.
    _numRows = byteBuffer.getInt();
    _numColumns = byteBuffer.getInt();
    int exceptionsStart = byteBuffer.getInt();
    int exceptionsLength = byteBuffer.getInt();
    int dictionaryMapStart = byteBuffer.getInt();
    int dictionaryMapLength = byteBuffer.getInt();
    int dataSchemaStart = byteBuffer.getInt();
    int dataSchemaLength = byteBuffer.getInt();
    int fixedSizeDataStart = byteBuffer.getInt();
    int fixedSizeDataLength = byteBuffer.getInt();
    int variableSizeDataStart = byteBuffer.getInt();
    int variableSizeDataLength = byteBuffer.getInt();

    // Read exceptions.
    if (exceptionsLength != 0) {
      byteBuffer.position(exceptionsStart);
      _errCodeToExceptionMap = deserializeExceptions(byteBuffer);
    } else {
      _errCodeToExceptionMap = new HashMap<>();
    }

    // Read dictionary.
    if (dictionaryMapLength != 0) {
      byteBuffer.position(dictionaryMapStart);
      _stringDictionary = deserializeStringDictionary(byteBuffer);
    } else {
      _stringDictionary = null;
    }

    // Read data schema.
    if (dataSchemaLength != 0) {
      byteBuffer.position(dataSchemaStart);
      _dataSchema = DataSchema.fromBytes(byteBuffer);
    } else {
      _dataSchema = null;
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
    _variableSizeDataBytes = new byte[variableSizeDataLength];
    if (variableSizeDataLength != 0) {
      byteBuffer.position(variableSizeDataStart);
      byteBuffer.get(_variableSizeDataBytes);
    }
    _variableSizeData = ByteBuffer.wrap(_variableSizeDataBytes);

    // Read metadata.
    int metadataLength = byteBuffer.getInt();
    if (metadataLength != 0) {
      _metadata = deserializeMetadata(byteBuffer);
    }

    // Compute extra constants.
    computeBlockObjectConstants();
  }

  @Override
  public int getVersion() {
    return DataTableFactory.VERSION_4;
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

  // --------------------------------------------------------------------------
  // Fixed sized element access.
  // --------------------------------------------------------------------------

  @Override
  public int getInt(int rowId, int colId) {
    return _fixedSizeData.getInt(getOffsetInFixedBuffer(rowId, colId));
  }

  @Override
  public long getLong(int rowId, int colId) {
    return _fixedSizeData.getLong(getOffsetInFixedBuffer(rowId, colId));
  }

  @Override
  public float getFloat(int rowId, int colId) {
    return _fixedSizeData.getFloat(getOffsetInFixedBuffer(rowId, colId));
  }

  @Override
  public double getDouble(int rowId, int colId) {
    return _fixedSizeData.getDouble(getOffsetInFixedBuffer(rowId, colId));
  }

  @Override
  public BigDecimal getBigDecimal(int rowId, int colId) {
    int size = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    ByteBuffer byteBuffer = _variableSizeData.slice();
    byteBuffer.limit(size);
    return BigDecimalUtils.deserialize(byteBuffer);
  }

  @Override
  public String getString(int rowId, int colId) {
    return _stringDictionary[_fixedSizeData.getInt(getOffsetInFixedBuffer(rowId, colId))];
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    int size = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    byte[] buffer = new byte[size];
    _variableSizeData.get(buffer);
    return new ByteArray(buffer);
  }

  // --------------------------------------------------------------------------
  // Variable sized element access.
  // --------------------------------------------------------------------------

  @Override
  public int[] getIntArray(int rowId, int colId) {
    int length = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    int[] ints = new int[length];
    for (int i = 0; i < length; i++) {
      ints[i] = _variableSizeData.getInt();
    }
    return ints;
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    int length = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    long[] longs = new long[length];
    for (int i = 0; i < length; i++) {
      longs[i] = _variableSizeData.getLong();
    }
    return longs;
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    int length = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    float[] floats = new float[length];
    for (int i = 0; i < length; i++) {
      floats[i] = _variableSizeData.getFloat();
    }
    return floats;
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    int length = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    double[] doubles = new double[length];
    for (int i = 0; i < length; i++) {
      doubles[i] = _variableSizeData.getDouble();
    }
    return doubles;
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    int length = positionOffsetInVariableBufferAndGetLength(rowId, colId);
    String[] strings = new String[length];
    for (int i = 0; i < length; i++) {
      strings[i] = _stringDictionary[_variableSizeData.getInt()];
    }
    return strings;
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    int size = positionOffsetInVariableBufferAndGetLength(rowId, colId);
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
    // _fixedSizeData stores two ints per col's null bitmap: offset, and length.
    int position = _fixDataSize + colId * Integer.BYTES * 2;
    if (_fixedSizeData == null || position >= _fixedSizeData.limit()) {
      return null;
    }
    _fixedSizeData.position(position);
    int offset = _fixedSizeData.getInt();
    int bytesLength = _fixedSizeData.getInt();
    if (bytesLength > 0) {
      _variableSizeData.position(offset);
      byte[] nullBitmapBytes = new byte[bytesLength];
      _variableSizeData.get(nullBitmapBytes);
      return RoaringBitmapUtils.deserialize(nullBitmapBytes);
    } else {
      return null;
    }
  }

  // --------------------------------------------------------------------------
  // Ser/De and exception handling
  // --------------------------------------------------------------------------

  /**
   * Helper method to serialize dictionary map.
   */
  protected byte[] serializeStringDictionary()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_stringDictionary.length);
    for (String entry : _stringDictionary) {
      byte[] valueBytes = entry.getBytes(UTF_8);
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Helper method to deserialize dictionary map.
   */
  protected String[] deserializeStringDictionary(ByteBuffer buffer)
      throws IOException {
    int dictionarySize = buffer.getInt();
    String[] stringDictionary = new String[dictionarySize];
    for (int i = 0; i < dictionarySize; i++) {
      stringDictionary[i] = DataTableUtils.decodeString(buffer);
    }
    return stringDictionary;
  }

  @Override
  public void addException(ProcessingException processingException) {
    _errCodeToExceptionMap.put(processingException.getErrorCode(), processingException.getMessage());
  }

  @Override
  public void addException(int errCode, String errMsg) {
    _errCodeToExceptionMap.put(errCode, errMsg);
  }

  @Override
  public Map<Integer, String> getExceptions() {
    return _errCodeToExceptionMap;
  }

  @Override
  public byte[] toBytes()
      throws IOException {
    ThreadResourceUsageProvider threadTimer = new ThreadResourceUsageProvider();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    writeLeadingSections(dataOutputStream);

    // Add table serialization time metadata if thread timer is enabled.
    if (ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled()) {
      long responseSerializationCpuTimeNs = threadTimer.getThreadTimeNs();
      getMetadata().put(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName(), String.valueOf(responseSerializationCpuTimeNs));
    }

    // Write metadata: length followed by actual metadata bytes.
    // NOTE: We ignore metadata serialization time in "responseSerializationCpuTimeNs" as it's negligible while
    // considering it will bring a lot code complexity.
    byte[] metadataBytes = serializeMetadata();
    dataOutputStream.writeInt(metadataBytes.length);
    dataOutputStream.write(metadataBytes);

    return byteArrayOutputStream.toByteArray();
  }

  private void writeLeadingSections(DataOutputStream dataOutputStream)
      throws IOException {
    dataOutputStream.writeInt(getVersion());
    dataOutputStream.writeInt(_numRows);
    dataOutputStream.writeInt(_numColumns);
    int dataOffset = HEADER_SIZE;

    // Write exceptions section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    byte[] exceptionsBytes;
    exceptionsBytes = serializeExceptions();
    dataOutputStream.writeInt(exceptionsBytes.length);
    dataOffset += exceptionsBytes.length;

    // Write dictionary map section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    byte[] dictionaryBytes = null;
    if (_stringDictionary != null) {
      dictionaryBytes = serializeStringDictionary();
      dataOutputStream.writeInt(dictionaryBytes.length);
      dataOffset += dictionaryBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write data schema section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    byte[] dataSchemaBytes = null;
    if (_dataSchema != null) {
      dataSchemaBytes = _dataSchema.toBytes();
      dataOutputStream.writeInt(dataSchemaBytes.length);
      dataOffset += dataSchemaBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write fixed size data section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    if (_fixedSizeDataBytes != null) {
      dataOutputStream.writeInt(_fixedSizeDataBytes.length);
      dataOffset += _fixedSizeDataBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write variable size data section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    if (_variableSizeDataBytes != null) {
      dataOutputStream.writeInt(_variableSizeDataBytes.length);
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write actual data.
    // Write exceptions bytes.
    dataOutputStream.write(exceptionsBytes);
    // Write dictionary map bytes.
    if (dictionaryBytes != null) {
      dataOutputStream.write(dictionaryBytes);
    }
    // Write data schema bytes.
    if (dataSchemaBytes != null) {
      dataOutputStream.write(dataSchemaBytes);
    }
    // Write fixed size data bytes.
    if (_fixedSizeDataBytes != null) {
      dataOutputStream.write(_fixedSizeDataBytes);
    }
    // Write variable size data bytes.
    if (_variableSizeDataBytes != null) {
      dataOutputStream.write(_variableSizeDataBytes);
    }
  }

  /**
   * Serialize metadata section to bytes.
   * Format of the bytes looks like:
   * [numEntries, bytesOfKV2, bytesOfKV2, bytesOfKV3]
   * For each KV pair:
   * - if the value type is String, encode it as: [enumKeyOrdinal, valueLength, Utf8EncodedValue].
   * - if the value type is int, encode it as: [enumKeyOrdinal, bigEndianRepresentationOfIntValue]
   * - if the value type is long, encode it as: [enumKeyOrdinal, bigEndianRepresentationOfLongValue]
   *
   * Unlike V2, where numeric metadata values (int and long) in V3 are encoded in UTF-8 in the wire format,
   * in V3 big endian representation is used.
   */
  private byte[] serializeMetadata()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_metadata.size());

    for (Map.Entry<String, String> entry : _metadata.entrySet()) {
      MetadataKey key = MetadataKey.getByName(entry.getKey());
      // Ignore unknown keys.
      if (key == null) {
        continue;
      }
      String value = entry.getValue();
      dataOutputStream.writeInt(key.getId());
      if (key.getValueType() == MetadataValueType.INT) {
        dataOutputStream.write(Ints.toByteArray(Integer.parseInt(value)));
      } else if (key.getValueType() == MetadataValueType.LONG) {
        dataOutputStream.write(Longs.toByteArray(Long.parseLong(value)));
      } else {
        byte[] valueBytes = value.getBytes(UTF_8);
        dataOutputStream.writeInt(valueBytes.length);
        dataOutputStream.write(valueBytes);
      }
    }

    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Even though the wire format of V3 uses UTF-8 for string/bytes and big-endian for numeric values,
   * the in-memory representation is STRING based for processing the metadata before serialization
   * (by the server as it adds the statistics in metadata) and after deserialization (by the broker as it receives
   * DataTable from each server and aggregates the values).
   * This is to make V3 implementation keep the consumers of Map<String, String> getMetadata() API in the code happy
   * by internally converting it.
   *
   * This method use relative operations on the ByteBuffer and expects the buffer's position to be set correctly.
   */
  private Map<String, String> deserializeMetadata(ByteBuffer buffer)
      throws IOException {
    int numEntries = buffer.getInt();
    Map<String, String> metadata = new HashMap<>();
    for (int i = 0; i < numEntries; i++) {
      int keyId = buffer.getInt();
      MetadataKey key = MetadataKey.getById(keyId);
      // Ignore unknown keys.
      if (key == null) {
        continue;
      }
      if (key.getValueType() == MetadataValueType.INT) {
        String value = "" + buffer.getInt();
        metadata.put(key.getName(), value);
      } else if (key.getValueType() == MetadataValueType.LONG) {
        String value = "" + buffer.getLong();
        metadata.put(key.getName(), value);
      } else {
        String value = DataTableUtils.decodeString(buffer);
        metadata.put(key.getName(), value);
      }
    }
    return metadata;
  }

  private byte[] serializeExceptions()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_errCodeToExceptionMap.size());

    for (Map.Entry<Integer, String> entry : _errCodeToExceptionMap.entrySet()) {
      int key = entry.getKey();
      String value = entry.getValue();
      byte[] valueBytes = value.getBytes(UTF_8);
      dataOutputStream.writeInt(key);
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  private Map<Integer, String> deserializeExceptions(ByteBuffer buffer)
      throws IOException {
    int numExceptions = buffer.getInt();
    Map<Integer, String> exceptions = new HashMap<>(HashUtil.getHashMapCapacity(numExceptions));
    for (int i = 0; i < numExceptions; i++) {
      int errCode = buffer.getInt();
      String errMessage = DataTableUtils.decodeString(buffer);
      exceptions.put(errCode, errMessage);
    }
    return exceptions;
  }

  protected void computeBlockObjectConstants() {
    if (_dataSchema != null) {
      _columnOffsets = new int[_numColumns];
      _rowSizeInBytes = DataBlockUtils.computeColumnOffsets(_dataSchema, _columnOffsets);
      _fixDataSize = _numRows * _rowSizeInBytes;
    }
  }

  /**
   * return the offset in {@code _fixedSizeDataBytes} of the row/column ID.
   * @param rowId row ID
   * @param colId column ID
   * @return the offset in the fixed size buffer for the row/columnID.
   */
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    return rowId * _rowSizeInBytes + _columnOffsets[colId];
  }

  /**
   * position the {@code _variableSizeDataBytes} to the corresponding row/column ID. and return the
   * length of bytes to extract from the variable size buffer.
   *
   * @param rowId row ID
   * @param colId column ID
   * @return the length to extract from variable size buffer.
   */
  protected int positionOffsetInVariableBufferAndGetLength(int rowId, int colId) {
    int offset = getOffsetInFixedBuffer(rowId, colId);
    _variableSizeData.position(_fixedSizeData.getInt(offset));
    return _fixedSizeData.getInt(offset + 4);
  }

  @Override
  public DataTable toMetadataOnlyDataTable() {
    DataTableImplV4 metadataOnlyDataTable = new DataTableImplV4();
    metadataOnlyDataTable._metadata.putAll(_metadata);
    metadataOnlyDataTable._errCodeToExceptionMap.putAll(_errCodeToExceptionMap);
    return metadataOnlyDataTable;
  }

  @Override
  public DataTable toDataOnlyDataTable() {
    return new DataTableImplV4(_numRows, _dataSchema, _stringDictionary, _fixedSizeDataBytes, _variableSizeDataBytes);
  }

  public int getRowSizeInBytes() {
    return _rowSizeInBytes;
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
