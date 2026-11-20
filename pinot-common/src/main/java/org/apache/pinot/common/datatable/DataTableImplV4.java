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
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.accounting.ThreadResourceSnapshot;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/// Datatable V4 implementation.
///
/// The layout of serialized V4 datatable looks like:
/// +-----------------------------------------------+
/// | 13 integers of header:                        |
/// | VERSION                                       |
/// | NUM_ROWS                                      |
/// | NUM_COLUMNS                                   |
/// | EXCEPTIONS SECTION START OFFSET               |
/// | EXCEPTIONS SECTION LENGTH                     |
/// | DICTIONARY_MAP SECTION START OFFSET           |
/// | DICTIONARY_MAP SECTION LENGTH                 |
/// | DATA_SCHEMA SECTION START OFFSET              |
/// | DATA_SCHEMA SECTION LENGTH                    |
/// | FIXED_SIZE_DATA SECTION START OFFSET          |
/// | FIXED_SIZE_DATA SECTION LENGTH                |
/// | VARIABLE_SIZE_DATA SECTION START OFFSET       |
/// | VARIABLE_SIZE_DATA SECTION LENGTH             |
/// +-----------------------------------------------+
/// | EXCEPTIONS SECTION                            |
/// +-----------------------------------------------+
/// | DICTIONARY_MAP SECTION                        |
/// +-----------------------------------------------+
/// | DATA_SCHEMA SECTION                           |
/// +-----------------------------------------------+
/// | FIXED_SIZE_DATA SECTION                       |
/// +-----------------------------------------------+
/// | VARIABLE_SIZE_DATA SECTION                    |
/// +-----------------------------------------------+
/// | METADATA LENGTH                               |
/// | METADATA SECTION                              |
/// +-----------------------------------------------+
public class DataTableImplV4 implements DataTable {

  protected static final int HEADER_SIZE = Integer.BYTES * 13;
  // _errCodeToExceptionMap stores exceptions as a map of errorCode->errorMessage
  protected Map<Integer, String> _errCodeToExceptionMap;

  protected int _numRows;
  protected int _numColumns;
  protected int _fixDataSize;
  protected DataSchema _dataSchema;
  protected String[] _stringDictionary;
  /// Both sections are read with absolute offsets only, so they can be split in pages (as the server builds them) or
  /// be a single array (as the broker deserializes them).
  @Nullable
  protected DataBuffer _fixedSizeData;
  @Nullable
  protected DataBuffer _variableSizeData;
  protected Map<String, String> _metadata;
  protected int[] _columnOffsets;
  protected int _rowSizeInBytes;

  public DataTableImplV4() {
    _numRows = 0;
    _numColumns = 0;
    _fixDataSize = 0;
    _dataSchema = null;
    _stringDictionary = null;
    _fixedSizeData = null;
    _variableSizeData = null;
    _rowSizeInBytes = 0;
    _columnOffsets = null;
    _metadata = new HashMap<>();
    _errCodeToExceptionMap = new HashMap<>();
  }

  /// Creates a table that reads its fixed and variable size sections from the given big-endian buffers, without
  /// copying them.
  public DataTableImplV4(int numRows, DataSchema dataSchema, String[] stringDictionary, DataBuffer fixedSizeData,
      DataBuffer variableSizeData) {
    _numRows = numRows;
    _dataSchema = dataSchema;
    _numColumns = dataSchema == null ? 0 : dataSchema.size();
    _fixDataSize = 0;
    _stringDictionary = stringDictionary;
    _fixedSizeData = fixedSizeData;
    _variableSizeData = variableSizeData;
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
      byte[] fixedSizeDataBytes = new byte[fixedSizeDataLength];
      byteBuffer.position(fixedSizeDataStart);
      byteBuffer.get(fixedSizeDataBytes);
      _fixedSizeData = PinotByteBuffer.wrap(fixedSizeDataBytes);
    } else {
      _fixedSizeData = null;
    }

    // Read variable size data.
    byte[] variableSizeDataBytes = new byte[variableSizeDataLength];
    if (variableSizeDataLength != 0) {
      byteBuffer.position(variableSizeDataStart);
      byteBuffer.get(variableSizeDataBytes);
    }
    _variableSizeData = PinotByteBuffer.wrap(variableSizeDataBytes);

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
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    return BigDecimalUtils.deserialize(copyFromVariableBuffer(offsetInFixed));
  }

  @Override
  public String getString(int rowId, int colId) {
    return _stringDictionary[_fixedSizeData.getInt(getOffsetInFixedBuffer(rowId, colId))];
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    return new ByteArray(copyFromVariableBuffer(offsetInFixed));
  }

  // --------------------------------------------------------------------------
  // Variable sized element access.
  // --------------------------------------------------------------------------
  // These read with absolute offsets instead of a stream: a stream over a paged buffer creates a view of every page
  // up to the end of the section on each call.

  @Override
  public int[] getIntArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    int[] ints = new int[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = _variableSizeData.getInt(offsetInVar + (long) i * Integer.BYTES);
    }
    return ints;
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    long[] longs = new long[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = _variableSizeData.getLong(offsetInVar + (long) i * Long.BYTES);
    }
    return longs;
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    float[] floats = new float[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < floats.length; i++) {
      floats[i] = _variableSizeData.getFloat(offsetInVar + (long) i * Float.BYTES);
    }
    return floats;
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    double[] doubles = new double[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = _variableSizeData.getDouble(offsetInVar + (long) i * Double.BYTES);
    }
    return doubles;
  }

  @Override
  public BigDecimal[] getBigDecimalArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    BigDecimal[] bigDecimals = new BigDecimal[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < bigDecimals.length; i++) {
      byte[] bytes = new byte[_variableSizeData.getInt(offsetInVar)];
      offsetInVar += Integer.BYTES;
      _variableSizeData.copyTo(offsetInVar, bytes, 0, bytes.length);
      offsetInVar += bytes.length;
      bigDecimals[i] = BigDecimalUtils.deserialize(bytes);
    }
    return bigDecimals;
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    String[] strings = new String[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < strings.length; i++) {
      strings[i] = _stringDictionary[_variableSizeData.getInt(offsetInVar + (long) i * Integer.BYTES)];
    }
    return strings;
  }

  @Override
  public ByteArray[] getBytesArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    long offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    ByteArray[] bytes = new ByteArray[_fixedSizeData.getInt(offsetInFixed + Integer.BYTES)];
    for (int i = 0; i < bytes.length; i++) {
      byte[] value = new byte[_variableSizeData.getInt(offsetInVar)];
      offsetInVar += Integer.BYTES;
      _variableSizeData.copyTo(offsetInVar, value, 0, value.length);
      offsetInVar += value.length;
      bytes[i] = new ByteArray(value);
    }
    return bytes;
  }

  @Nullable
  @Override
  public Map<String, Object> getMap(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    if (_fixedSizeData.getInt(offsetInFixed + Integer.BYTES) == 0) {
      return null;
    }
    return MapUtils.deserializeMap(copyFromVariableBuffer(offsetInFixed));
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    int size = _fixedSizeData.getInt(offsetInFixed + Integer.BYTES);
    int type = _variableSizeData.getInt(offsetInVar);
    if (size == 0) {
      assert type == CustomObject.NULL_TYPE_VALUE;
      return null;
    }
    // A view when the value fits in one page, otherwise a copy. On a table built in this process the pages are
    // read-only, so the view is read-only too. See DataTable.getCustomObject for the contract.
    return new CustomObject(type, _variableSizeData.copyOrView(offsetInVar + Integer.BYTES, size));
  }

  @Nullable
  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    // _fixedSizeData stores two ints per col's null bitmap: offset, and length.
    int position = _fixDataSize + colId * Integer.BYTES * 2;
    if (_fixedSizeData == null || position >= _fixedSizeData.size()) {
      return null;
    }
    int bytesLength = _fixedSizeData.getInt(position + Integer.BYTES);
    if (bytesLength > 0) {
      byte[] nullBitmapBytes = new byte[bytesLength];
      _variableSizeData.copyTo(_fixedSizeData.getInt(position), nullBitmapBytes, 0, bytesLength);
      return RoaringBitmapUtils.deserialize(nullBitmapBytes);
    } else {
      return null;
    }
  }

  // --------------------------------------------------------------------------
  // Ser/De and exception handling
  // --------------------------------------------------------------------------

  /// Helper method to serialize dictionary map.
  protected byte[] serializeStringDictionary()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_stringDictionary.length);
    int numEntriesAdded = 0;
    for (String entry : _stringDictionary) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numEntriesAdded++,
          "DataTableImplV4#serializeStringDictionary");
      byte[] valueBytes = entry.getBytes(UTF_8);
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  /// Helper method to deserialize dictionary map.
  protected String[] deserializeStringDictionary(ByteBuffer buffer)
      throws IOException {
    return DataTableUtils.decodeStringArray(buffer);
  }

  @Override
  public void addException(int errCode, String errMsg) {
    _errCodeToExceptionMap.put(errCode, errMsg);
  }

  @Override
  public Map<Integer, String> getExceptions() {
    return _errCodeToExceptionMap;
  }

  /// Serializes the table into one array of the exact final size. Prefer [#toDataBuffer()], which does not copy the
  /// fixed and variable size sections.
  @Override
  public byte[] toBytes()
      throws IOException {
    DataBuffer dataBuffer = toDataBuffer();
    byte[] bytes = new byte[Math.toIntExact(dataBuffer.size())];
    dataBuffer.copyTo(0, bytes);
    return bytes;
  }

  /// Serializes the table without copying the fixed and variable size sections. The header, the small sections and
  /// the metadata are new arrays, and the two data sections are the buffers of this table. On a table built in this
  /// process, they are the builder's pages, so no buffer is larger than one page plus these small arrays.
  @Override
  public DataBuffer toDataBuffer()
      throws IOException {
    ThreadResourceSnapshot resourceSnapshot = new ThreadResourceSnapshot();

    byte[] exceptionsBytes = serializeExceptions();
    byte[] dictionaryBytes = _stringDictionary != null ? serializeStringDictionary() : null;
    byte[] dataSchemaBytes = _dataSchema != null ? _dataSchema.toBytes() : null;
    // The header stores every section offset as an int.
    long sectionsLength = HEADER_SIZE + exceptionsBytes.length + lengthOf(dictionaryBytes) + lengthOf(dataSchemaBytes)
        + sizeOf(_fixedSizeData) + sizeOf(_variableSizeData);
    if (sectionsLength > Integer.MAX_VALUE) {
      throw new IllegalStateException("Serialized data table size " + sectionsLength + " exceeds the 2 GB limit");
    }
    ByteBuffer leadingSections = ByteBuffer.allocate(
        HEADER_SIZE + exceptionsBytes.length + lengthOf(dictionaryBytes) + lengthOf(dataSchemaBytes));
    writeLeadingSections(leadingSections, exceptionsBytes, dictionaryBytes, dataSchemaBytes);

    // Add table serialization time and memory metadata when the corresponding measurement is enabled.
    // When CPU time/memory usage is not collectable, we omit these values from the metadata.
    if (ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled()) {
      _metadata.put(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName(), String.valueOf(resourceSnapshot.getCpuTimeNs()));
    }
    if (ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
      _metadata.put(MetadataKey.RESPONSE_SER_MEM_ALLOCATED_BYTES.getName(),
          String.valueOf(resourceSnapshot.getAllocatedBytes()));
    }

    // Write metadata: length followed by actual metadata bytes.
    // NOTE: We ignore metadata serialization time in "responseSerializationCpuTimeNs" as it's negligible while
    // considering it will bring a lot code complexity.
    byte[] metadataBytes = serializeMetadata();
    // Readers parse the whole table as one buffer, so the total must fit in an int as well.
    long totalLength = sectionsLength + Integer.BYTES + metadataBytes.length;
    if (totalLength > Integer.MAX_VALUE) {
      throw new IllegalStateException("Serialized data table size " + totalLength + " exceeds the 2 GB limit");
    }
    ByteBuffer metadata = ByteBuffer.allocate(Integer.BYTES + metadataBytes.length);
    metadata.putInt(metadataBytes.length);
    metadata.put(metadataBytes);

    CompoundDataBuffer.Builder builder = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN, false);
    builder.addBuffer(leadingSections.flip());
    if (_fixedSizeData != null) {
      builder.addBuffer(_fixedSizeData);
    }
    if (_variableSizeData != null) {
      builder.addBuffer(_variableSizeData);
    }
    builder.addBuffer(metadata.flip());
    return builder.build();
  }

  private static int lengthOf(@Nullable byte[] bytes) {
    return bytes != null ? bytes.length : 0;
  }

  private static long sizeOf(@Nullable DataBuffer dataBuffer) {
    return dataBuffer != null ? dataBuffer.size() : 0;
  }

  /// Writes the header and the exceptions, dictionary and data schema sections. The fixed and variable size sections
  /// follow them in the serialized table.
  private void writeLeadingSections(ByteBuffer byteBuffer, byte[] exceptionsBytes, @Nullable byte[] dictionaryBytes,
      @Nullable byte[] dataSchemaBytes) {
    int fixedSizeDataLength = (int) sizeOf(_fixedSizeData);
    int variableSizeDataLength = (int) sizeOf(_variableSizeData);

    byteBuffer.putInt(getVersion());
    byteBuffer.putInt(_numRows);
    byteBuffer.putInt(_numColumns);
    int dataOffset = HEADER_SIZE;

    // Write exceptions section offset(START|SIZE).
    byteBuffer.putInt(dataOffset);
    byteBuffer.putInt(exceptionsBytes.length);
    dataOffset += exceptionsBytes.length;

    // Write dictionary map section offset(START|SIZE).
    byteBuffer.putInt(dataOffset);
    byteBuffer.putInt(lengthOf(dictionaryBytes));
    dataOffset += lengthOf(dictionaryBytes);

    // Write data schema section offset(START|SIZE).
    byteBuffer.putInt(dataOffset);
    byteBuffer.putInt(lengthOf(dataSchemaBytes));
    dataOffset += lengthOf(dataSchemaBytes);

    // Write fixed size data section offset(START|SIZE).
    byteBuffer.putInt(dataOffset);
    byteBuffer.putInt(fixedSizeDataLength);
    dataOffset += fixedSizeDataLength;

    // Write variable size data section offset(START|SIZE).
    byteBuffer.putInt(dataOffset);
    byteBuffer.putInt(variableSizeDataLength);

    // Write actual data.
    byteBuffer.put(exceptionsBytes);
    if (dictionaryBytes != null) {
      byteBuffer.put(dictionaryBytes);
    }
    if (dataSchemaBytes != null) {
      byteBuffer.put(dataSchemaBytes);
    }
  }

  /// Serialize metadata section to bytes.
  /// Format of the bytes looks like:
  /// \[numEntries, bytesOfKV2, bytesOfKV2, bytesOfKV3\]
  /// For each KV pair:
  /// - if the value type is String, encode it as: \[enumKeyOrdinal, valueLength, Utf8EncodedValue\].
  /// - if the value type is int, encode it as: \[enumKeyOrdinal, bigEndianRepresentationOfIntValue\]
  /// - if the value type is long, encode it as: \[enumKeyOrdinal, bigEndianRepresentationOfLongValue\]
  ///
  /// Unlike V2, where numeric metadata values (int and long) in V3 are encoded in UTF-8 in the wire format,
  /// in V3 big endian representation is used.
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
      } else if (key.getValueType() == MetadataValueType.BOOLEAN) {
        dataOutputStream.write(DataTableUtils.encodeBoolean(Boolean.parseBoolean(value)));
      } else {
        byte[] valueBytes = value.getBytes(UTF_8);
        dataOutputStream.writeInt(valueBytes.length);
        dataOutputStream.write(valueBytes);
      }
    }

    return byteArrayOutputStream.toByteArray();
  }

  /// Even though the wire format of V3 uses UTF-8 for string/bytes and big-endian for numeric values,
  /// the in-memory representation is STRING based for processing the metadata before serialization
  /// (by the server as it adds the statistics in metadata) and after deserialization (by the broker as it receives
  /// DataTable from each server and aggregates the values).
  /// This is to make V3 implementation keep the consumers of Map<String, String> getMetadata() API in the code happy
  /// by internally converting it.
  ///
  /// This method use relative operations on the ByteBuffer and expects the buffer's position to be set correctly.
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
      } else if (key.getValueType() == MetadataValueType.BOOLEAN) {
        String value = "" + DataTableUtils.decodeBoolean(buffer);
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

  /// Returns the offset in the fixed size buffer of the row/column ID.
  /// @param rowId row ID
  /// @param colId column ID
  /// @return the offset in the fixed size buffer for the row/columnID.
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    return rowId * _rowSizeInBytes + _columnOffsets[colId];
  }

  /// Copies the variable size value referenced by the (offset, length) pair at `offsetInFixed` into a new array.
  private byte[] copyFromVariableBuffer(int offsetInFixed) {
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);
    int length = _fixedSizeData.getInt(offsetInFixed + Integer.BYTES);
    byte[] bytes = new byte[length];
    _variableSizeData.copyTo(offsetInVar, bytes, 0, length);
    return bytes;
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
    return new DataTableImplV4(_numRows, _dataSchema, _stringDictionary, _fixedSizeData, _variableSizeData);
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
