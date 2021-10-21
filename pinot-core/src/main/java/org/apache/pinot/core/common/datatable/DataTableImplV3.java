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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.ThreadTimer;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Datatable V3 implementation.
 * The layout of serialized V3 datatable looks like:
 * +-----------------------------------------------+
 * | 13 integers of header:                        |
 * | VERSION                                       |
 * | NUM_ROWS                                      |
 * | NUM_COLUMNS                                   |
 * | EXCEPTIONS SECTION START OFFSET               |
 * | EXCEPTIONS SECTION LENGTH                     |
 * | DICTIONARY_MAP SECTION START OFFSET           |
 * | DICTIONARY_MAP SECTION LENGTH                 |
 * | DATA_SCHEMA SECTION START OFFSET              |
 * | DATA_SCHEMA SECTION LENGTH                    |
 * | FIXED_SIZE_DATA SECTION START OFFSET          |
 * | FIXED_SIZE_DATA SECTION LENGTH                |
 * | VARIABLE_SIZE_DATA SECTION START OFFSET       |
 * | VARIABLE_SIZE_DATA SECTION LENGTH             |
 * +-----------------------------------------------+
 * | EXCEPTIONS SECTION                            |
 * +-----------------------------------------------+
 * | DICTIONARY_MAP SECTION                        |
 * +-----------------------------------------------+
 * | DATA_SCHEMA SECTION                           |
 * +-----------------------------------------------+
 * | FIXED_SIZE_DATA SECTION                       |
 * +-----------------------------------------------+
 * | VARIABLE_SIZE_DATA SECTION                    |
 * +-----------------------------------------------+
 * | METADATA LENGTH                               |
 * | METADATA SECTION                              |
 * +-----------------------------------------------+
 */
public class DataTableImplV3 extends BaseDataTable {
  private static final int HEADER_SIZE = Integer.BYTES * 13;
  // _errCodeToExceptionMap stores exceptions as a map of errorCode->errorMessage
  private final Map<Integer, String> _errCodeToExceptionMap;

  /**
   * Construct data table with results. (Server side)
   */
  public DataTableImplV3(int numRows, DataSchema dataSchema, Map<String, Map<Integer, String>> dictionaryMap,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    super(numRows, dataSchema, dictionaryMap, fixedSizeDataBytes, variableSizeDataBytes);
    _errCodeToExceptionMap = new HashMap<>();
  }

  /**
   * Construct empty data table. (Server side)
   */
  public DataTableImplV3() {
    _errCodeToExceptionMap = new HashMap<>();
  }

  /**
   * Construct data table from byte array. (broker side)
   */
  public DataTableImplV3(ByteBuffer byteBuffer)
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
      byte[] exceptionsBytes = new byte[exceptionsLength];
      byteBuffer.position(exceptionsStart);
      byteBuffer.get(exceptionsBytes);
      _errCodeToExceptionMap = deserializeExceptions(exceptionsBytes);
    } else {
      _errCodeToExceptionMap = new HashMap<>();
    }

    // Read dictionary.
    if (dictionaryMapLength != 0) {
      byte[] dictionaryMapBytes = new byte[dictionaryMapLength];
      byteBuffer.position(dictionaryMapStart);
      byteBuffer.get(dictionaryMapBytes);
      _dictionaryMap = deserializeDictionaryMap(dictionaryMapBytes);
    } else {
      _dictionaryMap = null;
    }

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

    // Read metadata.
    int metadataLength = byteBuffer.getInt();
    if (metadataLength != 0) {
      byte[] metadataBytes = new byte[metadataLength];
      byteBuffer.get(metadataBytes);
      _metadata = deserializeMetadata(metadataBytes);
    }
  }

  @Override
  public void addException(ProcessingException processingException) {
    _errCodeToExceptionMap.put(processingException.getErrorCode(), processingException.getMessage());
  }

  @Override
  public Map<Integer, String> getExceptions() {
    return _errCodeToExceptionMap;
  }

  @Override
  public byte[] toBytes()
      throws IOException {
    ThreadTimer threadTimer = new ThreadTimer();
    threadTimer.start();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dataOutputStream.writeInt(DataTableBuilder.VERSION_3);
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
    byte[] dictionaryMapBytes = null;
    if (_dictionaryMap != null) {
      dictionaryMapBytes = serializeDictionaryMap();
      dataOutputStream.writeInt(dictionaryMapBytes.length);
      dataOffset += dictionaryMapBytes.length;
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
    if (dictionaryMapBytes != null) {
      dataOutputStream.write(dictionaryMapBytes);
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

    // Update the value of "threadCpuTimeNs" to account data table serialization time.
    long responseSerializationCpuTimeNs = threadTimer.stopAndGetThreadTimeNs();
    // TODO: currently log/emit a total thread cpu time for query execution time and data table serialization time.
    //  Figure out a way to log/emit separately. Probably via providing an API on the DataTable to get/set query
    //  context, which is supposed to be used at server side only.
    long threadCpuTimeNs = Long.parseLong(getMetadata().getOrDefault(MetadataKey.THREAD_CPU_TIME_NS.getName(), "0"))
        + responseSerializationCpuTimeNs;
    getMetadata().put(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(threadCpuTimeNs));

    // Write metadata: length followed by actual metadata bytes.
    byte[] metadataBytes = serializeMetadata();
    dataOutputStream.writeInt(metadataBytes.length);
    dataOutputStream.write(metadataBytes);

    return byteArrayOutputStream.toByteArray();
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
      dataOutputStream.writeInt(key.ordinal());
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
   */
  private Map<String, String> deserializeMetadata(byte[] bytes)
      throws IOException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      int numEntries = dataInputStream.readInt();
      Map<String, String> metadata = new HashMap<>();
      for (int i = 0; i < numEntries; i++) {
        int keyId = dataInputStream.readInt();
        MetadataKey key = MetadataKey.getByOrdinal(keyId);
        // Ignore unknown keys.
        if (key == null) {
          continue;
        }
        if (key.getValueType() == MetadataValueType.INT) {
          String value = String.valueOf(DataTableUtils.decodeInt(dataInputStream));
          metadata.put(key.getName(), value);
        } else if (key.getValueType() == MetadataValueType.LONG) {
          String value = String.valueOf(DataTableUtils.decodeLong(dataInputStream));
          metadata.put(key.getName(), value);
        } else {
          String value = String.valueOf(DataTableUtils.decodeString(dataInputStream));
          metadata.put(key.getName(), value);
        }
      }
      return metadata;
    }
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

  private Map<Integer, String> deserializeExceptions(byte[] bytes)
      throws IOException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      int numExceptions = dataInputStream.readInt();
      Map<Integer, String> exceptions = new HashMap<>(numExceptions);
      for (int i = 0; i < numExceptions; i++) {
        int errCode = dataInputStream.readInt();
        String errMessage = DataTableUtils.decodeString(dataInputStream);
        exceptions.put(errCode, errMessage);
      }
      return exceptions;
    }
  }
}
