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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;

import static java.nio.charset.StandardCharsets.UTF_8;


@Deprecated
public class DataTableImplV2 extends BaseDataTable {
  // VERSION
  // NUM_ROWS
  // NUM_COLUMNS
  // DICTIONARY_MAP (START|SIZE)
  // METADATA (START|SIZE)
  // DATA_SCHEMA (START|SIZE)
  // FIXED_SIZE_DATA (START|SIZE)
  // VARIABLE_SIZE_DATA (START|SIZE)
  private static final int HEADER_SIZE = Integer.BYTES * 13;

  /**
   * Construct data table with results. (Server side)
   */
  public DataTableImplV2(int numRows, DataSchema dataSchema, Map<String, Map<Integer, String>> dictionaryMap,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    super(numRows, dataSchema, dictionaryMap, fixedSizeDataBytes, variableSizeDataBytes);
  }

  /**
   * Construct empty data table. (Server side)
   */
  public DataTableImplV2() {
  }

  /**
   * Construct data table from byte array. (broker side)
   */
  public DataTableImplV2(ByteBuffer byteBuffer)
      throws IOException {
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
      byteBuffer.position(dictionaryMapStart);
      _dictionaryMap = deserializeDictionaryMap(byteBuffer);
    } else {
      _dictionaryMap = null;
    }

    // Read metadata.
    byteBuffer.position(metadataStart);
    _metadata = deserializeMetadata(byteBuffer);

    // Read data schema.
    if (dataSchemaLength != 0) {
      byteBuffer.position(dataSchemaStart);
      _dataSchema = DataSchema.fromBytes(byteBuffer);
      _columnOffsets = new int[_dataSchema.size()];
      _rowSizeInBytes = DataTableUtils.computeColumnOffsets(_dataSchema, _columnOffsets, getVersion());
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

  @Override
  public int getVersion() {
    return DataTableFactory.VERSION_2;
  }

  private Map<String, String> deserializeMetadata(ByteBuffer buffer)
      throws IOException {
    int numEntries = buffer.getInt();
    Map<String, String> metadata = new HashMap<>(numEntries);

    for (int i = 0; i < numEntries; i++) {
      String key = DataTableUtils.decodeString(buffer);
      String value = DataTableUtils.decodeString(buffer);
      metadata.put(key, value);
    }

    return metadata;
  }

  @Override
  public void addException(ProcessingException processingException) {
    _metadata.put(EXCEPTION_METADATA_KEY + processingException.getErrorCode(), processingException.getMessage());
  }

  @Override
  public void addException(int errCode, String errMsg) {
    _metadata.put(EXCEPTION_METADATA_KEY + errCode, errMsg);
  }

  // getExceptions return a map of errorCode->errMessage of the datatable.
  @Override
  public Map<Integer, String> getExceptions() {
    Map<Integer, String> exceptions = new HashMap<>();
    for (String key : _metadata.keySet()) {
      if (key.startsWith(EXCEPTION_METADATA_KEY)) {
        // In V2, all exceptions are added into metadata, using "Exception"+errCode as key,
        // Integer.parseInt(key.substring(9)) can extract the error code from the key.
        exceptions.put(Integer.parseInt(key.substring(9)), _metadata.get(key));
      }
    }
    return exceptions;
  }

  @Override
  public byte[] toBytes()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dataOutputStream.writeInt(DataTableFactory.VERSION_2);
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

  @Override
  public DataTableImplV2 toMetadataOnlyDataTable() {
    DataTableImplV2 metadataOnlyDataTable = new DataTableImplV2();
    metadataOnlyDataTable._metadata.putAll(_metadata);
    return metadataOnlyDataTable;
  }

  @Override
  public DataTableImplV2 toDataOnlyDataTable() {
    return new DataTableImplV2(_numRows, _dataSchema, _dictionaryMap, _fixedSizeDataBytes, _variableSizeDataBytes);
  }

  private byte[] serializeMetadata()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(_metadata.size());
    for (Entry<String, String> entry : _metadata.entrySet()) {
      byte[] keyBytes = entry.getKey().getBytes(UTF_8);
      dataOutputStream.writeInt(keyBytes.length);
      dataOutputStream.write(keyBytes);

      byte[] valueBytes = entry.getValue().getBytes(UTF_8);
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }
}
