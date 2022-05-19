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
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Datatable V4 implementation.
 * The layout of serialized V4 datatable looks like:
 * +-----------------------------------------------+
 * | 17 integers of header:                        |
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
 * | FIXED_SIZE_NULL_VECTOR SECTION START OFFSET   |
 * | FIXED_SIZE_NULL_VECTOR SECTION LENGTH         |
 * | VARIABLE_SIZE_NULL_VECTOR SECTION START OFFSET|
 * | VARIABLE_SIZE_NULL_VECTOR SECTION LENGTH      |
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
 * | FIXED_SIZE_NULL_VECTOR SECTION                |
 * +-----------------------------------------------+
 * | VARIABLE_SIZE_VECTOR SECTION SECTION          |
 * +-----------------------------------------------+
 * | METADATA LENGTH                               |
 * | METADATA SECTION                              |
 * +-----------------------------------------------+
 */
public class DataTableImplV4 extends DataTableImplV3 {
  private static final int HEADER_SIZE = Integer.BYTES * 17;
  // _errCodeToExceptionMap stores exceptions as a map of errorCode->errorMessage
  private final Map<Integer, String> _errCodeToExceptionMap;
  protected byte[] _fixedSizeNullVectorBytes;
  protected ByteBuffer _fixedSizeNullVectorData;
  protected byte[] _variableSizeNullVectorBytes;
  protected ByteBuffer _variableSizeNullVectorData;

  /**
   * Construct data table with results. (Server side)
   */
  public DataTableImplV4(int numRows, DataSchema dataSchema, Map<String, Map<Integer, String>> dictionaryMap,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes, byte[] fixedSizeNullVectorBytes,
      byte[] variableSizeNullVectorBytes) {
    super(numRows, dataSchema, dictionaryMap, fixedSizeDataBytes, variableSizeDataBytes);
    _errCodeToExceptionMap = new HashMap<>();
    _fixedSizeNullVectorBytes = fixedSizeNullVectorBytes;
    _fixedSizeNullVectorData = ByteBuffer.wrap(fixedSizeNullVectorBytes);
    _variableSizeNullVectorBytes = variableSizeNullVectorBytes;
    _variableSizeNullVectorData = ByteBuffer.wrap(variableSizeNullVectorBytes);
  }

  @Override
  public MutableRoaringBitmap getColumnNullBitmap(int colId) {
    // _fixedSizeNullVectorData stores two ints per col: offset, and length.
    _fixedSizeNullVectorData.position(colId * Integer.BYTES * 2);
    int offset = _fixedSizeNullVectorData.getInt();
    int bitmapLength = _fixedSizeNullVectorData.getInt();
    MutableRoaringBitmap mutableRoaringBitmap = new MutableRoaringBitmap();
    if (bitmapLength > 0) {
      _variableSizeNullVectorData.position(offset);
      for (int i = 0; i < bitmapLength; i++) {
        mutableRoaringBitmap.add(_variableSizeNullVectorData.getInt());
      }
    }
    return mutableRoaringBitmap;
  }

  /**
   * Construct empty data table. (Server side)
   */
  public DataTableImplV4() {
    _errCodeToExceptionMap = new HashMap<>();
  }

  /**
   * Construct data table from byte array. (broker side)
   */
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
    int fixedSizeNullVectorStart = byteBuffer.getInt();
    int fixedSizeNullVectorLength = byteBuffer.getInt();
    int variableSizeNullVectorStart = byteBuffer.getInt();
    int variableSizeNullVectorLength = byteBuffer.getInt();

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
      _dictionaryMap = deserializeDictionaryMap(byteBuffer);
    } else {
      _dictionaryMap = null;
    }

    // Read data schema.
    if (dataSchemaLength != 0) {
      byteBuffer.position(dataSchemaStart);
      _dataSchema = DataSchema.fromBytes(byteBuffer);
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

    // Read fixed size null vector data.
    if (fixedSizeNullVectorLength != 0) {
      _fixedSizeNullVectorBytes = new byte[fixedSizeNullVectorLength];
      byteBuffer.position(fixedSizeNullVectorStart);
      byteBuffer.get(_fixedSizeNullVectorBytes);
      _fixedSizeNullVectorData = ByteBuffer.wrap(_fixedSizeNullVectorBytes);
    } else {
      _fixedSizeNullVectorBytes = null;
      _fixedSizeNullVectorData = null;
    }

    // Read variable size null vector data.
    if (variableSizeNullVectorLength != 0) {
      _variableSizeNullVectorBytes = new byte[variableSizeNullVectorLength];
      byteBuffer.position(variableSizeNullVectorStart);
      byteBuffer.get(_variableSizeNullVectorBytes);
      _variableSizeNullVectorData = ByteBuffer.wrap(_variableSizeNullVectorBytes);
    } else {
      _variableSizeNullVectorBytes = null;
      _variableSizeNullVectorData = null;
    }

    // Read metadata.
    int metadataLength = byteBuffer.getInt();
    if (metadataLength != 0) {
      _metadata = deserializeMetadata(byteBuffer);
    }
  }

  @Override
  public byte[] toBytes()
      throws IOException {
    ThreadTimer threadTimer = new ThreadTimer();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    writeLeadingSections(dataOutputStream);

    // Add table serialization time metadata if thread timer is enabled.
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
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

  @Override
  public DataTableImplV4 toMetadataOnlyDataTable() {
    DataTableImplV4 metadataOnlyDataTable = new DataTableImplV4();
    metadataOnlyDataTable._metadata.putAll(_metadata);
    metadataOnlyDataTable._errCodeToExceptionMap.putAll(_errCodeToExceptionMap);
    return metadataOnlyDataTable;
  }

  @Override
  public DataTableImplV4 toDataOnlyDataTable() {
    return new DataTableImplV4(_numRows, _dataSchema, _dictionaryMap, _fixedSizeDataBytes, _variableSizeDataBytes,
        _fixedSizeNullVectorBytes, _variableSizeNullVectorBytes);
  }

  private void writeLeadingSections(DataOutputStream dataOutputStream)
      throws IOException {
    dataOutputStream.writeInt(DataTableBuilder.VERSION_4);
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
      dataOffset += _variableSizeDataBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write fixed size null vector section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    if (_fixedSizeNullVectorBytes != null) {
      dataOutputStream.writeInt(_fixedSizeNullVectorBytes.length);
      dataOffset += _fixedSizeNullVectorBytes.length;
    } else {
      dataOutputStream.writeInt(0);
    }

    // Write variable size null vector section offset(START|SIZE).
    dataOutputStream.writeInt(dataOffset);
    if (_variableSizeNullVectorBytes != null) {
      dataOutputStream.writeInt(_variableSizeNullVectorBytes.length);
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
    // Write fixed size null vector bytes.
    if (_fixedSizeNullVectorBytes != null) {
      dataOutputStream.write(_fixedSizeNullVectorBytes);
    }
    // Write variable size null vector bytes.
    if (_variableSizeNullVectorBytes != null) {
      dataOutputStream.write(_variableSizeNullVectorBytes);
    }
  }
}
