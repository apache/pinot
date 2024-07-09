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
package org.apache.pinot.common.datablock;

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTableImplV3;
import org.apache.pinot.common.datatable.DataTableUtils;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotInputStream;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Base data block mostly replicating implementation of {@link DataTableImplV3}.
 *
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
 *
 * To support both row and columnar data format. the size of the data payload will be exactly the same. the only
 * difference is the data layout in FIXED_SIZE_DATA and VARIABLE_SIZE_DATA section, see each impl for details.
 */
@SuppressWarnings("DuplicatedCode")
public abstract class BaseDataBlock implements DataBlock, DataBlock.Raw {
  protected static final int HEADER_SIZE = Integer.BYTES * 13;
  // _errCodeToExceptionMap stores exceptions as a map of errorCode->errorMessage
  protected Map<Integer, String> _errCodeToExceptionMap;

  protected int _numRows;
  protected int _numColumns;
  protected DataSchema _dataSchema;
  protected String[] _stringDictionary;
  protected DataBuffer _fixedSizeData;
  protected DataBuffer _variableSizeData;
  @Nullable
  private List<ByteBuffer> _serialized;

  /**
   * construct a base data block.
   * @param numRows num of rows in the block
   * @param dataSchema schema of the data in the block
   * @param stringDictionary dictionary encoding map
   * @param fixedSizeDataBytes byte[] for fix-sized columns.
   * @param variableSizeDataBytes byte[] for variable length columns (arrays).
   */
  public BaseDataBlock(int numRows, @Nullable DataSchema dataSchema, String[] stringDictionary,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    _numRows = numRows;
    _dataSchema = dataSchema;
    _numColumns = dataSchema == null ? 0 : dataSchema.size();
    _stringDictionary = stringDictionary;
    _fixedSizeData = PinotByteBuffer.wrap(ByteBuffer.wrap(fixedSizeDataBytes));
    _variableSizeData = PinotByteBuffer.wrap(ByteBuffer.wrap(variableSizeDataBytes));
    _errCodeToExceptionMap = new HashMap<>();
  }

  public BaseDataBlock(int numRows, DataSchema dataSchema, String[] stringDictionary,
      DataBuffer fixedSizeData, DataBuffer variableSizeData) {
    Preconditions.checkArgument(fixedSizeData.size() <= Integer.MAX_VALUE, "Fixed size data too large ({} bytes",
        fixedSizeData.size());
    Preconditions.checkArgument(variableSizeData.size() <= Integer.MAX_VALUE, "Variable size data too large ({} bytes",
        variableSizeData.size());
    _numRows = numRows;
    _dataSchema = dataSchema;
    _numColumns = dataSchema.size();
    _stringDictionary = stringDictionary;
    _fixedSizeData = fixedSizeData;
    _variableSizeData = variableSizeData;
    _errCodeToExceptionMap = new HashMap<>();
  }

  @Override
  public int getVersion() {
    return 0;
  }

  /**
   * Return the int serialized form of the data block version and type.
   * @return
   */
  protected abstract int getDataBlockVersionType();

  /**
   * return the offset in {@code _fixedSizeDataBytes} of the row/column ID.
   * @param rowId row ID
   * @param colId column ID
   * @return the offset in the fixed size buffer for the row/columnID.
   */
  protected abstract int getOffsetInFixedBuffer(int rowId, int colId);

  @Override
  public Map<String, String> getMetadata() {
    return Collections.emptyMap();
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
  public int getNumberOfColumns() {
    return _numColumns;
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
    // TODO: Add a allocation free deserialization mechanism.
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    byte[] buffer = new byte[size];
    _variableSizeData.copyTo(offsetInVar, buffer, 0, size);

    return BigDecimalUtils.deserialize(buffer);
  }

  @Override
  public String getString(int rowId, int colId) {
    return _stringDictionary[_fixedSizeData.getInt(getOffsetInFixedBuffer(rowId, colId))];
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    byte[] buffer = new byte[size];
    _variableSizeData.copyTo(offsetInVar, buffer, 0, size);
    return new ByteArray(buffer);
  }

  // --------------------------------------------------------------------------
  // Variable sized element access.
  // --------------------------------------------------------------------------

  @Override
  public int[] getIntArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    int[] ints = new int[size];
    try (PinotInputStream stream = _variableSizeData.openInputStream(offsetInVar)) {
      for (int i = 0; i < ints.length; i++) {
        ints[i] = stream.readInt();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return ints;
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    long[] longs = new long[size];
    try (PinotInputStream stream = _variableSizeData.openInputStream(offsetInVar)) {
      for (int i = 0; i < longs.length; i++) {
        longs[i] = stream.readLong();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return longs;
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    float[] floats = new float[size];
    try (PinotInputStream stream = _variableSizeData.openInputStream(offsetInVar)) {
      for (int i = 0; i < floats.length; i++) {
        floats[i] = stream.readFloat();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return floats;
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    double[] doubles = new double[size];
    try (PinotInputStream stream = _variableSizeData.openInputStream(offsetInVar)) {
      for (int i = 0; i < doubles.length; i++) {
        doubles[i] = stream.readDouble();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return doubles;
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    String[] strings = new String[size];
    try (PinotInputStream stream = _variableSizeData.openInputStream(offsetInVar)) {
      for (int i = 0; i < strings.length; i++) {
        strings[i] = _stringDictionary[stream.readInt()];
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return strings;
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    int offsetInFixed = getOffsetInFixedBuffer(rowId, colId);
    int size = _fixedSizeData.getInt(offsetInFixed + 4);
    int offsetInVar = _fixedSizeData.getInt(offsetInFixed);

    int type = _variableSizeData.getInt(offsetInVar);
    if (size == 0) {
      assert type == CustomObject.NULL_TYPE_VALUE;
      return null;
    }
    return new CustomObject(type, _variableSizeData.copyOrView(offsetInVar + Integer.BYTES, size));
  }

  @Nullable
  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    // _fixedSizeData stores two ints per col's null bitmap: offset, and length.
    int position = colId * Integer.BYTES * 2;
    if (_fixedSizeData == null || position >= _fixedSizeData.size()) {
      return null;
    }
    int offset = _fixedSizeData.getInt(position);
    int bytesLength = _fixedSizeData.getInt(position + Integer.BYTES);
    if (bytesLength > 0) {
      // TODO: This will allocate. Can we change code to use ImmutableRoaringBitmap?
      return _variableSizeData.viewAsRoaringBitmap(offset, bytesLength).toRoaringBitmap();
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
    if (_stringDictionary.length == 0) {
      return new byte[4];
    }
    UnsynchronizedByteArrayOutputStream byteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(1024);
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
  public List<ByteBuffer> serialize()
      throws IOException {
    if (_serialized == null) {
      _serialized = DataBlockUtils.serialize(this);
    }
    return _serialized;
  }

  @Override
  public String toString() {
    if (_dataSchema == null) {
      return "{}";
    } else {
      return "resultSchema:" + '\n' + _dataSchema + '\n' + "numRows: " + _numRows + '\n';
    }
  }

  @Nullable
  @Override
  public String[] getStringDictionary() {
    return _stringDictionary;
  }

  @Nullable
  @Override
  public DataBuffer getFixedData() {
    return _fixedSizeData;
  }

  @Nullable
  @Override
  public DataBuffer getVarSizeData() {
    return _variableSizeData;
  }

  /**
   * Returns the list of serialized stats.
   * <p>
   * The returned list may contain nulls, which would mean that no stats were available for that stage.
   * <p>
   * The list itself may also be null.
   */
  @Nullable
  @Override
  public List<DataBuffer> getStatsByStage() {
    return Collections.emptyList();
  }

  @Override
  public Raw asRaw() {
    return this;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataBlock)) {
      return false;
    }
    DataBlock that = (DataBlock) o;
    if (getDataBlockType() != that.getDataBlockType()) {
      return false;
    }
    switch (getDataBlockType()) {
      case ROW:
      case COLUMNAR:
        assert _dataSchema != null;
        if (!_dataSchema.equals(that.getDataSchema())) {
          return false;
        }
        if (_numRows != that.getNumberOfRows() || _numColumns != that.getNumberOfColumns()) {
          return false;
        }
        DataSchema.ColumnDataType[] colTypes = _dataSchema.getColumnDataTypes();
        String[] colNames = _dataSchema.getColumnNames();
        for (int colId = 0; colId < colNames.length; colId++) {
          switch (colTypes[colId]) {
            case INT:
            case BOOLEAN:
              for (int did = 0; did < _numRows; did++) {
                if (getInt(did, colId) != that.getInt(did, colId)) {
                  return false;
                }
              }
              break;
            case LONG:
              for (int did = 0; did < _numRows; did++) {
                if (getLong(did, colId) != that.getLong(did, colId)) {
                  return false;
                }
              }
              break;
            case FLOAT:
              for (int did = 0; did < _numRows; did++) {
                if (getFloat(did, colId) != that.getFloat(did, colId)) {
                  return false;
                }
              }
              break;
            case DOUBLE:
              for (int did = 0; did < _numRows; did++) {
                if (getDouble(did, colId) != that.getDouble(did, colId)) {
                  return false;
                }
              }
              break;
            case TIMESTAMP:
              break;
            case STRING:
            case JSON:
              for (int did = 0; did < _numRows; did++) {
                if (!getString(did, colId).equals(that.getString(did, colId))) {
                  return false;
                }
              }
              break;
            case BYTES:
              for (int did = 0; did < _numRows; did++) {
                if (!getBytes(did, colId).equals(that.getBytes(did, colId))) {
                  return false;
                }
              }
              break;
            case BIG_DECIMAL:
              for (int did = 0; did < _numRows; did++) {
                if (!getBigDecimal(did, colId).equals(that.getBigDecimal(did, colId))) {
                  return false;
                }
              }
              break;
            case OBJECT:
              for (int did = 0; did < _numRows; did++) {
                if (!Objects.equals(getCustomObject(did, colId), that.getCustomObject(did, colId))) {
                  return false;
                }
              }
              break;
            case INT_ARRAY:
              for (int did = 0; did < _numRows; did++) {
                if (!Arrays.equals(getIntArray(did, colId), that.getIntArray(did, colId))) {
                  return false;
                }
              }
              break;
            case LONG_ARRAY:
            case TIMESTAMP_ARRAY:
              for (int did = 0; did < _numRows; did++) {
                if (!Arrays.equals(getLongArray(did, colId), that.getLongArray(did, colId))) {
                  return false;
                }
              }
              break;
            case FLOAT_ARRAY:
              for (int did = 0; did < _numRows; did++) {
                if (!Arrays.equals(getFloatArray(did, colId), that.getFloatArray(did, colId))) {
                  return false;
                }
              }
              break;
            case DOUBLE_ARRAY:
              for (int did = 0; did < _numRows; did++) {
                if (!Arrays.equals(getDoubleArray(did, colId), that.getDoubleArray(did, colId))) {
                  return false;
                }
              }
              break;
            case STRING_ARRAY:
              for (int did = 0; did < _numRows; did++) {
                if (!Arrays.equals(getStringArray(did, colId), that.getStringArray(did, colId))) {
                  return false;
                }
              }
              break;
            case BYTES_ARRAY:
            case BOOLEAN_ARRAY:
            case UNKNOWN:
              throw new UnsupportedOperationException("Check how to read " + colTypes[colId] + " from data block");
            default:
              throw new UnsupportedOperationException("Unsupported column type: " + colTypes[colId]);
          }
        }
        return true;
      case METADATA: {
        return Objects.equals(_errCodeToExceptionMap, that.getExceptions())
            && Objects.equals(getStatsByStage(), that.asRaw().getStatsByStage());
      }
      default:
        throw new UnsupportedOperationException("Unsupported data block type: " + getDataBlockType());
    }
  }

  @Override
  public final int hashCode() {
    return Objects.hash(_errCodeToExceptionMap, _numRows, _dataSchema);
  }
}
