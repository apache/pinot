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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class DataBlockUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockUtils.class);

  private DataBlockUtils() {
  }

  static final int VERSION_TYPE_SHIFT = 5;

  public static MetadataBlock getErrorDataBlock(Exception e) {
    LOGGER.info("Caught exception while processing query", e);
    if (e instanceof ProcessingException) {
      return getErrorDataBlock(Collections.singletonMap(((ProcessingException) e).getErrorCode(), extractErrorMsg(e)));
    } else {
      // TODO: Pass in meaningful error code.
      return getErrorDataBlock(Collections.singletonMap(QueryException.UNKNOWN_ERROR_CODE, extractErrorMsg(e)));
    }
  }

  private static String extractErrorMsg(Throwable t) {
    while (t.getCause() != null && t.getMessage() == null) {
      t = t.getCause();
    }
    return t.getMessage() + "\n" + QueryException.getTruncatedStackTrace(t);
  }

  public static MetadataBlock getErrorDataBlock(Map<Integer, String> exceptions) {
    return MetadataBlock.newError(exceptions);
  }

  /**
   * Reads an integer from the given byte buffer.
   * <p>
   * The returned integer contains both the version and the type of the data block.
   * {@link #getVersion(int)} and {@link #getType(int)} can be used to extract the version and the type.
   * @param byteBuffer byte buffer to read from. A single int will be read
   */
  public static int readVersionType(ByteBuffer byteBuffer) {
    return byteBuffer.getInt();
  }

  public static int getVersion(int versionType) {
    return versionType & ((1 << VERSION_TYPE_SHIFT) - 1);
  }

  public static DataBlock.Type getType(int versionType) {
    return DataBlock.Type.fromOrdinal(versionType >> VERSION_TYPE_SHIFT);
  }

  public static DataBlock getDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    int versionType = readVersionType(byteBuffer);
    int version = getVersion(versionType);
    DataBlock.Type type = getType(versionType);
    switch (type) {
      case COLUMNAR:
        return new ColumnarDataBlock(byteBuffer);
      case ROW:
        return new RowDataBlock(byteBuffer);
      case METADATA:
        return MetadataBlock.deserialize(byteBuffer, version);
      default:
        throw new UnsupportedOperationException("Unsupported data table version: " + version + " with type: " + type);
    }
  }

  /**
   * Given a {@link DataSchema}, compute each column's offset and fill them into the passed in array, then return the
   * row size in bytes.
   *
   * @param dataSchema data schema.
   * @param columnOffsets array of column offsets.
   * @return row size in bytes.
   */
  public static int computeColumnOffsets(DataSchema dataSchema, int[] columnOffsets) {
    int numColumns = columnOffsets.length;
    assert numColumns == dataSchema.size();

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (storedColumnDataTypes[i]) {
        case INT:
          rowSizeInBytes += 4;
          break;
        case LONG:
          rowSizeInBytes += 8;
          break;
        case FLOAT:
          rowSizeInBytes += 4;
          break;
        case DOUBLE:
          rowSizeInBytes += 8;
          break;
        case STRING:
          rowSizeInBytes += 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          rowSizeInBytes += 8;
          break;
      }
    }

    return rowSizeInBytes;
  }

  /**
   * Given a {@link DataSchema}, compute each column's size and fill them into the passed in array.
   *
   * @param dataSchema data schema.
   * @param columnSizes array of column size.
   * @return row size in bytes.
   */
  public static void computeColumnSizeInBytes(DataSchema dataSchema, int[] columnSizes) {
    int numColumns = columnSizes.length;
    assert numColumns == dataSchema.size();

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    for (int i = 0; i < numColumns; i++) {
      switch (storedColumnDataTypes[i]) {
        case INT:
          columnSizes[i] = 4;
          break;
        case LONG:
          columnSizes[i] = 8;
          break;
        case FLOAT:
          columnSizes[i] = 4;
          break;
        case DOUBLE:
          columnSizes[i] = 8;
          break;
        case STRING:
          columnSizes[i] = 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          columnSizes[i] = 8;
          break;
      }
    }
  }
}
