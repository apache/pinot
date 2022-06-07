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
package org.apache.pinot.query.runtime.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;


public final class DataBlockUtils {
  protected static final int VERSION_TYPE_SHIFT = 5;
  private DataBlockUtils() {
    // do not instantiate.
  }

  private static final DataSchema EMPTY_SCHEMA = new DataSchema(new String[0], new DataSchema.ColumnDataType[0]);
  private static final MetadataBlock EOS_DATA_BLOCK = new MetadataBlock(EMPTY_SCHEMA);

  static {
    EOS_DATA_BLOCK._metadata.put(DataTable.MetadataKey.TABLE.getName(), "END_OF_STREAM");
  }
  private static final TransferableBlock EOS_TRANSFERABLE_BLOCK = new TransferableBlock(EOS_DATA_BLOCK);

  public static TransferableBlock getEndOfStreamTransferableBlock() {
    return EOS_TRANSFERABLE_BLOCK;
  }

  public static MetadataBlock getEndOfStreamDataBlock() {
    return EOS_DATA_BLOCK;
  }

  public static MetadataBlock getErrorDataBlock(Exception e) {
    MetadataBlock errorBlock = new MetadataBlock(EMPTY_SCHEMA);
    errorBlock._metadata.put(DataTable.MetadataKey.TABLE.getName(), "ERROR");
    if (e instanceof ProcessingException) {
      errorBlock.addException(((ProcessingException) e).getErrorCode(), e.getMessage());
    } else {
      errorBlock.addException(QueryException.UNKNOWN_ERROR_CODE, e.getMessage());
    }
    return errorBlock;
  }

  public static TransferableBlock getErrorTransferableBlock(Exception e) {
    return new TransferableBlock(getErrorDataBlock(e));
  }

  public static MetadataBlock getEmptyDataBlock(DataSchema dataSchema) {
    return dataSchema == null ? EOS_DATA_BLOCK : new MetadataBlock(dataSchema);
  }

  public static TransferableBlock getEmptyTransferableBlock(DataSchema dataSchema) {
    return new TransferableBlock(getEmptyDataBlock(dataSchema));
  }

  public static boolean isEndOfStream(TransferableBlock transferableBlock) {
    return transferableBlock.getType().equals(BaseDataBlock.Type.METADATA)
        && "END_OF_STREAM".equals(transferableBlock.getDataBlock().getMetadata()
            .get(DataTable.MetadataKey.TABLE.getName()));
  }

  public static BaseDataBlock getDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    int versionType = byteBuffer.getInt();
    int version = versionType & ((1 << VERSION_TYPE_SHIFT) - 1);
    BaseDataBlock.Type type = BaseDataBlock.Type.fromOrdinal(versionType >> VERSION_TYPE_SHIFT);
    switch (type) {
      case COLUMNAR:
        return new ColumnarDataBlock(byteBuffer);
      case ROW:
        return new RowDataBlock(byteBuffer);
      case METADATA:
        return new MetadataBlock(byteBuffer);
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

    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
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
        // TODO: fix float size (should be 4). For backward compatible, DON'T CHANGE for now.
        case FLOAT:
          rowSizeInBytes += 8;
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

    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    for (int i = 0; i < numColumns; i++) {
      switch (storedColumnDataTypes[i]) {
        case INT:
          columnSizes[i] = 4;
          break;
        case LONG:
          columnSizes[i] = 8;
          break;
        // TODO: fix float size (should be 4). For backward compatible, DON'T CHANGE for now.
        case FLOAT:
          columnSizes[i] = 8;
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
