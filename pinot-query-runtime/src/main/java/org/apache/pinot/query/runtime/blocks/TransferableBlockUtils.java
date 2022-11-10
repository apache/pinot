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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;


public final class TransferableBlockUtils {
  private static final int MEDIAN_COLUMN_SIZE_BYTES = 8;

  private TransferableBlockUtils() {
    // do not instantiate.
  }

  public static TransferableBlock getEndOfStreamTransferableBlock() {
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock());
  }

  public static TransferableBlock getNoOpTransferableBlock() {
    return new TransferableBlock(DataBlockUtils.getNoOpBlock());
  }

  public static TransferableBlock getErrorTransferableBlock(Exception e) {
    return new TransferableBlock(DataBlockUtils.getErrorDataBlock(e));
  }

  public static TransferableBlock getErrorTransferableBlock(Map<Integer, String> exceptions) {
    return new TransferableBlock(DataBlockUtils.getErrorDataBlock(exceptions));
  }

  public static boolean isEndOfStream(TransferableBlock transferableBlock) {
    return transferableBlock.isEndOfStreamBlock();
  }

  public static boolean isNoOpBlock(TransferableBlock transferableBlock) {
    return transferableBlock.isNoOpBlock();
  }

  /**
   * Split block into multiple blocks. Default without any clean up.
   *
   * @see TransferableBlockUtils#splitBlock(TransferableBlock, DataBlock.Type, int, boolean)
   */
  public static List<TransferableBlock> splitBlock(TransferableBlock block, DataBlock.Type type, int maxBlockSize) {
    return splitBlock(block, type, maxBlockSize, false);
  }

  /**
   *
   *  Split a block into multiple block so that each block size is within maxBlockSize. Currently,
   *  <ul>
   *    <li>For row data block, we split for row type dataBlock.</li>
   *    <li>For columnar data block, exceptions are thrown.</li>
   *    <li>For metadata block, split is not supported.</li>
   *  </ul>
   *
   * @param block the data block
   * @param type type of block
   * @param maxBlockSize Each chunk of data is estimated to be less than maxBlockSize
   * @param needsCanonicalize whether we need to canonicalize the input rows. set to true if the block is constructed
   *                          from leaf stage.
   * @return a list of data block chunks
   */
  public static List<TransferableBlock> splitBlock(TransferableBlock block, DataBlock.Type type, int maxBlockSize,
      boolean needsCanonicalize) {
    List<TransferableBlock> blockChunks = new ArrayList<>();
    if (type == DataBlock.Type.ROW) {
      // Use estimated row size, this estimate is not accurate and is used to estimate numRowsPerChunk only.
      int estimatedRowSizeInBytes = block.getDataSchema().getColumnNames().length * MEDIAN_COLUMN_SIZE_BYTES;
      int numRowsPerChunk = maxBlockSize / estimatedRowSizeInBytes;
      Preconditions.checkState(numRowsPerChunk > 0, "row size too large for query engine to handle, abort!");

      Collection<Object[]> allRows = block.getContainer();
      DataSchema dataSchema = block.getDataSchema();
      int rowId = 0;
      List<Object[]> chunk = new ArrayList<>(numRowsPerChunk);
      for (Object[] row : allRows) {
        if (needsCanonicalize) {
          chunk.add(canonicalizeRow(row, dataSchema));
        } else {
          chunk.add(row);
        }
        rowId++;
        if (rowId % numRowsPerChunk == 0) {
          blockChunks.add(new TransferableBlock(chunk, block.getDataSchema(), block.getType()));
          chunk = new ArrayList<>();
        }
      }
      if (chunk.size() > 0) {
        blockChunks.add(new TransferableBlock(chunk, block.getDataSchema(), block.getType()));
      }
      return blockChunks;
    } else if (type == DataBlock.Type.METADATA) {
      return Collections.singletonList(block);
    } else {
      throw new IllegalArgumentException("Unsupported data block type: " + type);
    }
  }

  /**
   * This util is used to canonicalize row generated from V1 engine, which is stored using
   * {@link DataSchema#getStoredColumnDataTypes()} format. However, the transferable block ser/de stores data in the
   * {@link DataSchema#getColumnDataTypes()} format.
   *
   * @param row un-canonicalize row.
   * @param dataSchema data schema desired for the row.
   * @return canonicalize row.
   */
  private static Object[] canonicalizeRow(Object[] row, DataSchema dataSchema) {
    Object[] resultRow = new Object[row.length];
    for (int colId = 0; colId < row.length; colId++) {
      resultRow[colId] = dataSchema.getColumnDataType(colId).convert(row[colId]);
    }
    return resultRow;
  }
}
