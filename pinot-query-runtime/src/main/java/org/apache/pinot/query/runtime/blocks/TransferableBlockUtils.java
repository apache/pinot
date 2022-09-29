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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;


public final class TransferableBlockUtils {
  private TransferableBlockUtils() {
    // do not instantiate.
  }

  public static TransferableBlock getEndOfStreamTransferableBlock(DataSchema dataSchema) {
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(dataSchema));
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

  /**
   *  Split a block into multiple block so that each block size is within maxBlockSize.
   *  Currently, we only support split for row type dataBlock.
   *  For columnar data block, we return the original data block.
   *  Metadata data block split is not supported.
   *
   *  When row size is greater than maxBlockSize, we pack each row as a separate block.
   */
  public static List<BaseDataBlock> getDataBlockChunks(BaseDataBlock block, BaseDataBlock.Type type, int maxBlockSize)
      throws IOException {
    List<BaseDataBlock> blockChunks = new ArrayList<>();
    switch (type) {
      // TODO: Add support for columnar data block.
      case COLUMNAR:
        blockChunks.add(block);
        return blockChunks;
      case METADATA:
        throw new UnsupportedOperationException("splitBlock is not supported for metdata block.");
      case ROW:
        break;
      default:
        throw new UnsupportedOperationException("splitBlock is not supported for type:" + type);
    }
    // TODO: Store row size in bytes inside data block.
    // TODO: Calculate dictionary and var bytes size as well.
    DataSchema dataSchema = block.getDataSchema();
    int[] columnOffsets = new int[dataSchema.size()];
    int rowSizeInBytes = DataBlockUtils.computeColumnOffsets(dataSchema, columnOffsets);
    List<Object[]> chunk = new ArrayList<>();
    int numRowsPerChunk = maxBlockSize / rowSizeInBytes;
    long numRows = 0;
    List<Object[]> container = DataBlockUtils.extractRows(block);
    for (Object[] unit : container) {
      // When rowSizeInBytes is greater than maxBlockSize, send one row per block.
      if (numRowsPerChunk == 0) {
        chunk.add(unit);
        blockChunks.add(DataBlockBuilder.buildFromRows(chunk, dataSchema));
        chunk = new ArrayList<>();
        continue;
      }
      if (numRows < numRowsPerChunk) {
        chunk.add(unit);
        ++numRows;
      }
      if (numRows == numRowsPerChunk) {
        blockChunks.add(DataBlockBuilder.buildFromRows(chunk, dataSchema));
        numRows = 0;
        chunk = new ArrayList<>();
      }
    }
    // Send last chunk.
    if (numRows > 0) {
      blockChunks.add(DataBlockBuilder.buildFromRows(chunk, dataSchema));
    }
    return blockChunks;
  }
}
