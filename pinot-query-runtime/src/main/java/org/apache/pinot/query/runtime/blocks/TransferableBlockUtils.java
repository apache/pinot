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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;


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
  public static List<TransferableBlock> splitBlock(TransferableBlock block, BaseDataBlock.Type type, int maxBlockSize) {
    List<TransferableBlock> blockChunks = new ArrayList<>();
    if (type != BaseDataBlock.Type.ROW) {
      return Collections.singletonList(block);
    } else {
      int rowSizeInBytes = ((RowDataBlock) block.getDataBlock()).getRowSizeInBytes();
      int numRowsPerChunk = maxBlockSize / rowSizeInBytes;
      Preconditions.checkState(numRowsPerChunk > 0, "row size too large for query engine to handle, abort!");

      int totalNumRows = block.getNumRows();
      List<Object[]> allRows = block.getContainer();
      int currentRow = 0;
      while (currentRow < totalNumRows) {
        List<Object[]> chunk = allRows.subList(currentRow, Math.min(currentRow + numRowsPerChunk, allRows.size()));
        currentRow += numRowsPerChunk;
        blockChunks.add(new TransferableBlock(chunk, block.getDataSchema(), block.getType()));
      }
    }
    return blockChunks;
  }

  public static Object[] getRow(TransferableBlock transferableBlock, int rowId) {
    Preconditions.checkState(transferableBlock.getType() == BaseDataBlock.Type.ROW,
        "TransferableBlockUtils doesn't support get row from non-ROW-based data block type yet!");
    return transferableBlock.getContainer().get(rowId);
  }
}
