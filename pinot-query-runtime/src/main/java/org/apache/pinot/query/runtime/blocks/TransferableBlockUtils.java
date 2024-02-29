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
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;


public final class TransferableBlockUtils {
  private static final int MEDIAN_COLUMN_SIZE_BYTES = 8;
  private static final TransferableBlock EMPTY_EOS = new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock());

  private TransferableBlockUtils() {
    // do not instantiate.
  }

  public static TransferableBlock getEndOfStreamTransferableBlock() {
    return EMPTY_EOS;
  }

  public static TransferableBlock getEndOfStreamTransferableBlock(Map<String, String> statsMap) {
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(statsMap));
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
   * @return a list of data block chunks
   */
  public static Iterator<TransferableBlock> splitBlock(TransferableBlock block, DataBlock.Type type, int maxBlockSize) {
    List<TransferableBlock> blockChunks = new ArrayList<>();
    if (type == DataBlock.Type.ROW) {
      // Use estimated row size, this estimate is not accurate and is used to estimate numRowsPerChunk only.
      int estimatedRowSizeInBytes = block.getDataSchema().getColumnNames().length * MEDIAN_COLUMN_SIZE_BYTES;
      int numRowsPerChunk = maxBlockSize / estimatedRowSizeInBytes;
      Preconditions.checkState(numRowsPerChunk > 0, "row size too large for query engine to handle, abort!");

      int totalNumRows = block.getNumRows();
      List<Object[]> allRows = block.getContainer();
      int currentRow = 0;
      while (currentRow < totalNumRows) {
        List<Object[]> chunk = allRows.subList(currentRow, Math.min(currentRow + numRowsPerChunk, allRows.size()));
        currentRow += numRowsPerChunk;
        blockChunks.add(new TransferableBlock(chunk, block.getDataSchema(), block.getType()));
      }
      return blockChunks.iterator();
    } else if (type == DataBlock.Type.METADATA) {
      return Iterators.singletonIterator(block);
    } else {
      throw new IllegalArgumentException("Unsupported data block type: " + type);
    }
  }
}
