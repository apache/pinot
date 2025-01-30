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
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;


public final class TransferableBlockUtils {
  private static final int MEDIAN_COLUMN_SIZE_BYTES = 8;
  private static final TransferableBlock EMPTY_EOS = new TransferableBlock(MetadataBlock.newEos());

  private TransferableBlockUtils() {
    // do not instantiate.
  }

  public static TransferableBlock getEndOfStreamTransferableBlock() {
    return EMPTY_EOS;
  }

  public static TransferableBlock getEndOfStreamTransferableBlock(MultiStageQueryStats stats) {
    return new TransferableBlock(stats);
  }

  public static TransferableBlock wrap(DataBlock dataBlock) {
    return new TransferableBlock(dataBlock);
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
   *  Split a block into multiple block so that each block size is within maxBlockSize. Currently,
   *  <ul>
   *    <li>For row data block, we split for row type dataBlock.</li>
   *    <li>For columnar data block, exceptions are thrown.</li>
   *    <li>For metadata block, split is not supported.</li>
   *  </ul>
   *
   * @param block the data block
   * @param maxBlockSize Each chunk of data is estimated to be less than maxBlockSize
   * @return a list of data block chunks
   */
  public static Iterator<TransferableBlock> splitBlock(TransferableBlock block, int maxBlockSize) {
    DataBlock.Type type = block.getType();
    if (type == DataBlock.Type.ROW) {
      // Use estimated row size, this estimate is not accurate and is used to estimate numRowsPerChunk only.
      DataSchema dataSchema = block.getDataSchema();
      assert dataSchema != null;
      int estimatedRowSizeInBytes = dataSchema.getColumnNames().length * MEDIAN_COLUMN_SIZE_BYTES;
      int numRowsPerChunk = maxBlockSize / estimatedRowSizeInBytes;
      Preconditions.checkState(numRowsPerChunk > 0, "row size too large for query engine to handle, abort!");

      List<Object[]> rows = block.getContainer();
      int numRows = rows.size();
      int numChunks = (numRows + numRowsPerChunk - 1) / numRowsPerChunk;
      if (numChunks == 1) {
        return Iterators.singletonIterator(block);
      }
      List<TransferableBlock> blockChunks = new ArrayList<>(numChunks);
      for (int fromIndex = 0; fromIndex < numRows; fromIndex += numRowsPerChunk) {
        int toIndex = Math.min(fromIndex + numRowsPerChunk, numRows);
        blockChunks.add(new TransferableBlock(rows.subList(fromIndex, toIndex), dataSchema, DataBlock.Type.ROW));
      }
      return blockChunks.iterator();
    } else {
      return Iterators.singletonIterator(block);
    }
  }
}
