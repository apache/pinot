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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Interface for splitting MSE blocks. This is used for ensuring
 * that the blocks that are sent along the wire play nicely with the
 * underlying transport.
 */
public interface BlockSplitter {
  BlockSplitter NO_OP = (block, maxBlockSize) -> Iterators.singletonIterator(block);
  BlockSplitter DEFAULT = new Default();

  /**
   * @return a list of blocks that was split from the original {@code block}
   */
  Iterator<? extends MseBlock.Data> split(MseBlock.Data block, int maxBlockSize);

  /**
   *  Split a block into multiple block so that each block size is within maxBlockSize. Currently,
   *  <ul>
   *    <li>For row data block, we split for row type dataBlock.</li>
   *    <li>For columnar data block, exceptions are thrown.</li>
   *    <li>For metadata block, split is not supported.</li>
   *  </ul>
   *
   * @return a list of data block chunks
   */
  class Default implements BlockSplitter, MseBlock.Data.Visitor<Iterator<MseBlock.Data>, Integer> {
    private static final int MEDIAN_COLUMN_SIZE_BYTES = 8;

    private Default() {
    }

    @Override
    public Iterator<MseBlock.Data> split(MseBlock.Data block, int maxBlockSize) {
      return block.accept(this, maxBlockSize);
    }

    @Override
    public Iterator<MseBlock.Data> visit(RowHeapDataBlock block, Integer maxBlockSize) {
      // Use estimated row size, this estimate is not accurate and is used to estimate numRowsPerChunk only.
      DataSchema dataSchema = block.getDataSchema();
      assert dataSchema != null;
      int estimatedRowSizeInBytes = dataSchema.getColumnNames().length * MEDIAN_COLUMN_SIZE_BYTES;
      int numRowsPerChunk = maxBlockSize / estimatedRowSizeInBytes;
      Preconditions.checkState(numRowsPerChunk > 0, "row size too large for query engine to handle, abort!");

      List<Object[]> rows = block.getRows();
      int numRows = rows.size();
      int numChunks = (numRows + numRowsPerChunk - 1) / numRowsPerChunk;
      if (numChunks == 1) {
        return Iterators.singletonIterator(block);
      }
      List<MseBlock.Data> blockChunks = new ArrayList<>(numChunks);
      for (int fromIndex = 0; fromIndex < numRows; fromIndex += numRowsPerChunk) {
        int toIndex = Math.min(fromIndex + numRowsPerChunk, numRows);
        blockChunks.add(new RowHeapDataBlock(rows.subList(fromIndex, toIndex), dataSchema, block.getAggFunctions()));
      }
      return blockChunks.iterator();
    }

    @Override
    public Iterator<MseBlock.Data> visit(SerializedDataBlock block, Integer maxBlockSize
    ) {
      if (block.getDataBlock().getDataBlockType() == DataBlock.Type.ROW) {
        return visit(block.asRowHeap(), maxBlockSize);
      }
      return Iterators.singletonIterator(block);
    }
  }
}
