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
package org.apache.pinot.query.runtime.operator.join;

import it.unimi.dsi.fastutil.HashCommon;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Single-threaded primitive-key hash table with compact build-row ordinals and insertion-ordered duplicate chains.
 * Borrows build blocks; their owner must keep them alive until probing finishes.
 */
public final class ArrowHashTable {
  private static final int MAX_CAPACITY = 1 << 30;

  private final long[] _keys;
  // Zero denotes an unoccupied slot, otherwise the value is a row ordinal plus one. No key value is reserved.
  private final int[] _heads;
  private final int[] _blockByRow;
  private final int[] _blockStarts;
  private final int _mask;
  private final boolean _keepDuplicates;
  private final Runnable _checkTermination;
  @Nullable
  private int[] _tails;
  @Nullable
  private int[] _next;

  public ArrowHashTable(List<ArrowBlock> blocks, int keyColumn, ColumnDataType keyType, int numRows,
      boolean keepDuplicates, Runnable checkTermination) {
    long requiredCapacity = Math.max(16, (numRows * 5L + 2) / 3);
    if (requiredCapacity > MAX_CAPACITY) {
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException("Arrow join hash table capacity exceeded");
    }
    int capacity = Integer.highestOneBit((int) requiredCapacity - 1) << 1;
    _keys = new long[capacity];
    _heads = new int[capacity];
    _mask = capacity - 1;
    _blockByRow = new int[numRows];
    _blockStarts = new int[blocks.size()];
    _keepDuplicates = keepDuplicates;
    _checkTermination = checkTermination;
    long[] keys = new long[ArrowJoinKeys.BATCH_SIZE];
    int ordinal = 0;
    for (int blockId = 0; blockId < blocks.size(); blockId++) {
      ArrowBlock block = blocks.get(blockId);
      int rows = block.getNumRows();
      _blockStarts[blockId] = ordinal;
      Arrays.fill(_blockByRow, ordinal, ordinal + rows, blockId);
      FieldVector keyVector = block.getDataBlock().getRoot().getVector(keyColumn);
      for (int start = 0; start < rows; start += ArrowJoinKeys.BATCH_SIZE) {
        checkTermination.run();
        int count = Math.min(ArrowJoinKeys.BATCH_SIZE, rows - start);
        ArrowJoinKeys.read(keyVector, keyType, start, count, keys);
        for (int i = 0; i < count; i++) {
          if (!keyVector.isNull(start + i)) {
            insert(keys[i], ordinal + start + i);
          }
        }
      }
      ordinal += rows;
    }
  }

  private void insert(long key, int row) {
    int slot = findSlot(key);
    if (_heads[slot] == 0) {
      _keys[slot] = key;
      _heads[slot] = row + 1;
      if (_tails != null) {
        _tails[slot] = row + 1;
      }
    } else if (_keepDuplicates) {
      if (_tails == null) {
        _tails = Arrays.copyOf(_heads, _heads.length);
        _next = new int[_blockByRow.length];
      }
      _next[_tails[slot] - 1] = row + 1;
      _tails[slot] = row + 1;
    }
  }

  private int findSlot(long key) {
    int slot = (int) HashCommon.mix(key) & _mask;
    int collisions = 0;
    while (_heads[slot] != 0 && _keys[slot] != key) {
      slot = (slot + 1) & _mask;
      if ((++collisions & (ArrowJoinKeys.BATCH_SIZE - 1)) == 0) {
        _checkTermination.run();
      }
    }
    return slot;
  }

  public int lookup(long key) {
    return _heads[findSlot(key)] - 1;
  }

  public int next(int row) {
    return _next == null ? -1 : _next[row] - 1;
  }

  public int blockId(int row) {
    return _blockByRow[row];
  }

  public int rowInBlock(int row) {
    return row - _blockStarts[_blockByRow[row]];
  }
}
