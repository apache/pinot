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
package org.apache.pinot.query.planner.partitioning;

import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.ArrowDataBlock;


/**
 * The {@code KeySelector} provides a partitioning function to encode a specific input data type into a key.
 *
 * <p>This key selector is used for computation such as GROUP BY or equality JOINs.
 *
 * <p>Key selector should always produce the same selection hash key when the same input is provided.
 */
public interface KeySelector<T> {
  String DEFAULT_HASH_ALGORITHM = "absHashCode";

  /**
   * Extracts the key out of the given row.
   */
  @Nullable
  T getKey(Object[] row);

  /**
   * Computes the hash of the given row.
   */
  int computeHash(Object[] input);

  /**
   * Returns the hash algorithm used to compute the hash.
   */
  default String hashAlgorithm() {
    return DEFAULT_HASH_ALGORITHM;
  }

  /** Returns the column indices this selector operates on. */
  int[] getColumnIds();

  /**
   * Creates an {@link ArrowKeyHasher} bound to the given Arrow data block.
   * The hasher is valid only for the lifetime of the block.
   */
  ArrowKeyHasher getArrowHasher(ArrowDataBlock arrowDataBlock);

  /**
   * Creates an {@link ArrowKeyComparator} that compares rows between two Arrow blocks.
   *
   * @param left  the left (probe) block
   * @param right the right (build) block
   * @param other the key selector for the right block
   */
  ArrowKeyComparator getArrowKeyComparator(ArrowDataBlock left, ArrowDataBlock right, KeySelector<?> other);

  /** Per-row hash function bound to a specific {@link ArrowDataBlock}. */
  interface ArrowKeyHasher {
    /** Returns the hash for the given row index. */
    int computeHash(int rowIdx);
  }

  /** Per-row equality check across two {@link ArrowDataBlock}s. */
  interface ArrowKeyComparator {
    /** Returns {@code true} if the left row and right row have equal keys. */
    boolean equals(int leftIdx, int rightIdx);
  }
}
