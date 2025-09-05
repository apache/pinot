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
package org.apache.pinot.core.common;

import org.apache.pinot.segment.spi.Constants;


/**
 * The interface <code>BlockDocIdIterator</code> represents the iterator for {@link BlockDocIdSet}.
 *
 * Each call to {@link #next()} or {@link #advance(int)} returns the next matching document id
 * in the iterator, or {@link Constants#EOF} if there is no more matching document.
 *
 * The order of the returned document ids depends on the order of the iterator, which can be
 * either ascending or descending.
 */
public interface BlockDocIdIterator extends AutoCloseable {

  /**
   * Returns the next matching document id, or {@link Constants#EOF} if there is no more matching documents.
   * <p>NOTE: There should be no more calls to this method after it returns {@link Constants#EOF}.
   */
  int next();

  /**
   * Returns the first matching document whose id is equal or greater (for ascending iterators) or smaller
   * (for descending iterators) than the given target document id, or {@link Constants#EOF} if there is no such
   * document.
   * <p>NOTE: The target document id should be GREATER/SMALLER THAN the document id previous returned because the
   *          iterator should not return the same value twice.
   * <p>NOTE: There should be no more calls to this method after it returns {@link Constants#EOF}.
   * <p>The actual constraint depends on the order of the iterator: For ascending iterator, the target document id
   * should be greater than the previous returned document id; For descending iterator, the target document id
   * should be smaller than the previous returned document id.
   */
  int advance(int targetDocId);

  /**
   * Empirically determined to be the best batch size for batch iterators.
   * @see {https://github.com/RoaringBitmap/RoaringBitmap/pull/243#issuecomment-381278304}
   */
  int OPTIMAL_ITERATOR_BATCH_SIZE = 256;

  /**
   * Close resources if applicable.
   */
  @Override
  default void close() {
    // do nothing by default
  }
}
