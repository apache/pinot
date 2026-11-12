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

/// The interface `BlockDocIdIterator` represents the iterator for `BlockDocIdSet`. The document
/// ids returned from the iterator should be in ascending order.
public interface BlockDocIdIterator extends AutoCloseable {

  /// Returns the next matching document id, or [org.apache.pinot.segment.spi.Constants#EOF] if there is no
  /// more matching documents.
  ///
  /// NOTE: There should be no more calls to this method after it returns
  /// [org.apache.pinot.segment.spi.Constants#EOF].
  int next();

  /// Returns the first matching document whose id is greater than or equal to the given target document id, or
  /// [org.apache.pinot.segment.spi.Constants#EOF] if there is no such document.
  ///
  /// NOTE: The target document id should be GREATER THAN the document id previous returned because the iterator
  ///          should not return the same value twice.
  ///
  /// NOTE: There should be no more calls to this method after it returns
  /// [org.apache.pinot.segment.spi.Constants#EOF].
  int advance(int targetDocId);

  /// Empirically determined to be the best batch size for batch iterators.
  /// @see {https://github.com/RoaringBitmap/RoaringBitmap/pull/243#issuecomment-381278304}
  int OPTIMAL_ITERATOR_BATCH_SIZE = 256;

  /// Close resources if applicable.
  @Override
  default void close() {
    // do nothing by default
  }
}
