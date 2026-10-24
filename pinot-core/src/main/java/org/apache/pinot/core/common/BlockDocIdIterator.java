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

import static com.google.common.base.Preconditions.checkArgument;


/// The interface `BlockDocIdIterator` represents the iterator for `BlockDocIdSet`. The document
/// ids returned from the iterator should be in ascending order.
public interface BlockDocIdIterator extends AutoCloseable {

  /// Returns the next matching document id, or [org.apache.pinot.segment.spi.Constants#EOF] if there is no
  /// more matching documents.
  ///
  /// NOTE: There should be no more calls to this method after it returns
  /// [org.apache.pinot.segment.spi.Constants#EOF].
  int next();

  /// Writes up to `maxDocs` next matching document ids into the prefix of the caller-owned `output` array and returns
  /// the number written. Requires `0 < maxDocs <= output.length`. Does not retain the array or modify its remaining
  /// elements. A full batch does not consume another document to look ahead.
  ///
  /// A return value less than `maxDocs` means the iterator is exhausted. There must be no further calls to
  /// [#next()], [#advance(int)], or this method after a short batch. Otherwise, these methods share the same cursor
  /// and may be interleaved subject to the target constraint of [#advance(int)]. This method must not be called after
  /// either scalar method returns [Constants#EOF].
  ///
  /// The default implementation preserves scalar iteration for existing implementations.
  default int nextBatch(int[] output, int maxDocs) {
    checkArgument(maxDocs > 0 && maxDocs <= output.length);
    int size = 0;
    while (size < maxDocs) {
      int docId = next();
      if (docId == Constants.EOF) {
        break;
      }
      output[size++] = docId;
    }
    return size;
  }

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
