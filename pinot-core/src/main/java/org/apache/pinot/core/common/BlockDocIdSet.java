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

import org.apache.pinot.core.operator.dociditerators.AndDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.EmptyDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.RangelessBitmapDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// The `BlockDocIdSet` contains the matching document ids returned by the
/// [org.apache.pinot.core.operator.blocks.FilterBlock].
///
/// A result known to be empty is represented as an [EmptyDocIdSet], so that a parent can recognize it by type and
/// short-circuit. The index-based implementations offer a `create` factory that returns one for an empty input.
public interface BlockDocIdSet {

  /// Returns an iterator of the matching document ids. The document ids returned from the iterator should be in
  /// ascending order.
  BlockDocIdIterator iterator();

  /// Returns the number of entries (SV value contains one entry, MV value contains multiple entries) scanned in the
  /// filtering phase. This method should be called after the filtering is done.
  long getNumEntriesScannedInFilter();

  /// For scan-based FilterBlockDocIdSet, pre-scans the documents and returns a non-scan-based FilterBlockDocIdSet.
  default BlockDocIdSet toNonScanDocIdSet() {
    BlockDocIdIterator docIdIterator = iterator();
    // NOTE: AND and OR DocIdIterator might contain scan-based DocIdIterator
    // TODO: This scan is not counted in the execution stats
    if (docIdIterator instanceof ScanBasedDocIdIterator || docIdIterator instanceof AndDocIdIterator
        || docIdIterator instanceof OrDocIdIterator) {
      RoaringBitmapWriter<MutableRoaringBitmap> bitmapWriter =
          RoaringBitmapWriter.bufferWriter().runCompress(false).get();
      int docId;
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        bitmapWriter.add(docId);
      }
      return RangelessBitmapDocIdSet.create(bitmapWriter.get());
    }

    // NOTE: AND and OR DocIdSet might return BitmapBasedDocIdIterator after processing the iterators. Create a new
    //       DocIdSet to prevent processing the iterators again
    if (docIdIterator instanceof RangelessBitmapDocIdIterator) {
      return RangelessBitmapDocIdSet.create((RangelessBitmapDocIdIterator) docIdIterator);
    }
    if (docIdIterator instanceof BitmapDocIdIterator) {
      return BitmapDocIdSet.create((BitmapDocIdIterator) docIdIterator);
    }
    if (docIdIterator instanceof EmptyDocIdIterator) {
      return EmptyDocIdSet.unscanned();
    }

    return this;
  }
}
