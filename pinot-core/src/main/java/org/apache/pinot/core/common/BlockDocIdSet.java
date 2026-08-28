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
import org.apache.pinot.core.operator.dociditerators.BitmapBasedDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.EmptyDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.RangelessBitmapDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// The `BlockDocIdSet` contains the matching document ids returned by the
/// [org.apache.pinot.core.operator.blocks.FilterBlock].
public interface BlockDocIdSet {

  /// Returns an iterator of the matching document ids. The document ids returned from the iterator should be in
  /// ascending order.
  BlockDocIdIterator iterator();

  /// Returns the number of entries (SV value contains one entry, MV value contains multiple entries) scanned in the
  /// filtering phase. This method should be called after the filtering is done.
  long getNumEntriesScannedInFilter();



  /// Returns an optimized version of this DocIdSet, potentially returning EmptyDocIdSet or MatchAllDocIdSet
  /// when appropriate, following the same pattern as filter operators.
  default BlockDocIdSet getOptimizedDocIdSet() {
    return this;
  }

  /// Returns whether [#applyAnd] can restrict this DocIdSet to a candidate document set *without* first evaluating
  /// the whole subtree over the segment.
  ///
  /// Composite DocIdSets (AND, OR, NOT) return `true`: they can forward the candidate set to their own children.
  /// This is what lets a restriction from an enclosing AND reach a scan-based predicate nested inside an OR, which
  /// would otherwise be evaluated against every document the OR branch matches. Leaf DocIdSets return `false`:
  /// their index lookup has already happened by the time the DocIdSet exists, so deferring them buys nothing.
  ///
  /// A caller must only defer calling [#iterator] on a child when this returns `true`.
  default boolean canApplyAnd() {
    return false;
  }

  /// Returns the document ids matching this DocIdSet that are also in `docIds`, i.e. the intersection of this
  /// DocIdSet with the given candidate set.
  ///
  /// `docIds` is never modified, so the same candidate set may be handed to several DocIdSets; an OR does exactly
  /// that when it forwards the candidate set to each of its branches.
  ///
  /// Like [ScanBasedDocIdIterator#applyAnd], this method consumes the DocIdSet: call it at most once, and never
  /// together with [#iterator].
  default MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    if (docIds.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    BlockDocIdIterator docIdIterator = iterator();
    if (docIdIterator instanceof ScanBasedDocIdIterator) {
      // The scan only visits the candidate documents, which is the whole point of the push-down
      return ((ScanBasedDocIdIterator) docIdIterator).applyAnd(docIds);
    }
    if (docIdIterator instanceof BitmapBasedDocIdIterator) {
      return ImmutableRoaringBitmap.and(((BitmapBasedDocIdIterator) docIdIterator).getDocIds(), docIds);
    }
    if (docIdIterator instanceof SortedDocIdIterator) {
      MutableRoaringBitmap docIdsFromRanges = new MutableRoaringBitmap();
      for (IntPair docIdRange : ((SortedDocIdIterator) docIdIterator).getDocIdRanges()) {
        // NOTE: docIdRange has inclusive start and end.
        docIdsFromRanges.add(docIdRange.getLeft(), docIdRange.getRight() + 1L);
      }
      docIdsFromRanges.and(docIds);
      return docIdsFromRanges;
    }
    // Generic fallback: drive the iterator from the candidate set so that it is only asked about candidate documents
    BlockDocIdIterator restrictedDocIdIterator = new AndDocIdIterator(
        new BlockDocIdIterator[]{new RangelessBitmapDocIdIterator(docIds), docIdIterator});
    RoaringBitmapWriter<MutableRoaringBitmap> bitmapWriter =
        RoaringBitmapWriter.bufferWriter().runCompress(false).get();
    int docId;
    while ((docId = restrictedDocIdIterator.next()) != Constants.EOF) {
      bitmapWriter.add(docId);
    }
    return bitmapWriter.get();
  }

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
      return new RangelessBitmapDocIdSet(bitmapWriter.get());
    }

    // NOTE: AND and OR DocIdSet might return BitmapBasedDocIdIterator after processing the iterators. Create a new
    //       DocIdSet to prevent processing the iterators again
    if (docIdIterator instanceof RangelessBitmapDocIdIterator) {
      return new RangelessBitmapDocIdSet((RangelessBitmapDocIdIterator) docIdIterator);
    }
    if (docIdIterator instanceof BitmapDocIdIterator) {
      return new BitmapDocIdSet((BitmapDocIdIterator) docIdIterator);
    }
    if (docIdIterator instanceof EmptyDocIdIterator) {
      return EmptyDocIdSet.getInstance();
    }

    return this;
  }
}
