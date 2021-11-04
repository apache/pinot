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
package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.query.spi.data.BlockDocIdIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * All scan-based filter iterators should implement this interface to allow intersection (AND operation) to be
 * optimized.
 * <p>When there are at least one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator) and at
 * least one ScanBasedDocIdIterator, instead of iterating on each BlockDocIdIterator (we should avoid iterating on
 * ScanBasedDocIdIterator because that requires a lot of document scans), it can be optimized by first constructing a
 * bitmap of matching document ids from the index-based BlockDocIdIterators, and let ScanBasedDocIdIterator only scan
 * the matching document ids from the index-base BlockDocIdIterators.
 */
public interface ScanBasedDocIdIterator extends BlockDocIdIterator {

  /**
   * Applies AND operation to the given bitmap of document ids, returns a bitmap of the matching document ids.
   */
  MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds);

  /**
   * Returns the number of entries (SV value contains one entry, MV value contains multiple entries) scanned during the
   * iteration. This method should be called after the iteration is done.
   */
  long getNumEntriesScanned();
}
