/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.dociditerators;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;

/**
 * All scan based filter iterators must implement this interface. This allows interesection to be
 * optimized.
 * For example, if the we have two iterators one index based and another scan based, instead of
 * iterating on both iterators while doing intersection, we iterate of index based and simply look
 * up on scan based iterator to check if docId matches
 */
public interface ScanBasedDocIdIterator extends BlockDocIdIterator {
  /**
   * @param docId
   * @return true if the doc id matches
   */
  boolean isMatch(int docId);

  MutableRoaringBitmap applyAnd(MutableRoaringBitmap answer);

  /**
   * Get number of entries scanned.
   *
   * @return number of entries scanned.
   */
  int getNumEntriesScanned();
}
