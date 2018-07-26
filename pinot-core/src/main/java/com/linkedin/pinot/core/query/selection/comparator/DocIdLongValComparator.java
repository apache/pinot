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
package com.linkedin.pinot.core.query.selection.comparator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockSingleValIterator;

public class DocIdLongValComparator implements IDocIdValComparator {

  int orderToggleMultiplier = 1;
  private final BlockSingleValIterator blockValSetIterator;

  public DocIdLongValComparator(Block block, boolean ascending) {
    blockValSetIterator = (BlockSingleValIterator) block.getBlockValueSet().iterator();
    if (!ascending) {
      orderToggleMultiplier = -1;
    }
  }

  public int compare(int docId1, int docId2) {
    blockValSetIterator.skipTo(docId1);
    long val1 = blockValSetIterator.nextLongVal();
    blockValSetIterator.skipTo(docId2);
    long val2 = blockValSetIterator.nextLongVal();
    return Long.compare(val1, val2) * orderToggleMultiplier;
  }

}
