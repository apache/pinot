/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.CompositeDocIdIterator;
import java.util.ArrayList;
import java.util.List;


public final class CompositeFilterBlockDocIdSet implements FilterBlockDocIdSet {
  private final int finalMaxDocId;
  private final int finalMinDocId;
  List<FilterBlockDocIdSet> filterDocIdSets;

  public CompositeFilterBlockDocIdSet(List<FilterBlockDocIdSet> filterDocIdSets) {
    this.filterDocIdSets = filterDocIdSets;
    Integer minDocId = null;
    Integer maxDocId = null;
    for (FilterBlockDocIdSet filterBlockDocIdSet : filterDocIdSets) {
      if (minDocId == null || filterBlockDocIdSet.getMinDocId() < minDocId) {
        minDocId = filterBlockDocIdSet.getMinDocId();
      }
      if (maxDocId == null || filterBlockDocIdSet.getMaxDocId() > maxDocId) {
        maxDocId = filterBlockDocIdSet.getMaxDocId();
      }
    }
    finalMinDocId = (minDocId == null) ? 0 : minDocId;
    finalMaxDocId = (maxDocId == null) ? 0 : maxDocId;

  }

  @Override
  public int getMinDocId() {
    return finalMinDocId;
  }

  @Override
  public int getMaxDocId() {
    return finalMaxDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEndDocId(int endDocId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    long numEntriesScannedInFilter = 0L;
    for (FilterBlockDocIdSet blockDocIdSet : filterDocIdSets) {
      numEntriesScannedInFilter += blockDocIdSet.getNumEntriesScannedInFilter();
    }
    return numEntriesScannedInFilter;
  }

  @Override
  public BlockDocIdIterator iterator() {
    List<BlockDocIdIterator> docIdIterators = new ArrayList<>();
    for (FilterBlockDocIdSet filterBlockDocIdSet : filterDocIdSets) {
      docIdIterators.add(filterBlockDocIdSet.iterator());
    }
    return new CompositeDocIdIterator(docIdIterators);
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException();
  }
}
