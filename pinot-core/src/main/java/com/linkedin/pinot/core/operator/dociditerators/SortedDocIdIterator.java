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
package com.linkedin.pinot.core.operator.dociditerators;

import java.util.List;

import com.linkedin.pinot.common.utils.DocIdRange;
import com.linkedin.pinot.core.common.Constants;


public final class SortedDocIdIterator implements IndexBasedDocIdIterator {
  /**
   * 
   */
  private List<DocIdRange> docIdRanges;
  private String datasourceName;

  /**
   * @param sortedDocIdSet
   */
  public SortedDocIdIterator(String datasourceName, List<DocIdRange> docIdRanges) {
    this.datasourceName = datasourceName;
    this.docIdRanges = docIdRanges;
  }

  int rangeIndex = 0;
  int currentDocId = -1;

  @Override
  public int advance(int targetDocId) {
    if (rangeIndex == this.docIdRanges.size() || targetDocId > docIdRanges.get(docIdRanges.size() - 1).getEnd()) {
      rangeIndex = docIdRanges.size();
      return (currentDocId = Constants.EOF);
    }
    if (currentDocId >= targetDocId) {
      return currentDocId;
    }
    // couter < targetDocId
    while (rangeIndex < docIdRanges.size()) {
      if (docIdRanges.get(rangeIndex).getStart() > targetDocId) {
        // targetDocId in the gap between two valid pairs.
        currentDocId = docIdRanges.get(rangeIndex).getStart();
        break;
      } else if (targetDocId >= docIdRanges.get(rangeIndex).getStart()
          && targetDocId <= docIdRanges.get(rangeIndex).getEnd()) {
        // targetDocId in the future valid pair.
        currentDocId = targetDocId;
        break;
      }
      rangeIndex++;
    }
    if (rangeIndex == docIdRanges.size()) {
      currentDocId = Constants.EOF;
    }
    return currentDocId;
  }

  @Override
  public int next() {
    if (rangeIndex == docIdRanges.size() || currentDocId > docIdRanges.get(docIdRanges.size() - 1).getEnd()) {
      rangeIndex = docIdRanges.size();
      return (currentDocId = Constants.EOF);
    }
    currentDocId = currentDocId + 1;
    if (rangeIndex < docIdRanges.size() && currentDocId > docIdRanges.get(rangeIndex).getEnd()) {
      rangeIndex++;
      if (rangeIndex == docIdRanges.size()) {
        currentDocId = Constants.EOF;
      } else {
        currentDocId = docIdRanges.get(rangeIndex).getStart();
      }
    } else if (currentDocId < docIdRanges.get(rangeIndex).getStart()) {
      currentDocId = docIdRanges.get(rangeIndex).getStart();
    }
    return currentDocId;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }

  @Override
  public String toString() {
    return SortedDocIdIterator.class.getSimpleName() + " [ " + datasourceName + "]";
  }
}
