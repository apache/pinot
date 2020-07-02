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

import java.util.List;
import org.apache.pinot.common.utils.Pairs.IntPair;
import org.apache.pinot.core.common.Constants;


public final class SortedDocIdIterator implements IndexBasedDocIdIterator {
  private List<IntPair> pairs;
  private String datasourceName;

  public SortedDocIdIterator(String datasourceName, List<IntPair> pairs) {
    this.datasourceName = datasourceName;
    this.pairs = pairs;
  }

  int pairPointer = 0;
  int currentDocId = -1;

  @Override
  public int advance(int targetDocId) {
    if (pairPointer == this.pairs.size() || targetDocId > pairs.get(pairs.size() - 1).getRight()) {
      pairPointer = pairs.size();
      return (currentDocId = Constants.EOF);
    }
    if (currentDocId >= targetDocId) {
      return currentDocId;
    }
    // couter < targetDocId
    while (pairPointer < pairs.size()) {
      if (pairs.get(pairPointer).getLeft() > targetDocId) {
        // targetDocId in the gap between two valid pairs.
        currentDocId = pairs.get(pairPointer).getLeft();
        break;
      } else if (targetDocId >= pairs.get(pairPointer).getLeft() && targetDocId <= pairs.get(pairPointer).getRight()) {
        // targetDocId in the future valid pair.
        currentDocId = targetDocId;
        break;
      }
      pairPointer++;
    }
    if (pairPointer == pairs.size()) {
      currentDocId = Constants.EOF;
    }
    return currentDocId;
  }

  @Override
  public int next() {
    if (pairPointer == pairs.size() || currentDocId > pairs.get(pairs.size() - 1).getRight()) {
      pairPointer = pairs.size();
      return (currentDocId = Constants.EOF);
    }
    currentDocId = currentDocId + 1;
    if (pairPointer < pairs.size() && currentDocId > pairs.get(pairPointer).getRight()) {
      pairPointer++;
      if (pairPointer == pairs.size()) {
        currentDocId = Constants.EOF;
      } else {
        currentDocId = pairs.get(pairPointer).getLeft();
      }
    } else if (currentDocId < pairs.get(pairPointer).getLeft()) {
      currentDocId = pairs.get(pairPointer).getLeft();
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
