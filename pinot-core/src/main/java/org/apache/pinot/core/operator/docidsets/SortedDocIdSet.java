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
package org.apache.pinot.core.operator.docidsets;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.utils.Pairs.IntPair;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.EmptyBlockDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;


public class SortedDocIdSet implements FilterBlockDocIdSet {

  public final List<IntPair> pairs;
  public final AtomicLong timeMeasure = new AtomicLong(0);
  int startDocId;
  int endDocId;
  private String datasourceName;

  public SortedDocIdSet(String datasourceName, List<IntPair> pairs) {
    this.datasourceName = datasourceName;
    this.pairs = pairs;
  }

  @Override
  public int getMinDocId() {
    if (pairs.size() > 0) {
      return pairs.get(0).getLeft();
    } else {
      return 0;
    }
  }

  @Override
  public int getMaxDocId() {
    if (pairs.size() > 0) {
      return pairs.get(pairs.size() - 1).getRight();
    } else {
      return 0;
    }
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  @Override
  public void setStartDocId(int startDocId) {
    this.startDocId = startDocId;
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  @Override
  public void setEndDocId(int endDocId) {
    this.endDocId = endDocId;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    // No value scanned when filtering with sorted index.
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    if (pairs == null || pairs.isEmpty()) {
      return EmptyBlockDocIdIterator.getInstance();
    }
    return new SortedDocIdIterator(datasourceName, pairs);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) pairs;
  }

  @Override
  public String toString() {
    return pairs.toString();
  }
}
