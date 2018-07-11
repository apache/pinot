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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;

public class StarTreeDocIdSet implements FilterBlockDocIdSet {
  public final class RangeBasedDocIdIterator implements BlockDocIdIterator {
    private int minDocId;
    private int maxDocId;

    public RangeBasedDocIdIterator(int minDocId, int maxDocId) {
      this.minDocId = minDocId;
      this.maxDocId = maxDocId;
    }

    @Override
    public int currentDocId() {
      return currentDocId;
    }

    @Override
    public int next() {
      if (currentDocId == -1) {
        currentDocId = minDocId;
      } else if (currentDocId != Constants.EOF && currentDocId < maxDocId) {
        currentDocId++;
      } else {
        currentDocId = Constants.EOF;
      }
      return currentDocId;
    }

    @Override
    public int advance(int targetDocId) {
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      if (currentDocId >= targetDocId) {
        return currentDocId;
      }
      currentDocId = targetDocId - 1;
      return next();
    }
  }

  private int minDocId = -1;
  private int maxDocId = -1;
  private int currentDocId = -1;

  @Override
  public int getMinDocId() {
    return minDocId;
  }

  @Override
  public int getMaxDocId() {
    return maxDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    this.minDocId = startDocId;
  }

  @Override
  public void setEndDocId(int endDocId) {
    this.maxDocId = endDocId;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    // Currently no one uses StarTreeDocIdSet.
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new RangeBasedDocIdIterator(minDocId, maxDocId);
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException("getRaw not supported for StarTreeDocIdSet");
  }
}
