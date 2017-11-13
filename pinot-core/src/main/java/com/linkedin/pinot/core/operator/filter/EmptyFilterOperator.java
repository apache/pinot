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
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;


/**
 * Extension of {@link BaseFilterOperator} that is empty, i.e. does not match any doc ids.
 */
public final class EmptyFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "EmptyFilterOperator";

  @Override
  protected BaseFilterBlock getNextBlock() {
    return new ExcludeEntireSegmentDocIdSetBlock();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean isResultEmpty() {
    return true;
  }

  public static class ExcludeEntireSegmentDocIdSetBlock extends BaseFilterBlock {
    @Override
    public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
      return new EmptyDocIdSet();
    }
  }

  /**
   * Implementation of {@link FilterBlockDocIdSet} that is empty.
   */
  public static final class EmptyDocIdSet implements FilterBlockDocIdSet {

    @Override
    public int getMinDocId() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxDocId() {
      return Integer.MIN_VALUE;
    }

    @Override
    public void setStartDocId(int startDocId) {
      // Nothing to set.
    }

    @Override
    public void setEndDocId(int endDocId) {
      // Nothing to set.
    }

    @Override
    public long getNumEntriesScannedInFilter() {
      return 0L;
    }

    @Override
    public BlockDocIdIterator iterator() {
      return new EmptyDocIdSetIterator();
    }

    @Override
    public <T> T getRaw() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Implementation of {@link BlockDocIdIterator} that is empty.
   */
  public static final class EmptyDocIdSetIterator implements BlockDocIdIterator {

    @Override
    public int advance(int targetDocId) {
      return Constants.EOF;
    }

    @Override
    public int next() {
      return Constants.EOF;
    }

    @Override
    public int currentDocId() {
      return Constants.EOF;
    }
  }
}
