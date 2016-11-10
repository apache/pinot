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
package com.linkedin.pinot.operator;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.dociditerators.ArrayBasedDocIdIterator;
import com.linkedin.pinot.core.operator.docidsets.ArrayBasedDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;


public final class ArrayBasedFilterBlock extends BaseFilterBlock {

  private final int[] _docIdArray;
  private final int _searchableLength;
  private int _minDocId;
  private int _maxDocId;

  public ArrayBasedFilterBlock(int[] docIdArray) {
    _docIdArray = docIdArray;
    _searchableLength = docIdArray.length;
    _minDocId = docIdArray[0];
    _maxDocId = docIdArray[_searchableLength - 1];
  }

  @Override
  public BlockId getId() {
    return new BlockId(0);
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    return new FilterBlockDocIdSet() {
      @Override
      public int getMinDocId() {
        return _minDocId;
      }

      @Override
      public int getMaxDocId() {
        return _maxDocId;
      }

      @Override
      public void setStartDocId(int startDocId) {
        _minDocId = Math.max(_minDocId, startDocId);
      }

      @Override
      public void setEndDocId(int endDocId) {
        _maxDocId = Math.min(_maxDocId, endDocId);
      }

      @Override
      public long getNumEntriesScannedInFilter() {
        return 0;
      }

      @Override
      public BlockDocIdIterator iterator() {
        return new ArrayBasedDocIdIterator(_docIdArray, _searchableLength);
      }

      @Override
      public int[] getRaw() {
        return _docIdArray;
      }
    };
  }
}
