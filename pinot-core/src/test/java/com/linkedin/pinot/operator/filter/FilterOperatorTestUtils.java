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
package com.linkedin.pinot.operator.filter;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.dociditerators.ArrayBasedDocIdIterator;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;


public class FilterOperatorTestUtils {
  private FilterOperatorTestUtils() {
  }

  public static BaseFilterOperator makeFilterOperator(final int[] docIds) {
    return new BaseFilterOperator() {
      @Override
      public boolean isResultEmpty() {
        return docIds.length == 0;
      }

      @Override
      protected BaseFilterBlock getNextBlock() {
        return new BaseFilterBlock() {
          @Override
          public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
            return new FilterBlockDocIdSet() {
              private int _minDocId = docIds[0];
              private int _maxDocId = docIds[docIds.length - 1];

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
                return new ArrayBasedDocIdIterator(docIds, docIds.length);
              }

              @Override
              public <T> T getRaw() {
                return null;
              }
            };
          }
        };
      }

      @Override
      public String getOperatorName() {
        return null;
      }
    };
  }
}
