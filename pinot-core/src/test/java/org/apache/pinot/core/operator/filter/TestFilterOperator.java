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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.segment.spi.Constants;


public class TestFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_TEST";

  private final int[] _docIds;

  public TestFilterOperator(int[] docIds) {
    _docIds = docIds;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new FilterBlockDocIdSet() {
      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          private final int _numDocIds = _docIds.length;
          private int _nextIndex = 0;

          @Override
          public int next() {
            if (_nextIndex < _numDocIds) {
              return _docIds[_nextIndex++];
            } else {
              return Constants.EOF;
            }
          }

          @Override
          public int advance(int targetDocId) {
            while (_nextIndex < _numDocIds) {
              int docId = _docIds[_nextIndex++];
              if (docId >= targetDocId) {
                return docId;
              }
            }
            return Constants.EOF;
          }
        };
      }

      @Override
      public long getNumEntriesScannedInFilter() {
        return 0L;
      }
    });
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }
}
