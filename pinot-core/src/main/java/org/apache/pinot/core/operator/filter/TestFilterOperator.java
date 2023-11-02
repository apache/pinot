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
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.NullMode;


public class TestFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_TEST";

  private final int[] _trueDocIds;
  private final int[] _nullDocIds;

  public TestFilterOperator(int[] trueDocIds, int[] nullDocIds, int numDocs) {
    super(numDocs, NullMode.ALL_NULLABLE);
    _trueDocIds = trueDocIds;
    _nullDocIds = nullDocIds;
  }

  public TestFilterOperator(int[] docIds, int numDocs) {
    super(numDocs, NullMode.NONE_NULLABLE);
    _trueDocIds = docIds;
    _nullDocIds = new int[0];
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  protected BlockDocIdSet getTrues() {
    return new TestBlockDocIdSet(_trueDocIds);
  }

  @Override
  protected BlockDocIdSet getNulls() {
    return new TestBlockDocIdSet(_nullDocIds);
  }

  private static class TestBlockDocIdSet implements BlockDocIdSet {
    private final int[] _docIds;

    public TestBlockDocIdSet(int[] docIds) {
      _docIds = docIds;
    }

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
  }
}
