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

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ArrayBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;


public class TestFilterOperator extends BaseFilterOperator {
  private int[] _docIds;

  public TestFilterOperator(int[] docIds) {
    _docIds = docIds;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new FilterBlockDocIdSet() {
      private int _minDocId = _docIds[0];
      private int _maxDocId = _docIds[_docIds.length - 1];

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
        return new ArrayBasedDocIdIterator(_docIds, _docIds.length);
      }

      @Override
      public <T> T getRaw() {
        throw new UnsupportedOperationException();
      }
    });
  }

  @Override
  public String getOperatorName() {
    return "TestFilterOperator";
  }
}
