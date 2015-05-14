/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;


public class DocIdSetBlock implements Block {

  private final int[] _docIdArray;
  private final int _searchableLength;

  public DocIdSetBlock(int[] docIdSet, int searchableLength) {
    _docIdArray = docIdSet;
    _searchableLength = searchableLength;
  }

  public int[] getDocIdSet() {
    return _docIdArray;
  }

  public int getSearchableLength() {
    return _searchableLength;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return true;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new BlockDocIdSet() {

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          int _pos = -1;

          @Override
          public int advance(int targetDocId) {
            throw new UnsupportedOperationException("Not support advance in DocIdSetBlock()");
          }

          @Override
          public int next() {
            _pos++;
            if (_pos == _searchableLength) {
              return Constants.EOF;
            }
            return _docIdArray[_pos];
          }

          @Override
          public int currentDocId() {
            return _docIdArray[_pos];
          }
        };
      }

      @Override
      public Object getRaw() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

}
