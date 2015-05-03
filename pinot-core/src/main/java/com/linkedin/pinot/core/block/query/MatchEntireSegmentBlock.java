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
package com.linkedin.pinot.core.block.query;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;


public class MatchEntireSegmentBlock implements Block {
  private final int _totalDocs;

  public MatchEntireSegmentBlock(int totalDocs) {
    _totalDocs = totalDocs;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public BlockId getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new BlockDocIdSet() {

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {

          private int _currentDoc = 0;

          @Override
          public int advance(int targetDocId) {
            return _currentDoc = targetDocId;
          }

          @Override
          public int next() {
            if (_currentDoc < _totalDocs) {
              return _currentDoc++;
            } else {
              return Constants.EOF;
            }
          }

          @Override
          public int currentDocId() {
            return _currentDoc;
          }
        };
      }

      @Override
      public Object getRaw() {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }

  @Override
  public BlockMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

}
