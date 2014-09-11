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
          public int skipTo(int targetDocId) {
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
    };
  }

  @Override
  public BlockMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getIntValue(int docId) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getFloatValue(int docId) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void resetBlock() {
    // TODO Auto-generated method stub

  }

}
