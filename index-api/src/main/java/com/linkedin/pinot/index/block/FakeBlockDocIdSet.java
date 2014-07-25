package com.linkedin.pinot.index.block;

import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.Constants;
import com.linkedin.pinot.index.common.Predicate;


public class FakeBlockDocIdSet implements BlockDocIdSet {

  int[] values;
  Predicate p;

  public FakeBlockDocIdSet(int[] values, Predicate p) {
    this.p = p;
    this.values = values;
    System.out.println("creating FakeBlockDocIdSet Set with length : " + values.length);
  }

  @Override
  public BlockDocIdIterator iterator() {
    // TODO Auto-generated method stub
    return new BlockDocIdIterator() {
      Predicate predicate = p;
      int[] vals = values;
      int counter = 0;

      @Override
      public int skipTo(int targetDocId) {
        if (vals.length == targetDocId)
          return -1;
        counter = targetDocId;
        return targetDocId;
      }

      @Override
      public int next() {
        switch (predicate.getType()) {
          case EQ:
            int ret = Constants.EOF;
            while (counter < vals.length) {
              if (vals[counter] == Integer.parseInt(predicate.getRhs().get(0))) {
                ret = counter;
                counter++;
                break;
              }
              counter++;
            }
            return ret;

          default:
            break;
        }
        return -1;
      }

      @Override
      public int currentDocId() {
        return counter;
      }

    };
  }

}
