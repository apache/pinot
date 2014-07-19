package com.linkedin.pinot.index.block;

import com.linkedin.pinot.index.common.BlockDocIdValueIterator;
import com.linkedin.pinot.index.common.BlockDocIdValueSet;
import com.linkedin.pinot.index.common.Constants;
import com.linkedin.pinot.index.common.Pairs;
import com.linkedin.pinot.index.common.Pairs.IntPair;
import com.linkedin.pinot.index.common.Predicate;


public class FakeBlockDocIdValueSet implements BlockDocIdValueSet {

  int[] values;
  Predicate p;

  public FakeBlockDocIdValueSet(int[] values, Predicate p) {
    this.values = values;
    this.p = p;
    System.out.println("creating FakeBlockDocIdValueSet Set with length : " + values.length);
  }

  @Override
  public BlockDocIdValueIterator iterator() {
    // TODO Auto-generated method stub
    return new BlockDocIdValueIterator() {
      private int[] vals = values;
      private int counter = 0;

      @Override
      public int currentDocId() {
        return counter;
      }

      @Override
      public int currentVal() {
        return vals[counter];
      }

      @Override
      public boolean advance() {
        // TODO Auto-generated method stub
        switch (p.getType()) {
          case EQ:
            while (counter < vals.length) {
              if (vals[counter] == Integer.parseInt(p.getRhs().get(0))) {
                counter++;
                return true;
              }
              counter++;
            }
            return false;
          default:
            break;
        }
        return false;
      }
    };
  }

}
