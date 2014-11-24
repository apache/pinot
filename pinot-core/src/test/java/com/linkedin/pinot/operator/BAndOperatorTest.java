package com.linkedin.pinot.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.testng.annotations.Test;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.operator.filter.BAndOperator;


public class BAndOperatorTest {

  /**
   * Simple interesection between two blocks
   */
  @Test
  public void simpleAndTest() {
    final int[] dataA = new int[] { 2, 3, 4, 1, 1, 3, 4, 4, 3, 2, 3, 1, 1 };
    final int[][] invertedIndexA = makeInverted(dataA);
    final int[] dictionaryA = new int[] { 1, 2, 3, 4 };
    final List<String> rhsA = new ArrayList<String>();
    rhsA.add("2");
    final Predicate predicateA = new Predicate("A", Predicate.Type.EQ, rhsA);
    final DataSource dsA = new DataSource() {

      @Override
      public boolean open() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public Block nextBlock(BlockId BlockId) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Block nextBlock() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean close() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean setPredicate(Predicate predicate) {
        // TODO Auto-generated method stub
        return false;
      }
    };
    dsA.setPredicate(predicateA);

    final int[] dataB = new int[] { 2, 3, 4, 1, 1, 3, 4, 4, 3, 2, 3, 1, 1 };
    final int[][] invertedIndexB = makeInverted(dataB);
    final int[] dictionaryB = new int[] { 1, 2, 3, 4 };
    final List<String> rhsB = new ArrayList<String>();
    rhsB.add("2");
    final Predicate predicateB = new Predicate("B", Predicate.Type.EQ, rhsB);
    final DataSource dsB = new DataSource() {

      @Override
      public boolean open() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public Block nextBlock(BlockId BlockId) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Block nextBlock() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean close() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean setPredicate(Predicate predicate) {
        // TODO Auto-generated method stub
        return false;
      }
    };
    dsB.setPredicate(predicateB);

    final BAndOperator andOperator = new BAndOperator(dsA, dsB);

    andOperator.open();
    Block block;
    while ((block = andOperator.nextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        System.out.println(docId);
      }
    }
    andOperator.close();

  }

  private int[][] makeInverted(int[] dataA) {
    final TreeMap<Integer, TreeSet<Integer>> map = new TreeMap<Integer, TreeSet<Integer>>();
    for (int i = 0; i < dataA.length; i++) {
      final int j = dataA[i];
      if (!map.containsKey(j)) {
        map.put(j, new TreeSet<Integer>());
      }
      map.get(j).add(i);
    }
    int valIndex = 0;
    final int[][] invertedIndex = new int[map.size()][];
    for (final Integer key : map.keySet()) {
      invertedIndex[valIndex] = new int[map.get(key).size()];
      int docIndex = 0;
      for (final int docId : map.get(key)) {
        invertedIndex[valIndex][docIndex] = docId;
        docIndex = docIndex + 1;
      }
      valIndex++;
    }
    return invertedIndex;
  }

}
