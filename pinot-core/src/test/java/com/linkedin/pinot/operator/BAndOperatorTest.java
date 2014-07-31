package com.linkedin.pinot.operator;

import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.testng.annotations.Test;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.datasource.SingleBlockDataSource;
import com.linkedin.pinot.core.operator.BAndOperator;

public class BAndOperatorTest {

	/**
	 * Simple interesection between two blocks
	 */
	@Test
	public void simpleAndTest() {
		int[] dataA = new int[] {2,3,4,1,1,3,4,4,3,2,3,1,1};
		int[][] invertedIndexA = makeInverted(dataA);
		int[] dictionaryA = new int[] {1,2,3,4};
		List<String> rhsA = new ArrayList<String>();
		rhsA.add("2");
		Predicate predicateA = new Predicate("A", Predicate.Type.EQ, rhsA);
		SingleBlockDataSource dsA = new SingleBlockDataSource(dataA,
				invertedIndexA, dictionaryA);
		dsA.setPredicate(predicateA);

		int[] dataB = new int[] {2,3,4,1,1,3,4,4,3,2,3,1,1};
		int[][] invertedIndexB = makeInverted(dataB);
		int[] dictionaryB = new int[] {1,2,3,4};
		List<String> rhsB = new ArrayList<String>();
		rhsB.add("2");
		Predicate predicateB = new Predicate("B", Predicate.Type.EQ, rhsB);
		SingleBlockDataSource dsB = new SingleBlockDataSource(dataB,
				invertedIndexB, dictionaryB);
		dsB.setPredicate(predicateB);

		BAndOperator andOperator = new BAndOperator(dsA, dsB);

		andOperator.open();
		Block block;
		while ((block = andOperator.nextBlock()) != null) {
			BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
			BlockDocIdIterator iterator = blockDocIdSet.iterator();
			int docId;
			while ((docId = iterator.next()) != Constants.EOF) {
				System.out.println(docId);
			}
		}
		andOperator.close();

	}

	private int[][] makeInverted(int[] dataA) {
		TreeMap<Integer,TreeSet<Integer>> map = new TreeMap<Integer, TreeSet<Integer>>();
		for (int i = 0; i < dataA.length; i++) {
			int j = dataA[i];
			if(!map.containsKey(j)){
				map.put(j, new TreeSet<Integer>());
			}
			map.get(j).add(i);
		}
		int valIndex =0;
		int[][] invertedIndex = new int[map.size()][];
		for(Integer key:map.keySet()){
			invertedIndex[valIndex]= new int[map.get(key).size()];
			int docIndex =0;
			for (int docId : map.get(key)) {
				invertedIndex[valIndex][docIndex] = docId;
				docIndex = docIndex + 1;
			}
			valIndex++;
		}
		return invertedIndex;
	}
	
	
}
