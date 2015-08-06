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
package com.linkedin.pinot.query.aggregation;

import static org.testng.Assert.assertEquals;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.query.aggregation.function.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

import com.linkedin.pinot.util.TestUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;

/**
 *
 * Tests for DistinctCountHLL estimation, the rountine is similar to {@link SimpleAggregationFunctionsTest}
 *
 *
 */
public class DistinctCountHLLTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistinctCountHLLTest.class);

    public static int[] _docIdsArray;
    public static IntArray _docIds;
    public static int _sizeOfDocIdArray = 5000;
    public static String _columnName = "met";
    public static AggregationInfo _paramsInfo;

    private static final int DUPLICATION_PER_ITEM = 1000;

    @BeforeClass
    public static void setup() {
        _docIdsArray = new int[_sizeOfDocIdArray];
        for (int i = 0; i < _sizeOfDocIdArray; ++i) {
            _docIdsArray[i] = i;
        }
        _docIds = new DefaultIntArray(_docIdsArray);
        Map<String, String> params = new HashMap<String, String>();
        params.put("column", _columnName);
        _paramsInfo = new AggregationInfo();
        _paramsInfo.setAggregationType("");
        _paramsInfo.setAggregationParams(params);

    }

    public static class RandomNumberArray {
        private static Random _rnd = new Random(System.currentTimeMillis());

        private final Integer[] arr;
        private final HashSet<Integer> set = new HashSet<Integer>();

        /**
         * Data ranges between [0, size)
         * @param size
         * @param duplicationPerItem
         */
        public RandomNumberArray(int size, int duplicationPerItem) {
            List<Integer> lst = new ArrayList<Integer>();
            for (int i = 0; i < size/duplicationPerItem; i++) {
                Integer item = _rnd.nextInt(size);
                for (int j = 0; j < duplicationPerItem; j++) {
                    lst.add(item); // add duplicates
                }
            }
            // add remaining items
            int st = lst.size();
            for (int i = st; i < size; i++) {
                Integer item = _rnd.nextInt(size);
                lst.add(item);
            }
            // add to set
            set.addAll(lst);
            // shuffle
            Collections.shuffle(lst);
            // toIntArray
            arr = lst.toArray(new Integer[0]);
            if (arr.length != size) {
                throw new RuntimeException("should not happen");
            }
        }

        public int size() {
            return arr.length;
        }

        public int getPreciseCardinality() {
            return set.size();
        }

        public void offerAllNumberTo(HyperLogLog hyperLogLog) {
            offerNumberInRangeTo(hyperLogLog, 0, arr.length);
        }

        public void offerAllNumberTo(IntOpenHashSet hashSet) {
            offerNumberInRangeTo(hashSet, 0, arr.length);
        }

        public void offerNumberInRangeTo(HyperLogLog hyperLogLog, int start, int end) {
            end = Math.min(end, arr.length);
            for (int i = start; i < end; i++) {
                hyperLogLog.offer(arr[i]);
            }
        }

        public void offerNumberInRangeTo(IntOpenHashSet hashSet, int start, int end) {
            end = Math.min(end, arr.length);
            for (int i = start; i < end; i++) {
                hashSet.add(arr[i]);
            }
        }
    }

    @Test
    public void testDistinctCountHLLAggregation() {
        AggregationFunction aggregationFunction = new DistinctCountHLLAggregationFunction();
        aggregationFunction.init(_paramsInfo);

        // Test aggregate

        // Test combine
        int _sizeOfCombineList = 1000;
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int i = 1; i <= _sizeOfCombineList; ++i) {
            List<Serializable> aggregationResults = getHLLResultValues(i);
            List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
            long estimate = ((HyperLogLog) (combinedResult.get(0))).cardinality();
            TestUtils.assertApproximation(estimate, i, 0.1);
            sb1.append(estimate + ", ");
            sb2.append(i + ", ");
        }
        // assertEquals(sb1.toString(), sb2.toString());  // assert actual equals (nearly impossible!)

        // Test reduce
        for (int i = 1; i <= _sizeOfCombineList; ++i) {
          List<Serializable> combinedResults = getHLLResultValues(i);
          long reduceSize = (Long) aggregationFunction.reduce(combinedResults);
          TestUtils.assertApproximation(reduceSize, i, 0.1);
        }
    }

    @Test
    public void testDistinctCountHLLRandomAggregationLarge() {
        AggregationFunction aggregationFunction = new DistinctCountHLLAggregationFunction();
        aggregationFunction.init(_paramsInfo);

        // Test aggregate

        // Test combine
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        // Make combine list number fixed to 10, each list has large number of elements
        int maxSize = 100000; // 10000000
        for (int i = 1; i <= maxSize; i += maxSize/17) {
            if (i==1) continue;
            RandomNumberArray arr = new RandomNumberArray(i*10, 1);
            long t1 = System.nanoTime();
            List<Serializable> aggregationResults = getHLLRandomResultValues(arr, 10, i);
            long t2 = System.nanoTime();
            List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
            long t3 = System.nanoTime();
            long estimate = ((HyperLogLog) (combinedResult.get(0))).cardinality();
            long precise = arr.getPreciseCardinality();
            TestUtils.assertApproximation(estimate, precise, 0.1);
            println(i + ", " + "" + (t2 - t1) + "" + ", " + (t3 - t2) + ", " + getErrorString(precise, estimate));
            sb1.append(estimate + ", ");
            sb2.append(precise + ", ");
        }
        // println("Error: " + sb3.toString());
        // assertEquals(sb1.toString(), sb2.toString());  // assert actual equals (nearly impossible!)

        // Test reduce
        /*for (int i = 1; i <= _sizeOfCombineList; ++i) {
          List<Serializable> combinedResults = getHLLResultValues(i);
          long reduceSize = (Long) aggregationFunction.reduce(combinedResults);
          assertApproximation(reduceSize, i, 1.1);
        }*/
    }

    @Test
    public void testDistinctCountHLLRandomAggregationCombine() {
        AggregationFunction hllAggregationFunction = new DistinctCountHLLAggregationFunction();
        hllAggregationFunction.init(_paramsInfo);
        AggregationFunction setAggregationFunction = new DistinctCountAggregationFunction();
        setAggregationFunction.init(_paramsInfo);

        final int numOfItemsPerList = 100;
        final int numOfListCombined = 100000;
        println("#list_combined, HLL_time(nano), IntOpenHashSet(nano), time_ratio, estimate, precise, error");
        // Test combine
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int i = 1; i <= numOfListCombined; i+=numOfListCombined/17) {
            if (i == 1) {
                continue;
            }
            RandomNumberArray arr = new RandomNumberArray(i*numOfItemsPerList, DUPLICATION_PER_ITEM);

            List<Serializable> hllAggregationResults = getHLLRandomResultValues(arr, i, numOfItemsPerList);
            long t1 = System.nanoTime();
            List<Serializable> hllCombinedResult = hllAggregationFunction.combine(hllAggregationResults, CombineLevel.SEGMENT);
            long t2 = System.nanoTime();

            List<Serializable> setAggregationResults = getIntOpenHashSets(arr, i, numOfItemsPerList);
            long t3 = System.nanoTime();
            List<Serializable> setCombinedResult = setAggregationFunction.combine(setAggregationResults, CombineLevel.SEGMENT);
            long t4 = System.nanoTime();

            long estimate = ((HyperLogLog) (hllCombinedResult.get(0))).cardinality();
            long precise = arr.getPreciseCardinality();

            TestUtils.assertApproximation(estimate, precise, 0.15);
            assertEquals(((IntOpenHashSet) (setCombinedResult.get(0))).size(), precise);
            println(i + ", " + (t2 - t1) + ", " + (t4 - t3) + ", " + (t2 - t1 + 0.0) / (t4 - t3 + 0.0) + ", "
                    + estimate + ", " + precise + ", " + getErrorString(precise, estimate));
        }
    }

    @Test
    public void testInsertionTime() {
        int numOfItems = 10000000;

        println("#items_inserted, HLL_time(nano), IntOpenHashSet(nano), time_ratio, estimate, precise, error");
        for (int i = 0; i < numOfItems; i+=numOfItems/17) {
            if (i == 0) {
                continue;
            }
            RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
            HyperLogLog hllResult = new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_BIT_SIZE);
            IntOpenHashSet set = new IntOpenHashSet(); //HashSet<Integer> set = new HashSet<Integer>();
            long t1 = System.nanoTime();
            arr.offerAllNumberTo(hllResult);
            long t2 = System.nanoTime();
            arr.offerAllNumberTo(set);
            long t3 = System.nanoTime();
            long estimate = hllResult.cardinality();
            long precise = set.size();
            println(i + ", " + "" + (t2 - t1) + ", " + (t3 - t2) + ", " + (t2 - t1 + 0.0) / (t3 - t2 + 0.0) + ", "
                    + estimate + ", " + precise + ", " + getErrorString(precise, estimate));
        }
        assertEquals(true, true);
    }

    @Test
    public void testMemoryConsumption() {
        int numOfItems = 10000000;

        println("#items_inserted, HLL_ser_size, openHashSet_ser_size, ser_size_ratio, estimate, precise, error");
        for (int i = 0; i < numOfItems; i+=numOfItems/17) {
            if (i == 0) {
                continue;
            }
            RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
            HyperLogLog hllResult = new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_BIT_SIZE);
            IntOpenHashSet set = new IntOpenHashSet();
            arr.offerAllNumberTo(hllResult);
            arr.offerAllNumberTo(set);
            int hllSize = getSerializedSize(hllResult);
            int setSize = getSerializedSize(set);
            long estimate = hllResult.cardinality();
            long precise = set.size();
            println(i + ", " + hllSize + ", " + setSize + ", " + (hllSize + 0.0) / (setSize + 0.0) + ", "
                    + estimate + ", " + precise + ", " + getErrorString(precise, estimate));
        }
        assertEquals(true, true);
    }

    // ------------ helper functions ------------

    private void println(String s) {
        System.out.println(s);
    }

    private String getErrorString(long precise, long estimate) {
        return Math.abs(precise - estimate + 0.0) /precise*100 + "%";
    }

    private int getSerializedSize(Serializable ser) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(ser);
            oos.close();
            return baos.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static List<Serializable> getHLLResultValues(int numberOfElements) {
        List<Serializable> hllResultList = new ArrayList<Serializable>();
        for (int i = 0; i < numberOfElements; ++i) {
            HyperLogLog hllResult = new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_BIT_SIZE);
            hllResult.offer(i);
            hllResultList.add(hllResult);
        }
        return hllResultList;
    }

    private static List<Serializable> getHLLRandomResultValues(RandomNumberArray arr, int numOfListCombined, int numOfItemsPerList) {
        List<Serializable> hllResultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            HyperLogLog hllResult = new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_BIT_SIZE);
            arr.offerNumberInRangeTo(hllResult, i*numOfItemsPerList, (i+1)*numOfItemsPerList);
            hllResultList.add(hllResult);
        }
        return hllResultList;
    }

    private static List<Serializable> getIntOpenHashSets(RandomNumberArray arr, int numberOfListCombined, int numOfItemsPerList) {
        List<Serializable> intOpenHashSets = new ArrayList<Serializable>();
        for (int i = 0; i < numberOfListCombined; ++i) {
            IntOpenHashSet intOpenHashSet = new IntOpenHashSet();
            arr.offerNumberInRangeTo(intOpenHashSet, i*numOfItemsPerList, (i+1)*numOfItemsPerList);
            intOpenHashSets.add(intOpenHashSet);
        }
        return intOpenHashSets;
    }
}
