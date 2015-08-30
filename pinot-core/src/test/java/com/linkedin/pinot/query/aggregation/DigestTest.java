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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.quantile.*;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.DigestAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.util.TestUtils;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

import static org.testng.Assert.assertEquals;

/**
 *
 * Tests for {@link QuantileDigest} estimation, the routine is similar to {@link SimpleAggregationFunctionsTest}
 *
 *
 */
public class DigestTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DigestTest.class);
    private static final double maxError = 0.05;
    private static final HashMap<Byte, DigestAggregationFunction> functionMap = new HashMap<Byte, DigestAggregationFunction>();
    private static final HashMap<Byte, PercentileAggregationFunction> accurateFunctionMap = new HashMap<Byte, PercentileAggregationFunction>();

    public static int[] _docIdsArray;
    public static IntArray _docIds;
    public static int _sizeOfDocIdArray = 5000;
    public static String _columnName = "met";
    public static AggregationInfo _paramsInfo;

    private static final double threshold = maxError + 0.01;  // Precision threshold, can be set to smaller value.

    /**
     * This does not mean too much sense here, but we fix it to a small number.
     */
    private static final int DUPLICATION_PER_ITEM = 10;

    static {
        functionMap.put((byte) 50, new Percentileest50());
        functionMap.put((byte) 90, new Percentileest90());
        functionMap.put((byte) 95, new Percentileest95());

        accurateFunctionMap.put((byte) 50, new PercentileAggregationFunction((byte) 50));
        accurateFunctionMap.put((byte) 90, new PercentileAggregationFunction((byte) 90));
        accurateFunctionMap.put((byte) 95, new PercentileAggregationFunction((byte) 95));
    }

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
        // For test purpose, we fix the random seeds.
        // It should also work for arbitrary seeds like System.currentTimeMillis()
        private static Random _rnd = new Random(0L);

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
            Collections.shuffle(lst, new Random(10L));
            // toIntArray
            arr = lst.toArray(new Integer[0]);
            if (arr.length != size) {
                throw new RuntimeException("should not happen");
            }
        }

        public int size() {
            return arr.length;
        }

        public void offerAllNumberTo(QuantileDigest digest) {
            offerNumberInRangeTo(digest, 0, arr.length);
        }

        public void offerAllNumberTo(DoubleArrayList list) {
            offerNumberInRangeTo(list, 0, arr.length);
        }

        public void offerNumberInRangeTo(QuantileDigest tDigest, int start, int end) {
            end = Math.min(end, arr.length);
            for (int i = start; i < end; i++) {
                tDigest.offer(arr[i]);
            }
        }

        public void offerNumberInRangeTo(DoubleArrayList list, int start, int end) {
            end = Math.min(end, arr.length);
            for (int i = start; i < end; i++) {
                list.add(arr[i]);
            }
        }
    }

    @Test
    public void testCombineReduce() {
        for (Byte percentile: functionMap.keySet()) {
            LOGGER.info("[Test Percentile " + percentile + " (combine)]");
            AggregationFunction aggregationFunction = functionMap.get(percentile);
            AggregationFunction aggregationAccurateFunction = accurateFunctionMap.get(percentile);
            aggregationFunction.init(_paramsInfo);
            aggregationAccurateFunction.init(_paramsInfo);

            // Test combine
            int _sizeOfCombineList = 1000;
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            for (int i = 1; i <= _sizeOfCombineList; ++i) {
                List<Serializable> aggregationResults = getQuantileDigestResultValues(i);
                List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
                long estimate = ((QuantileDigest) (combinedResult.get(0))).getQuantile(((double)percentile)/100);

                List<Serializable> aggregationResults2 = getDoubleArrayListResultValues(i);
                List<Serializable> combinedResult2 = aggregationAccurateFunction.combine(aggregationResults2, CombineLevel.SEGMENT);
                long actual = (long) PercentileUtil.getValueOnPercentile((DoubleArrayList) combinedResult2.get(0), percentile);

                TestUtils.assertApproximation(estimate, actual, threshold);
                sb1.append((int)estimate + ", ");
                sb2.append(i + ", ");
            }

            // Test reduce
            LOGGER.info("[Test Percentile " + percentile + " (reduce)]");
            for (int i = 1; i <= _sizeOfCombineList; ++i) {
              List<Serializable> combinedResults = getQuantileDigestResultValues(i);
              List<Serializable> combinedResults2 = getDoubleArrayListResultValues(i);
              long estimate = (long) aggregationFunction.reduce(combinedResults);
              double actual = (double) aggregationAccurateFunction.reduce(combinedResults2);
              TestUtils.assertApproximation(estimate, actual, threshold);
            }
        }
    }

    @Test
    public void testLargeCombineList() {
        for (Byte percentile: functionMap.keySet()) {
            LOGGER.info("[Test Percentile " + percentile + "]");
            AggregationFunction aggregationFunction = functionMap.get(percentile);
            AggregationFunction aggregationAccurateFunction = accurateFunctionMap.get(percentile);
            aggregationFunction.init(_paramsInfo);
            aggregationAccurateFunction.init(_paramsInfo);

            // Test combine
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            StringBuilder sb3 = new StringBuilder();

            // Make combine list number fixed to 10, each list has large number of elements
            int maxSize = 100000; // 10000000
            for (int i = 1; i <= maxSize; i += maxSize / 17) {
                if (i == 1) continue;
                RandomNumberArray arr = new RandomNumberArray(i * 10, 1);
                long t1 = System.nanoTime();
                List<Serializable> aggregationResults = getQuantileDigestResultValues(arr, 10, i);
                long t2 = System.nanoTime();
                List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
                long t3 = System.nanoTime();
                long estimate = ((QuantileDigest) (combinedResult.get(0))).getQuantile(((double)percentile)/100);

                List<Serializable> aggregationResults2 = getDoubleArrayListResultValues(arr, 10, i);
                List<Serializable> combinedResult2 = aggregationAccurateFunction.combine(aggregationResults2, CombineLevel.SEGMENT);
                long actual = (long) PercentileUtil.getValueOnPercentile((DoubleArrayList) combinedResult2.get(0), percentile);

                println(i + ", " + "" + (t2 - t1) + "" + ", " + (t3 - t2) + ", " + getErrorString(actual, estimate));
                TestUtils.assertApproximation(estimate, actual, threshold);
            }
        }
    }

    @Test
    public void testRandomAggregationCombine() {
        final int numOfItemsPerList = 100;
        final int numOfListCombined = 1000;

        for (Byte percentile: functionMap.keySet()) {
            LOGGER.info("[Test Percentile " + percentile + "]");
            AggregationFunction aggregationFunction = functionMap.get(percentile);
            AggregationFunction aggregationAccurateFunction = accurateFunctionMap.get(percentile);
            aggregationFunction.init(_paramsInfo);
            aggregationAccurateFunction.init(_paramsInfo);

            println("#list_combined, QuantileDigest_time(nano), DoubleArrayList_time(nano), time_ratio, estimate, precise, error");

            // Test combine
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            for (int i = 1; i <= numOfListCombined; i += numOfListCombined / 17) {
                if (i == 1) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i * numOfItemsPerList, DUPLICATION_PER_ITEM);

                List<Serializable> aggregationResults = getQuantileDigestResultValues(arr, i, numOfItemsPerList);
                long t1 = System.nanoTime();
                List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
                long estimate = ((QuantileDigest) (combinedResult.get(0))).getQuantile(((double)percentile)/100);
                long t2 = System.nanoTime();

                List<Serializable> aggregationResults2 = getDoubleArrayListResultValues(arr, i, numOfItemsPerList);
                long t3 = System.nanoTime();
                List<Serializable> combinedResult2 = aggregationAccurateFunction.combine(aggregationResults2, CombineLevel.SEGMENT);
                long actual = (long) PercentileUtil.getValueOnPercentile((DoubleArrayList) combinedResult2.get(0), percentile);
                long t4 = System.nanoTime();

                println(i + ", " + (t2 - t1) + ", " + (t4 - t3) + ", " + (t2 - t1 + 0.0) / (t4 - t3 + 0.0) + ", "
                        + estimate + ", " + actual + ", " + getErrorString(actual, estimate));
                TestUtils.assertApproximation(estimate, actual, threshold);
            }
        }
    }

    @Test
    public void testInsertionTime() {
        int numOfItems = 1000000;

        for (Byte percentile: functionMap.keySet()) {
            LOGGER.info("[Test Percentile " + percentile + "]");
            println("#items_inserted, QuantileDigest_time(nano), DoubleArrayList_time(nano), time_ratio, estimate, precise, error");
            for (int i = 0; i < numOfItems; i += numOfItems / 17) {
                if (i == 0) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
                QuantileDigest digest = new QuantileDigest(maxError);
                DoubleArrayList list = new DoubleArrayList();
                long t1 = System.nanoTime();
                arr.offerAllNumberTo(digest);
                long estimate = digest.getQuantile(((double)percentile)/100);
                long t2 = System.nanoTime();
                arr.offerAllNumberTo(list);
                long actual = (long) PercentileUtil.getValueOnPercentile(list, percentile);
                long t3 = System.nanoTime();

                println(i + ", " + "" + (t2 - t1) + ", " + (t3 - t2) + ", " + (t2 - t1 + 0.0) / (t3 - t2 + 0.0) + ", "
                        + estimate + ", " + actual + ", " + getErrorString(actual, estimate));
            }
        }

        assertEquals(true, true);
    }

    @Test
    public void testMemoryConsumption() {
        int numOfItems = 1000000;

        for (Byte percentile: functionMap.keySet()) {
            LOGGER.info("[Test Percentile " + percentile + "]");
            println("#items_inserted, QuantileDigest_ser_size, DoubleArrayList_ser_size, ser_size_ratio, estimate, precise, error");
            for (int i = 0; i < numOfItems; i += numOfItems / 17) {
                if (i == 0) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
                QuantileDigest digest = new QuantileDigest(maxError);
                DoubleArrayList list = new DoubleArrayList();
                arr.offerAllNumberTo(digest);
                arr.offerAllNumberTo(list);
                int digestSize = getSerializedSize(digest);
                int listSize = getSerializedSize(list);

                long estimate = digest.getQuantile(((double)percentile)/100);
                long actual = (long) PercentileUtil.getValueOnPercentile(list, percentile);

                println(i + ", " + digestSize + ", " + listSize + ", " + (digestSize + 0.0) / (listSize + 0.0) + ", "
                        + estimate + ", " + actual + ", " + getErrorString(actual, estimate));
            }
        }

        assertEquals(true, true);
    }

    @Test
    public void testSerialization() {
        int numOfItems = 10000;

        for (Byte percentile: functionMap.keySet()) {
            LOGGER.info("[Test Percentile " + percentile + "]");
            for (int i = 0; i < numOfItems; i += numOfItems / 17) {
                if (i == 0) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
                QuantileDigest digest = new QuantileDigest(maxError);
                DoubleArrayList list = new DoubleArrayList();
                arr.offerAllNumberTo(digest);
                arr.offerAllNumberTo(list);
                // write and read
                byte[] bytes = serialize(digest);
                QuantileDigest digest2 = deserialize(bytes);

                long estimate = digest.getQuantile((percentile+0.0)/100);
                long estimate2 = digest2.getQuantile((percentile+0.0)/100);
                long actual = (long) PercentileUtil.getValueOnPercentile(list, percentile);

                println("[Before Serialization Estimate]: " + estimate);
                println("[After Serialization Estimate]: " + estimate2);
            }
        }

        assertEquals(true, true);
    }

    // ------------ helper functions ------------

    private void println(String s) {
        System.out.println(s);
    }

    private String getErrorString(double precise, double estimate) {
        if (precise != 0) {
            return Math.abs((precise - estimate + 0.0) / precise) * 100 + "%";
        } else {
            return "precise: " + precise + " estimate: " + estimate;
        }
    }

    private int getSerializedSize(Serializable ser) {
        return serialize(ser).length;
    }

    private byte[] serialize(Serializable ser) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(ser);
            oos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private QuantileDigest deserialize(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bais);
            QuantileDigest digest = (QuantileDigest) ois.readObject();
            ois.close();
            return digest;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    // function set 1
    private static List<Serializable> getQuantileDigestResultValues(int numOfListCombined) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            QuantileDigest digest = new QuantileDigest(maxError);
            digest.offer(i);
            resultList.add(digest);
        }
        return resultList;
    }

    private static List<Serializable> getDoubleArrayListResultValues(int numOfListCombined) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            DoubleArrayList list = new DoubleArrayList();
            list.add(i);
            resultList.add(list);
        }
        return resultList;
    }

    // function set 2
    private static List<Serializable> getQuantileDigestResultValues(RandomNumberArray arr, int numOfListCombined, int numOfItemsPerList) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            QuantileDigest digest = new QuantileDigest(maxError);
            arr.offerNumberInRangeTo(digest, i*numOfItemsPerList, (i+1)*numOfItemsPerList);
            resultList.add(digest);
        }
        return resultList;
    }

    private static List<Serializable> getDoubleArrayListResultValues(RandomNumberArray arr, int numberOfListCombined, int numOfItemsPerList) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numberOfListCombined; ++i) {
            DoubleArrayList list = new DoubleArrayList();
            arr.offerNumberInRangeTo(list, i*numOfItemsPerList, (i+1)*numOfItemsPerList);
            resultList.add(list);
        }
        return resultList;
    }

    // others
    private DigestAggregationFunction getQuantileAggregationFunction(byte quantile) {
        DigestAggregationFunction ret = functionMap.get(quantile);
        if (ret != null) {
            return ret;
        }
        throw new RuntimeException("Quantile: " + quantile + " not supported!");
    }
}
