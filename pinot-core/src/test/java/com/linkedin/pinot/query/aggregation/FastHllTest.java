/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.FastHllAggregationFunction;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import com.linkedin.pinot.util.TestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit Test for FastHll Aggregation Function
 */
public class FastHllTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastHllTest.class);

  public static String _columnName = "met";
  public static AggregationInfo _paramsInfo;

  @BeforeClass
  public static void setup() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", _columnName);
    _paramsInfo = new AggregationInfo();
    _paramsInfo.setAggregationType("");
    _paramsInfo.setAggregationParams(params);
  }

  @Test
  public void testFastHllAggregation() {
    AggregationFunction aggregationFunction = new FastHllAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test combine
    int _sizeOfCombineList = 1000;
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<HyperLogLog> aggregationResults = getHllResultValues(i);
      List<HyperLogLog> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      long estimate = ((combinedResult.get(0))).cardinality();
      TestUtils.assertApproximation(estimate, i, 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<HyperLogLog> combinedResults = getHllResultValues(i);
      long reduceSize = (Long) aggregationFunction.reduce(combinedResults);
      TestUtils.assertApproximation(reduceSize, i, 0.1);
    }
  }

  private static List<HyperLogLog> getHllResultValues(int numberOfElements) {
    List<HyperLogLog> hllResultList = new ArrayList<>();
    for (int i = 0; i < numberOfElements; ++i) {
      HyperLogLog hllResult = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
      hllResult.offer(i);
      hllResultList.add(hllResult);
    }
    return hllResultList;
  }

}
