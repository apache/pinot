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
package com.linkedin.pinot.core.query.aggregation;

import com.linkedin.pinot.common.Utils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationNoDictionaryFunction;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.DistinctCountAggregationNoDictionaryFunction;
import com.linkedin.pinot.core.query.aggregation.function.MaxAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MaxAggregationNoDictionaryFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinAggregationNoDictionaryFunction;
import com.linkedin.pinot.core.query.aggregation.function.SumAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.SumAggregationNoDictionaryFunction;
import com.linkedin.pinot.core.query.aggregation.function.DistinctCountHLLAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregation function registry, that is used to register aggregation functions with the nicknames, so that it can be easily referred
 *
 */
@SuppressWarnings("rawtypes")
public class AggregationFunctionRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationFunctionRegistry.class);

  private static Map<String, Class<? extends AggregationFunction>> keyToFunctionWithDictionary =
      new ConcurrentHashMap<String, Class<? extends AggregationFunction>>();

  private static Map<String, Class<? extends AggregationFunction>> keyToFunctionWithoutDictionary =
      new ConcurrentHashMap<String, Class<? extends AggregationFunction>>();

  static {
    keyToFunctionWithDictionary.put("count", CountAggregationFunction.class);
    keyToFunctionWithDictionary.put("max", MaxAggregationFunction.class);
    keyToFunctionWithDictionary.put("min", MinAggregationFunction.class);
    keyToFunctionWithDictionary.put("sum", SumAggregationFunction.class);
    keyToFunctionWithDictionary.put("avg", AvgAggregationFunction.class);
    keyToFunctionWithDictionary.put("distinctcount", DistinctCountAggregationFunction.class);
    keyToFunctionWithDictionary.put("distinctcounthll", DistinctCountHLLAggregationFunction.class);
    // quantiles
    keyToFunctionWithDictionary.put("percentileest50", Percentileest50.class);
    keyToFunctionWithDictionary.put("percentileest90", Percentileest90.class);
    keyToFunctionWithDictionary.put("percentileest95", Percentileest95.class);
    keyToFunctionWithDictionary.put("percentileest99", Percentileest99.class);
    keyToFunctionWithDictionary.put("percentile50", Percentile50.class);
    keyToFunctionWithDictionary.put("percentile90", Percentile90.class);
    keyToFunctionWithDictionary.put("percentile95", Percentile95.class);
    keyToFunctionWithDictionary.put("percentile99", Percentile99.class);
  }

  static {
    keyToFunctionWithoutDictionary.put("max", MaxAggregationNoDictionaryFunction.class);
    keyToFunctionWithoutDictionary.put("min", MinAggregationNoDictionaryFunction.class);
    keyToFunctionWithoutDictionary.put("sum", SumAggregationNoDictionaryFunction.class);
    keyToFunctionWithoutDictionary.put("avg", AvgAggregationNoDictionaryFunction.class);
    keyToFunctionWithoutDictionary.put("distinctcount", DistinctCountAggregationNoDictionaryFunction.class);
  }

  public static void register(String aggregationKey, Class<? extends AggregationFunction> aggregationFunction) {
    keyToFunctionWithDictionary.put(aggregationKey, aggregationFunction);
  }

  public static boolean contains(String column) {
    return keyToFunctionWithDictionary.containsKey(column);
  }

  @SuppressWarnings("unchecked")
  public static AggregationFunction get(String aggregationKey) {
    try {
      Class<? extends AggregationFunction> cls = keyToFunctionWithDictionary.get(aggregationKey.toLowerCase());
      if (cls != null) {
        return cls.newInstance();
      }
      cls = (Class<? extends AggregationFunction>) Class.forName(aggregationKey);
      keyToFunctionWithDictionary.put(aggregationKey, cls);
      return cls.newInstance();
    } catch (Exception ex) {
      LOGGER.error("Caught exception while getting aggregation function", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    }
  }

  @SuppressWarnings("unchecked")
  public static AggregationFunction getAggregationNoDictionaryFunction(String aggregationKey) {
    try {
      Class<? extends AggregationFunction> cls = keyToFunctionWithoutDictionary.get(aggregationKey.toLowerCase());
      if (cls != null) {
        return cls.newInstance();
      }
      cls = (Class<? extends AggregationFunction>) Class.forName(aggregationKey);
      keyToFunctionWithoutDictionary.put(aggregationKey, cls);
      return cls.newInstance();
    } catch (Exception ex) {
      LOGGER.error("Caught exception while getting aggregation function", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    }
  }

}
