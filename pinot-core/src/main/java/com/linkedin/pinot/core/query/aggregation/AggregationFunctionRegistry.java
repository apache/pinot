package com.linkedin.pinot.core.query.aggregation;

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


/**
 * Aggregation function registry, that is used to register aggregation functions with the nicknames, so that it can be easily referred
 *
 */
@SuppressWarnings("rawtypes")
public class AggregationFunctionRegistry {

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
      throw new RuntimeException(ex);
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
      throw new RuntimeException(ex);
    }
  }

}
