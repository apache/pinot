package com.linkedin.pinot.core.query.aggregation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MaxAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.SumAggregationFunction;


/**
 * Aggregation function registry, that is used to register aggregation functions with the nicknames, so that it can be easily referred
 *
 */
@SuppressWarnings("rawtypes")
public class AggregationFunctionRegistry {

  private static Map<String, Class<? extends AggregationFunction>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends AggregationFunction>>();

  static {
    keyToFunction.put("count", CountAggregationFunction.class);
    keyToFunction.put("max", MaxAggregationFunction.class);
    keyToFunction.put("min", MinAggregationFunction.class);
    keyToFunction.put("sum", SumAggregationFunction.class);
    keyToFunction.put("avg", AvgAggregationFunction.class);
    keyToFunction.put("distinctcount", DistinctCountAggregationFunction.class);
  }

  public static void register(String aggregationKey, Class<? extends AggregationFunction> aggregationFunction) {
    keyToFunction.put(aggregationKey, aggregationFunction);
  }

  public static boolean contains(String column) {
    return keyToFunction.containsKey(column);
  }

  @SuppressWarnings("unchecked")
  public static AggregationFunction get(String aggregationKey) {
    try {
      Class<? extends AggregationFunction> cls = keyToFunction.get(aggregationKey.toLowerCase());
      if (cls != null) {
        return cls.newInstance();
      }
      cls = (Class<? extends AggregationFunction>) Class.forName(aggregationKey);
      keyToFunction.put(aggregationKey, cls);
      return cls.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
