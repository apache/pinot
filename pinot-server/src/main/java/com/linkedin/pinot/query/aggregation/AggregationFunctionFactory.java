package com.linkedin.pinot.query.aggregation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.query.aggregation.function.CountAggregationFunction;
import com.linkedin.pinot.query.aggregation.function.MaxAggregationFunction;
import com.linkedin.pinot.query.aggregation.function.MinAggregationFunction;
import com.linkedin.pinot.query.aggregation.function.SumAggregationFunction;
import com.linkedin.pinot.query.aggregation.function.SumDoubleAggregationFunction;
import com.linkedin.pinot.query.request.AggregationInfo;


public class AggregationFunctionFactory {

  private static Map<String, Class<? extends AggregationFunction>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends AggregationFunction>>();

  static {
    keyToFunction.put("count", CountAggregationFunction.class);
    keyToFunction.put("max", MaxAggregationFunction.class);
    keyToFunction.put("min", MinAggregationFunction.class);
    keyToFunction.put("sum", SumAggregationFunction.class);
    keyToFunction.put("sumlong", SumAggregationFunction.class);
    keyToFunction.put("sumdouble", SumDoubleAggregationFunction.class);
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

  public static AggregationFunction get(AggregationInfo aggregationInfo) {
    try {
      String aggregationKey = aggregationInfo.getAggregationType();
      AggregationFunction aggregationFunction = get(aggregationKey);
      aggregationFunction.init(aggregationInfo);
      return aggregationFunction;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
