package com.linkedin.pinot.core.query.aggregation.function.quantile;

import com.linkedin.pinot.core.query.aggregation.AggregationFunctionRegistry;

/**
 * register function in {@link AggregationFunctionRegistry}
 */
public class QuantileAccurate50 extends QuantileAccurateFunction {
    public QuantileAccurate50() {
        super((byte) 50);
    }
}