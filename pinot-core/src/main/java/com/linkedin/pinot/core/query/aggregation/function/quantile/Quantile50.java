package com.linkedin.pinot.core.query.aggregation.function.quantile;

import com.linkedin.pinot.core.query.aggregation.AggregationFunctionRegistry;

/**
 * register function in {@link AggregationFunctionRegistry}
 */
public class Quantile50 extends TDigestFunction {
    public Quantile50() {
        super((byte) 50);
    }
}