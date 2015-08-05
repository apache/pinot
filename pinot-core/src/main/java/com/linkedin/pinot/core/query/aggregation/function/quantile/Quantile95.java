package com.linkedin.pinot.core.query.aggregation.function.quantile;

import com.linkedin.pinot.core.query.aggregation.AggregationFunctionRegistry;

/**
 * register function in {@link AggregationFunctionRegistry}
 */
public class Quantile95 extends TDigestFunction {
    public Quantile95() {
        super((byte) 95);
    }
}