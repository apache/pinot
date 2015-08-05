package com.linkedin.pinot.core.query.aggregation.function.quantile;

import com.linkedin.pinot.core.query.aggregation.AggregationFunctionRegistry;

/**
 * register function in {@link AggregationFunctionRegistry}
 */
public class Quantile90 extends TDigestFunction {
    public Quantile90() {
        super((byte) 90);
    }
}