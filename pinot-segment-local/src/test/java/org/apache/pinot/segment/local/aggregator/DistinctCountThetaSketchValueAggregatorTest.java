package org.apache.pinot.segment.local.aggregator;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DistinctCountThetaSketchValueAggregatorTest {


    @Test
    public void shouldCreateSingleItemSketch() {
        DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
        assertEquals(
                agg.getInitialAggregatedValue("hello world").getEstimate(),
                1
        );
    }
}
