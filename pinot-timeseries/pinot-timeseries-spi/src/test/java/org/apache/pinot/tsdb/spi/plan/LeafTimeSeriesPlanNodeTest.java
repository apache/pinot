package org.apache.pinot.tsdb.spi.plan;

import java.time.Duration;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LeafTimeSeriesPlanNodeTest {
  @Test
  public void testGetEffectiveFilter() {
    LeafTimeSeriesPlanNode planNode = new LeafTimeSeriesPlanNode();
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(
        1000, Duration.ofSeconds(13), 9);
    planNode.getEffectiveFilter(timeBuckets);
  }
}
