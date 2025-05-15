package org.apache.pinot.query.planner.physical.v2.nodes;

import java.util.List;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PhysicalExchangeTest {
  @Test
  public void testGetRelDistribution() {
    List<Integer> keys = List.of(1);
    for (ExchangeStrategy strategy : ExchangeStrategy.values()) {
      assertNotNull(PhysicalExchange.getRelDistribution(strategy, keys));
    }
  }
}
