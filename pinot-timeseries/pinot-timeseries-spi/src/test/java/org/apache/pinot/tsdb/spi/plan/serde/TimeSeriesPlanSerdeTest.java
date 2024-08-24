package org.apache.pinot.tsdb.spi.plan.serde;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.ScanFilterAndProjectPlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesPlanSerdeTest {
  @Test
  public void testFoo() {
    ScanFilterAndProjectPlanNode scanFilterAndProjectPlanNode = new ScanFilterAndProjectPlanNode(
        "sfp#0", new ArrayList<>(), "myTable", "myTimeColumn", TimeUnit.MILLISECONDS,
        0L, "myFilterExpression", "myValueExpression",
        new AggInfo("SUM"), new ArrayList<>()
    );
    BaseTimeSeriesPlanNode planNode =
        TimeSeriesPlanSerde.deserialize(TimeSeriesPlanSerde.serialize(scanFilterAndProjectPlanNode));
    assertTrue(planNode instanceof ScanFilterAndProjectPlanNode);
    ScanFilterAndProjectPlanNode deserializedNode = (ScanFilterAndProjectPlanNode) planNode;
    assertEquals(deserializedNode.getTableName(), "myTable");
    assertEquals(deserializedNode.getTimeColumn(), "myTimeColumn");
    assertEquals(deserializedNode.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(deserializedNode.getOffset(), 0L);
    assertEquals(deserializedNode.getFilterExpression(), "myFilterExpression");
    assertEquals(deserializedNode.getValueExpression(), "myValueExpression");
    assertNull(deserializedNode.getAggInfo());
    assertEquals(deserializedNode.getGroupByColumns().size(), 0);
  }
}
