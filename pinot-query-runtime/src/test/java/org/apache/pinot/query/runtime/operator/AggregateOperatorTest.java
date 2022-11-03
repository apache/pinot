package org.apache.pinot.query.runtime.operator;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.data.FieldSpec;
import org.hamcrest.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;


public class AggregateOperatorTest {
  @Mock
  Operator<TransferableBlock> _upstreamOperator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testGroupByAggregateWithHashCollision(){
    // "Aa" and "BB" have same hash code in java.
    List<Object[]> rows = Arrays.asList(new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"});
    when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.getRowDataBlock(rows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());
    // Create an aggregation call with sum for first column and group by second column.
    RexExpression.FunctionCall agg = new RexExpression.FunctionCall(SqlKind.SUM,
        FieldSpec.DataType.INT, "SUM", Arrays.asList(new RexExpression.InputRef(0)));
    AggregateOperator sum0GroupBy1= new AggregateOperator( _upstreamOperator,  OperatorTestUtil.TEST_DATA_SCHEMA,
        Arrays.asList(agg), Arrays.asList(new RexExpression.InputRef(1)));
    TransferableBlock result = sum0GroupBy1.getNextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{"BB", 5.0}, new Object[]{"Aa", 1});
    assertThat(expectedRows, Matchers.containsInAnyOrder(resultRows.toArray()));
  }
}
