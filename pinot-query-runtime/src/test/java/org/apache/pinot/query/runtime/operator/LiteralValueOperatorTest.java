/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class LiteralValueOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldReturnLiteralBlock() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sLiteral", "iLiteral"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    List<List<RexExpression.Literal>> literalRows = List.of(
        List.of(new RexExpression.Literal(ColumnDataType.STRING, "foo"),
            new RexExpression.Literal(ColumnDataType.INT, 1)),
        List.of(new RexExpression.Literal(ColumnDataType.STRING, ""),
            new RexExpression.Literal(ColumnDataType.INT, 2)));
    LiteralValueOperator operator = getOperator(schema, literalRows);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{"foo", 1});
    assertEquals(resultRows.get(1), new Object[]{"", 2});
    assertTrue(operator.nextBlock().isSuccess(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldHandleEmptyLiteralRows() {
    // Given:
    LiteralValueOperator operator =
        getOperator(new DataSchema(new String[0], new ColumnDataType[0]), List.of(List.of()));

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{});
    assertTrue(operator.nextBlock().isSuccess());
  }

  private static LiteralValueOperator getOperator(DataSchema schema, List<List<RexExpression.Literal>> literalRows) {
    return new LiteralValueOperator(OperatorTestUtil.getTracingContext(),
        new ValueNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), literalRows));
  }
}
