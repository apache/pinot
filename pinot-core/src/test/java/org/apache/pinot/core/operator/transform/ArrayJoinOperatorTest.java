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
package org.apache.pinot.core.operator.transform;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.ArrayJoinType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.DocIdOrderedOperator.DocIdOrder;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.ArrayJoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


public class ArrayJoinOperatorTest {

  @Test
  public void testDelegatesToInputOperator() {
    ArrayJoinContext arrayJoinContext =
        new ArrayJoinContext(ArrayJoinType.INNER,
            List.of(new ArrayJoinContext.Operand(ExpressionContext.forIdentifier("mvRawCol1"), null)));
    QueryContext queryContext = new QueryContext.Builder()
        .setTableName("testTable")
        .setSelectExpressions(List.of(ExpressionContext.forIdentifier("mvRawCol1")))
        .setArrayJoinContexts(List.of(arrayJoinContext))
        .build();

    TestValueBlock valueBlock = new TestValueBlock();
    TestProjectOperator projectOperator = new TestProjectOperator(valueBlock);
    ArrayJoinOperator arrayJoinOperator = new ArrayJoinOperator(queryContext, projectOperator);

    assertSame(arrayJoinOperator.nextBlock(), valueBlock);
    assertSame(arrayJoinOperator.getArrayJoinContexts(), queryContext.getArrayJoinContexts());
    assertEquals(arrayJoinOperator.toExplainString(), "ARRAY_JOIN");
    assertSame(arrayJoinOperator.withOrder(DocIdOrder.ASC), arrayJoinOperator);
  }

  private static class TestProjectOperator extends BaseProjectOperator<ValueBlock> {
    private final ValueBlock _valueBlock;
    private boolean _returnedBlock;

    TestProjectOperator(ValueBlock valueBlock) {
      _valueBlock = valueBlock;
    }

    @Override
    protected ValueBlock getNextBlock() {
      if (_returnedBlock) {
        return null;
      }
      _returnedBlock = true;
      return _valueBlock;
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Override
    public String toExplainString() {
      return "TEST_PROJECT";
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }

    @Override
    public boolean isCompatibleWith(DocIdOrder order) {
      return true;
    }

    @Override
    public Map<String, ColumnContext> getSourceColumnContextMap() {
      return Collections.emptyMap();
    }

    @Override
    public ColumnContext getResultColumnContext(ExpressionContext expression) {
      throw new UnsupportedOperationException("Not required for this test");
    }

    @Override
    public BaseProjectOperator<ValueBlock> withOrder(DocIdOrder newOrder) {
      return this;
    }
  }

  private static class TestValueBlock implements ValueBlock {

    @Override
    public int getNumDocs() {
      return 0;
    }

    @Override
    public int[] getDocIds() {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      return null;
    }
  }
}
