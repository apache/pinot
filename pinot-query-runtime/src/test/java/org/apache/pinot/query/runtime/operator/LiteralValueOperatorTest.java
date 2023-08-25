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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LiteralValueOperatorTest {

  private AutoCloseable _mocks;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
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
    List<List<RexExpression>> literals = ImmutableList.of(
        ImmutableList.of(new RexExpression.Literal(DataType.STRING, "foo"), new RexExpression.Literal(DataType.INT, 1)),
        ImmutableList.of(new RexExpression.Literal(DataType.STRING, ""), new RexExpression.Literal(DataType.INT, 2)));
    LiteralValueOperator operator = new LiteralValueOperator(OperatorTestUtil.getDefaultContext(), schema, literals);

    // When:
    TransferableBlock transferableBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(transferableBlock.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(transferableBlock.getContainer().get(1), new Object[]{"", 2});
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldHandleEmptyLiteralRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{}, new ColumnDataType[]{});
    List<List<RexExpression>> literals = ImmutableList.of(ImmutableList.of());
    LiteralValueOperator operator = new LiteralValueOperator(OperatorTestUtil.getDefaultContext(), schema, literals);

    // When:
    TransferableBlock transferableBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(transferableBlock.getContainer().get(0), new Object[]{});
  }
}
