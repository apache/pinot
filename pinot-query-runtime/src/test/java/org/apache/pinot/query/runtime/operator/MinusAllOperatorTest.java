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
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MinusAllOperatorTest {
  private AutoCloseable _mocks;

  @Mock
  private MultiStageOperator _leftOperator;

  @Mock
  private MultiStageOperator _rightOperator;

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
  public void testExceptAllOperator() {
    DataSchema schema = new DataSchema(new String[]{"int_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(schema, new Object[]{1}, new Object[]{2}, new Object[]{5}))
        .thenReturn(SuccessMseBlock.INSTANCE);

    MinusAllOperator minusOperator =
        new MinusAllOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(_leftOperator, _rightOperator),
            schema);

    MseBlock result = minusOperator.nextBlock();
    while (result.isEos()) {
      result = minusOperator.nextBlock();
    }
    List<Object[]> resultRows = ((MseBlock.Data) result).asRowHeap().getRows();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{3}, new Object[]{4});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < resultRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @Test
  public void testExceptAllOperatorWithDups() {
    DataSchema schema = new DataSchema(new String[]{"int_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(schema, new Object[]{1}, new Object[]{2}, new Object[]{2}, new Object[]{2},
            new Object[]{3}, new Object[]{3}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(schema, new Object[]{2}, new Object[]{3}, new Object[]{3}, new Object[]{4}))
        .thenReturn(SuccessMseBlock.INSTANCE);

    MinusAllOperator minusOperator =
        new MinusAllOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(_leftOperator, _rightOperator),
            schema);

    MseBlock result = minusOperator.nextBlock();
    while (result.isEos()) {
      result = minusOperator.nextBlock();
    }
    List<Object[]> resultRows = ((MseBlock.Data) result).asRowHeap().getRows();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1}, new Object[]{2}, new Object[]{2}, new Object[]{3});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < resultRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }
}
