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
package org.apache.pinot.query.runtime.operator.set;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.BlockListMultiStageOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MinusOperatorTest {

  @Test
  public void testExceptOperator() {
    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    MultiStageOperator leftOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .addRow(3, "CC")
        .addRow(4, "DD")
        .buildWithEos();
    MultiStageOperator rightOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .addRow(5, "EE")
        .buildWithEos();

    MinusOperator minusOperator =
        new MinusOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(leftOperator, rightOperator),
            schema);

    MseBlock result = minusOperator.nextBlock();
    while (result.isEos()) {
      result = minusOperator.nextBlock();
    }
    List<Object[]> resultRows = ((MseBlock.Data) result).asRowHeap().getRows();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{3, "CC"}, new Object[]{4, "DD"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < resultRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @Test
  public void testDedup() {
    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    MultiStageOperator leftOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .addRow(3, "CC")
        .addRow(4, "DD")
        .addRow(1, "AA")
        .addRow(2, "BB")
        .addRow(3, "CC")
        .addRow(4, "DD")
        .buildWithEos();
    MultiStageOperator rightOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .addRow(5, "EE")
        .addRow(1, "AA")
        .addRow(2, "BB")
        .addRow(5, "EE")
        .buildWithEos();

    MinusOperator minusOperator =
        new MinusOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(leftOperator, rightOperator),
            schema);

    MseBlock result = minusOperator.nextBlock();
    while (result.isEos()) {
      result = minusOperator.nextBlock();
    }
    List<Object[]> resultRows = ((MseBlock.Data) result).asRowHeap().getRows();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{3, "CC"}, new Object[]{4, "DD"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < resultRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @Test
  public void testErrorBlockRightChild() {
    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    MultiStageOperator leftOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator rightOperator = new BlockListMultiStageOperator.Builder(schema)
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("Error in right operator")));

    MinusOperator minusOperator =
        new MinusOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(leftOperator, rightOperator),
            schema);
    MseBlock result = minusOperator.nextBlock();
    // Keep calling nextBlock until we get an EoS block
    while (!result.isEos()) {
      result = minusOperator.nextBlock();
    }
    Assert.assertTrue(result.isError());
  }

  @Test
  public void testErrorBlockLeftChild() {
    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    MultiStageOperator leftOperator = new BlockListMultiStageOperator.Builder(schema)
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("Error in left operator")));
    MultiStageOperator rightOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(3, "aa")
        .addRow(4, "bb")
        .buildWithEos();

    MinusOperator minusOperator =
        new MinusOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(leftOperator, rightOperator),
            schema);
    MseBlock result = minusOperator.nextBlock();
    // Keep calling nextBlock until we get an EoS block
    while (!result.isEos()) {
      result = minusOperator.nextBlock();
    }
    Assert.assertTrue(result.isError());
  }
}
