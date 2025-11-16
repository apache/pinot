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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.BlockListMultiStageOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IntersectAllOperatorTest {

  @Test
  public void testIntersectAllOperator() {
    DataSchema schema = new DataSchema(new String[]{"int_col"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    MultiStageOperator leftOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1)
        .addRow(2)
        .addRow(3)
        .buildWithEos();
    MultiStageOperator rightOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1)
        .addRow(2)
        .addRow(4)
        .buildWithEos();

    IntersectAllOperator intersectOperator =
        new IntersectAllOperator(OperatorTestUtil.getTracingContext(), List.of(leftOperator, rightOperator),
            schema);

    MseBlock result = intersectOperator.nextBlock();
    while (result.isEos()) {
      result = intersectOperator.nextBlock();
    }
    List<Object[]> resultRows = ((MseBlock.Data) result).asRowHeap().getRows();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1}, new Object[]{2});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < resultRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @Test
  public void testIntersectAllOperatorWithDups() {
    DataSchema schema = new DataSchema(new String[]{"int_col"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    MultiStageOperator leftOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1)
        .addRow(2)
        .addRow(2)
        .addRow(3)
        .addRow(3)
        .addRow(3)
        .buildWithEos();
    MultiStageOperator rightOperator = new BlockListMultiStageOperator.Builder(schema)
        .addRow(2)
        .addRow(3)
        .addRow(3)
        .addRow(4)
        .buildWithEos();

    IntersectAllOperator intersectOperator =
        new IntersectAllOperator(OperatorTestUtil.getTracingContext(), List.of(leftOperator, rightOperator),
            schema);

    MseBlock result = intersectOperator.nextBlock();
    while (result.isEos()) {
      result = intersectOperator.nextBlock();
    }
    List<Object[]> resultRows = ((MseBlock.Data) result).asRowHeap().getRows();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2}, new Object[]{3}, new Object[]{3});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < resultRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }
}
