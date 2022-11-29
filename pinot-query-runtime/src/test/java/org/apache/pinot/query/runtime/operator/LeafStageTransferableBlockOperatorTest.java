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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LeafStageTransferableBlockOperatorTest {

  @Test
  public void shouldReturnDataBlockThenMetadataBlock() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2})), null));
    LeafStageTransferableBlockOperator operator = new LeafStageTransferableBlockOperator(resultsBlockList, schema);

    // When:
    TransferableBlock transferableBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(transferableBlock.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(transferableBlock.getContainer().get(1), new Object[]{"", 2});
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldHandleCanonicalizationCorrectly() {
    // TODO: not all stored types are supported, add additional datatype when they are supported.
    // Given:
    DataSchema schema = new DataSchema(new String[]{"boolCol", "tsCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.TIMESTAMP});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{1, 1660000000000L},
            new Object[]{0, 1600000000000L})), null));
    LeafStageTransferableBlockOperator operator = new LeafStageTransferableBlockOperator(resultsBlockList, schema);

    // When:
    TransferableBlock transferableBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(transferableBlock.getContainer().get(0), new Object[]{true, new Timestamp(1660000000000L)});
    Assert.assertEquals(transferableBlock.getContainer().get(1), new Object[]{false, new Timestamp(1600000000000L)});
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldReturnMultipleDataBlockThenMetadataBlock() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<InstanceResponseBlock> resultsBlockList = Arrays.asList(
        new InstanceResponseBlock(new SelectionResultsBlock(schema,
            Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2})), null),
        new InstanceResponseBlock(new SelectionResultsBlock(schema,
            Arrays.asList(new Object[]{"bar", 3}, new Object[]{"foo", 4})), null),
        new InstanceResponseBlock(new SelectionResultsBlock(schema, Collections.emptyList()), null));
    LeafStageTransferableBlockOperator operator = new LeafStageTransferableBlockOperator(resultsBlockList, schema);

    // When:
    TransferableBlock transferableBlock1 = operator.nextBlock();
    TransferableBlock transferableBlock2 = operator.nextBlock();
    TransferableBlock transferableBlock3 = operator.nextBlock();

    // Then:
    Assert.assertEquals(transferableBlock1.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(transferableBlock1.getContainer().get(1), new Object[]{"", 2});
    Assert.assertEquals(transferableBlock2.getContainer().get(0), new Object[]{"bar", 3});
    Assert.assertEquals(transferableBlock2.getContainer().get(1), new Object[]{"foo", 4});
    Assert.assertEquals(transferableBlock3.getContainer().size(), 0);
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldGetErrorBlockWhenInstanceResponseContainsError() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    InstanceResponseBlock errorBlock = new InstanceResponseBlock();
    errorBlock.addException(QueryException.QUERY_EXECUTION_ERROR.getErrorCode(), "foobar");
    List<InstanceResponseBlock> resultsBlockList = Arrays.asList(
        new InstanceResponseBlock(new SelectionResultsBlock(schema,
            Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2})), null),
        errorBlock,
        new InstanceResponseBlock(new SelectionResultsBlock(schema, Collections.emptyList()), null));
    LeafStageTransferableBlockOperator operator = new LeafStageTransferableBlockOperator(resultsBlockList, schema);

    // When:
    TransferableBlock transferableBlock = operator.nextBlock();

    // Then:
    Assert.assertTrue(transferableBlock.isErrorBlock());
  }
}
