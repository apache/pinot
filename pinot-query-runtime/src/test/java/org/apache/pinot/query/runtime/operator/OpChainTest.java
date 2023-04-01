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

import java.util.ArrayList;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class OpChainTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _upstreamOperator;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testExecutionTimerStats() {
    Mockito.when(_upstreamOperator.nextBlock()).then(x -> {
      Thread.sleep(1000);
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    OpChain opChain = new OpChain(OperatorTestUtil.getDefaultContext(), _upstreamOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    Assert.assertTrue(opChain.getStats().getExecutionTime() >= 1000);

    Mockito.when(_upstreamOperator.nextBlock()).then(x -> {
      Thread.sleep(20);
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    opChain = new OpChain(OperatorTestUtil.getDefaultContext(), _upstreamOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    Assert.assertTrue(opChain.getStats().getExecutionTime() >= 20);
    Assert.assertTrue(opChain.getStats().getExecutionTime() < 100);
  }

  @Test
  public void testStatsCollection() {
    OpChainExecutionContext context = OperatorTestUtil.getDefaultContext();
    DummyMultiStageOperator dummyMultiStageOperator = new DummyMultiStageOperator(context);

    OpChain opChain = new OpChain(context, dummyMultiStageOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    Assert.assertTrue(opChain.getStats().getExecutionTime() >= 1000);
    Assert.assertEquals(opChain.getStats().getOperatorStatsMap().size(), 1);
    Assert.assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(dummyMultiStageOperator.getOperatorId()));

    Map<String, String> executionStats =
        opChain.getStats().getOperatorStatsMap().get(dummyMultiStageOperator.getOperatorId()).getExecutionStats();
    Assert.assertTrue(
        Long.parseLong(executionStats.get(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS.getName())) >= 1000);
    Assert.assertTrue(
        Long.parseLong(executionStats.get(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS.getName())) <= 2000);
  }

  static class DummyMultiStageOperator extends MultiStageOperator {
    public DummyMultiStageOperator(OpChainExecutionContext context) {
      super(context);
    }

    @Override
    protected TransferableBlock getNextBlock() {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE
      }
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }

    @Nullable
    @Override
    public String toExplainString() {
      return "DUMMY";
    }
  }
}
