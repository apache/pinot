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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.data.FieldSpec;
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

  private static int _numOperatorsInitialized = 0;
  private final List<TransferableBlock> _blockList = new ArrayList<>();

  @Mock
  private MailboxService<TransferableBlock> _mailboxService;
  @Mock
  private VirtualServer _server;
  @Mock
  private KeySelector<Object[], Object[]> _selector;
  @Mock
  private MailboxSendOperator.BlockExchangeFactory _exchangeFactory;
  @Mock
  private BlockExchange _exchange;

  @Mock
  private ReceivingMailbox<TransferableBlock> _mailbox;


  @Mock
  private MailboxService<TransferableBlock> _mailboxService2;
  @Mock
  private ReceivingMailbox<TransferableBlock> _mailbox2;


  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    Mockito.when(_exchangeFactory.build(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
        Mockito.anyLong())).thenReturn(_exchange);

    Mockito.when(_server.getHostname()).thenReturn("mock");
    Mockito.when(_server.getQueryMailboxPort()).thenReturn(0);
    Mockito.when(_server.getVirtualId()).thenReturn(0);

    Mockito.when(_mailboxService.getReceivingMailbox(Mockito.any())).thenReturn(_mailbox);
    Mockito.when(_mailboxService2.getReceivingMailbox(Mockito.any())).thenReturn(_mailbox2);

    try {
      Mockito.doAnswer(invocation -> {
        TransferableBlock arg = invocation.getArgument(0);
        _blockList.add(arg);
        return null;
      }).when(_exchange).send(Mockito.any(TransferableBlock.class));

      Mockito.when(_mailbox2.receive()).then(x -> {
        if (_blockList.isEmpty()) {
          return TransferableBlockUtils.getNoOpTransferableBlock();
        }
        return _blockList.remove(0);
      });

      Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
  public void testStatsCollectionTracingEnabled() {
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

  @Test
  public void testStatsCollectionTracingDisabled() {
    OpChainExecutionContext context = OperatorTestUtil.getDefaultContextWithTracingDisabled();
    DummyMultiStageOperator dummyMultiStageOperator = new DummyMultiStageOperator(context);

    OpChain opChain = new OpChain(context, dummyMultiStageOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    Assert.assertTrue(opChain.getStats().getExecutionTime() >= 1000);
    Assert.assertEquals(opChain.getStats().getOperatorStatsMap().size(), 0);
  }

<<<<<<< HEAD
  @Test
  public void testStatsCollectionTracingEnabledMultipleOperators() {
    long dummyOperatorWaitTime = 1000L;

    int receivedStageId = 2;
    int senderStageId = 1;
    StageMetadata stageMetadata = new StageMetadata();
    stageMetadata.setServerInstances(ImmutableList.of(_server));
    Map<Integer, StageMetadata> stageMetadataMap = Collections.singletonMap(receivedStageId, stageMetadata);
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 1, senderStageId, new VirtualServerAddress(_server), 1000, 1000,
            stageMetadataMap, true);

    Stack<MultiStageOperator> operators =
        getFullOpchain(receivedStageId, senderStageId, context, dummyOperatorWaitTime);

    OpChain opChain = new OpChain(context, operators.peek(), new ArrayList<>());
    opChain.getStats().executing();
    while (!opChain.getRoot().nextBlock().isEndOfStreamBlock()) {
      // Drain the opchain
    }
    opChain.getStats().queued();

    OpChainExecutionContext secondStageContext =
        new OpChainExecutionContext(_mailboxService2, 1, senderStageId + 1, new VirtualServerAddress(_server), 1000,
            1000, stageMetadataMap, true);

    MailboxReceiveOperator secondStageReceiveOp =
        new MailboxReceiveOperator(secondStageContext, ImmutableList.of(_server),
            RelDistribution.Type.BROADCAST_DISTRIBUTED, senderStageId, receivedStageId + 1, null);

    Assert.assertTrue(opChain.getStats().getExecutionTime() >= dummyOperatorWaitTime);
    int numOperators = operators.size();
    Assert.assertEquals(opChain.getStats().getOperatorStatsMap().size(), numOperators);
    while (!operators.isEmpty()) {
      Assert.assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(operators.pop().getOperatorId()));
    }

    while (!secondStageReceiveOp.nextBlock().isEndOfStreamBlock()) {
      // Drain the mailbox
    }
    Assert.assertEquals(secondStageContext.getStats().getOperatorStatsMap().size(), numOperators + 1);
  }

  @Test
  public void testStatsCollectionTracingDisableMultipleOperators() {
    long dummyOperatorWaitTime = 1000L;

    int receivedStageId = 2;
    int senderStageId = 1;
    StageMetadata stageMetadata = new StageMetadata();
    stageMetadata.setServerInstances(ImmutableList.of(_server));
    Map<Integer, StageMetadata> stageMetadataMap = Collections.singletonMap(receivedStageId, stageMetadata);
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 1, senderStageId, new VirtualServerAddress(_server), 1000, 1000,
            stageMetadataMap, false);

    Stack<MultiStageOperator> operators =
        getFullOpchain(receivedStageId, senderStageId, context, dummyOperatorWaitTime);

    OpChain opChain = new OpChain(context, operators.peek(), new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    OpChainExecutionContext secondStageContext =
        new OpChainExecutionContext(_mailboxService2, 1, senderStageId + 1, new VirtualServerAddress(_server), 1000,
            1000, stageMetadataMap, false);

    MailboxReceiveOperator secondStageReceiveOp =
        new MailboxReceiveOperator(secondStageContext, ImmutableList.of(_server),
            RelDistribution.Type.BROADCAST_DISTRIBUTED, senderStageId, receivedStageId + 1, null);

    Assert.assertTrue(opChain.getStats().getExecutionTime() >= dummyOperatorWaitTime);
    Assert.assertEquals(opChain.getStats().getOperatorStatsMap().size(), 2);
    Assert.assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(operators.pop().getOperatorId()));

    while (!secondStageReceiveOp.nextBlock().isEndOfStreamBlock()) {
      // Drain the mailbox
    }

    while (!operators.isEmpty()) {
      MultiStageOperator operator = operators.pop();
      if (operator.toExplainString().contains("SEND") || operator.toExplainString().contains("LEAF")) {
        Assert.assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(operator.getOperatorId()));
      }
    }
    Assert.assertEquals(secondStageContext.getStats().getOperatorStatsMap().size(), 2);
  }

  private Stack<MultiStageOperator> getFullOpchain(int receivedStageId, int senderStageId,
      OpChainExecutionContext context, long waitTimeInMillis) {
    Stack<MultiStageOperator> operators = new Stack<>();
    DataSchema upStreamSchema =
        new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    //Mailbox Receive Operator
    try {
      Mockito.when(_mailbox.receive()).thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1}),
          TransferableBlockUtils.getEndOfStreamTransferableBlock());
    } catch (Exception e) {
      Assert.fail("Exception while mocking mailbox receive: " + e.getMessage());
    }

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT intCol FROM tbl");
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(upStreamSchema, Arrays.asList(new Object[]{1}, new Object[]{2})), queryContext));
    LeafStageTransferableBlockOperator leafOp =
        new LeafStageTransferableBlockOperator(context, resultsBlockList, upStreamSchema);

    //Transform operator
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    TransformOperator transformOp =
        new TransformOperator(context, leafOp, upStreamSchema, ImmutableList.of(ref0), upStreamSchema);

    //Filter operator
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);
    FilterOperator filterOp = new FilterOperator(context, transformOp, upStreamSchema, booleanLiteral);

    // Dummy operator
    MultiStageOperator dummyWaitOperator = new DummyMultiStageCallableOperator(context, filterOp, waitTimeInMillis);

    //Mailbox Send operator
    MailboxSendOperator sendOperator =
        new MailboxSendOperator(context, dummyWaitOperator, RelDistribution.Type.HASH_DISTRIBUTED, _selector, null,
            null, false,
            server -> new JsonMailboxIdentifier("123", "0@from:1", "0@to:2", senderStageId, receivedStageId),
            _exchangeFactory, receivedStageId);

    operators.push(leafOp);
    operators.push(transformOp);
    operators.push(filterOp);
    operators.push(dummyWaitOperator);
    operators.push(sendOperator);
    return operators;
  }


=======
>>>>>>> f768fc5503 (Fix tests)
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

  static class DummyMultiStageCallableOperator extends MultiStageOperator {
    private final MultiStageOperator _upstream;
    private final long _sleepTimeInMillis;

    public DummyMultiStageCallableOperator(OpChainExecutionContext context, MultiStageOperator upstream,
        long sleepTimeInMillis) {
      super(context);
      _upstream = upstream;
      _sleepTimeInMillis = sleepTimeInMillis;
    }

    @Override
    protected TransferableBlock getNextBlock() {
      try {
        Thread.sleep(_sleepTimeInMillis);
        _upstream.nextBlock();
      } catch (InterruptedException e) {
        // IGNORE
      }
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }

    @Nullable
    @Override
    public String toExplainString() {
      return "DUMMY_" + _numOperatorsInitialized++;
    }
  }
}
