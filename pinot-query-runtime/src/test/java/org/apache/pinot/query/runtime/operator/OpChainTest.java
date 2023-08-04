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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class OpChainTest {
  private static int _numOperatorsInitialized = 0;
  private final List<TransferableBlock> _blockList = new ArrayList<>();

  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _sourceOperator;
  @Mock
  private MailboxService _mailboxService1;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private MailboxService _mailboxService2;
  @Mock
  private ReceivingMailbox _mailbox2;
  @Mock
  private BlockExchange _exchange;

  private VirtualServerAddress _serverAddress;
  private StageMetadata _receivingStageMetadata;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    _serverAddress = new VirtualServerAddress("localhost", 123, 0);
    _receivingStageMetadata = new StageMetadata.Builder().setWorkerMetadataList(Stream.of(_serverAddress).map(
        s -> new WorkerMetadata.Builder().setVirtualServerAddress(s).addMailBoxInfoMap(0,
            new MailboxMetadata(ImmutableList.of(MailboxIdUtils.toPlanMailboxId(0, 0, 0, 0)), ImmutableList.of(s),
                ImmutableMap.of())).addMailBoxInfoMap(1,
            new MailboxMetadata(ImmutableList.of(MailboxIdUtils.toPlanMailboxId(0, 0, 0, 0)), ImmutableList.of(s),
                ImmutableMap.of())).addMailBoxInfoMap(2,
            new MailboxMetadata(ImmutableList.of(MailboxIdUtils.toPlanMailboxId(0, 0, 0, 0)), ImmutableList.of(s),
                ImmutableMap.of())).build()).collect(Collectors.toList())).build();

    when(_mailboxService1.getReceivingMailbox(any())).thenReturn(_mailbox1);
    when(_mailboxService2.getReceivingMailbox(any())).thenReturn(_mailbox2);

    try {
      doAnswer(invocation -> {
        TransferableBlock arg = invocation.getArgument(0);
        _blockList.add(arg);
        return true;
      }).when(_exchange).offerBlock(any(TransferableBlock.class), anyLong());
      when(_exchange.getRemainingCapacity()).thenReturn(1);
      when(_mailbox2.poll()).then(x -> {
        if (_blockList.isEmpty()) {
          //return TransferableBlockUtils.getNoOpTransferableBlock();
          return TransferableBlockUtils.getEndOfStreamTransferableBlock();
        }
        return _blockList.remove(0);
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
    _exchange.close();
  }

  @Test
  public void testExecutionTimerStats() {
    when(_sourceOperator.nextBlock()).then(x -> {
      Thread.sleep(100);
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });
    OpChain opChain = new OpChain(OperatorTestUtil.getDefaultContext(), _sourceOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();
    assertTrue(opChain.getStats().getExecutionTime() >= 100);

    when(_sourceOperator.nextBlock()).then(x -> {
      Thread.sleep(20);
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });
    opChain = new OpChain(OperatorTestUtil.getDefaultContext(), _sourceOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();
    assertTrue(opChain.getStats().getExecutionTime() >= 20);
    assertTrue(opChain.getStats().getExecutionTime() < 100);
  }

  @Test
  public void testStatsCollectionTracingEnabled() {
    OpChainExecutionContext context = OperatorTestUtil.getDefaultContext();
    DummyMultiStageOperator dummyMultiStageOperator = new DummyMultiStageOperator(context);

    OpChain opChain = new OpChain(context, dummyMultiStageOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    assertTrue(opChain.getStats().getExecutionTime() >= 1000);
    assertEquals(opChain.getStats().getOperatorStatsMap().size(), 1);
    assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(dummyMultiStageOperator.getOperatorId()));

    Map<String, String> executionStats =
        opChain.getStats().getOperatorStatsMap().get(dummyMultiStageOperator.getOperatorId()).getExecutionStats();

    long time = Long.parseLong(executionStats.get(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS.getName()));
    assertTrue(time >= 1000 && time <= 2000, "Expected " + DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS
        + " to be in [1000, 2000] but found " + time);
  }

  @Test
  public void testStatsCollectionTracingDisabled() {
    OpChainExecutionContext context = OperatorTestUtil.getDefaultContextWithTracingDisabled();
    DummyMultiStageOperator dummyMultiStageOperator = new DummyMultiStageOperator(context);

    OpChain opChain = new OpChain(context, dummyMultiStageOperator, new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    assertTrue(opChain.getStats().getExecutionTime() >= 1000);
    assertEquals(opChain.getStats().getOperatorStatsMap().size(), 0);
  }

  @Test
  public void testStatsCollectionTracingEnabledMultipleOperators() {
    long dummyOperatorWaitTime = 1000L;

    int receivedStageId = 2;
    int senderStageId = 1;
    OpChainExecutionContext context = new OpChainExecutionContext(_mailboxService1, 1, senderStageId, _serverAddress,
        System.currentTimeMillis() + 1000, _receivingStageMetadata, null, true);

    Stack<MultiStageOperator> operators =
        getFullOpchain(receivedStageId, senderStageId, context, dummyOperatorWaitTime);

    OpChain opChain = new OpChain(context, operators.peek(), new ArrayList<>());
    opChain.getStats().executing();
    while (!opChain.getRoot().nextBlock().isEndOfStreamBlock()) {
      // Drain the opchain
    }
    opChain.getStats().queued();

    OpChainExecutionContext secondStageContext =
        new OpChainExecutionContext(_mailboxService2, 1, senderStageId + 1, _serverAddress,
            System.currentTimeMillis() + 1000, _receivingStageMetadata, null, true);

    MailboxReceiveOperator secondStageReceiveOp =
        new MailboxReceiveOperator(secondStageContext, RelDistribution.Type.BROADCAST_DISTRIBUTED, senderStageId + 1);

    assertTrue(opChain.getStats().getExecutionTime() >= dummyOperatorWaitTime);
    int numOperators = operators.size();
    assertEquals(opChain.getStats().getOperatorStatsMap().size(), numOperators);
    while (!operators.isEmpty()) {
      assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(operators.pop().getOperatorId()));
    }

    while (!secondStageReceiveOp.nextBlock().isEndOfStreamBlock()) {
      // Drain the mailbox
    }
    assertEquals(secondStageContext.getStats().getOperatorStatsMap().size(), numOperators + 1);
  }

  @Test
  public void testStatsCollectionTracingDisableMultipleOperators() {
    long dummyOperatorWaitTime = 1000L;

    int receivedStageId = 2;
    int senderStageId = 1;
    OpChainExecutionContext context = new OpChainExecutionContext(_mailboxService1, 1, senderStageId, _serverAddress,
        System.currentTimeMillis() + 1000, _receivingStageMetadata, null, false);

    Stack<MultiStageOperator> operators =
        getFullOpchain(receivedStageId, senderStageId, context, dummyOperatorWaitTime);

    OpChain opChain = new OpChain(context, operators.peek(), new ArrayList<>());
    opChain.getStats().executing();
    opChain.getRoot().nextBlock();
    opChain.getStats().queued();

    OpChainExecutionContext secondStageContext =
        new OpChainExecutionContext(_mailboxService2, 1, senderStageId + 1, _serverAddress,
            System.currentTimeMillis() + 1000, _receivingStageMetadata, null, false);
    MailboxReceiveOperator secondStageReceiveOp =
        new MailboxReceiveOperator(secondStageContext, RelDistribution.Type.BROADCAST_DISTRIBUTED, senderStageId);

    assertTrue(opChain.getStats().getExecutionTime() >= dummyOperatorWaitTime);
    assertEquals(opChain.getStats().getOperatorStatsMap().size(), 2);
    assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(operators.pop().getOperatorId()));

    while (!secondStageReceiveOp.nextBlock().isEndOfStreamBlock()) {
      // Drain the mailbox
    }

    while (!operators.isEmpty()) {
      MultiStageOperator operator = operators.pop();
      if (operator.toExplainString().contains("SEND") || operator.toExplainString().contains("LEAF")) {
        assertTrue(opChain.getStats().getOperatorStatsMap().containsKey(operator.getOperatorId()));
      }
    }
    assertEquals(secondStageContext.getStats().getOperatorStatsMap().size(), 2);
  }

  private Stack<MultiStageOperator> getFullOpchain(int receivedStageId, int senderStageId,
      OpChainExecutionContext context, long waitTimeInMillis) {
    Stack<MultiStageOperator> operators = new Stack<>();
    DataSchema upStreamSchema =
        new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    //Mailbox Receive Operator
    try {
      when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1}),
          TransferableBlockUtils.getEndOfStreamTransferableBlock());
    } catch (Exception e) {
      fail("Exception while mocking mailbox receive: " + e.getMessage());
    }

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT intCol FROM tbl");
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(upStreamSchema, Arrays.asList(new Object[]{1}, new Object[]{2})), queryContext));
    LeafStageTransferableBlockOperator leafOp = new LeafStageTransferableBlockOperator(context,
        LeafStageTransferableBlockOperatorTest.getStaticBlockProcessor(resultsBlockList),
        Collections.singletonList(mock(ServerQueryRequest.class)), upStreamSchema);

    //Transform operator
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    TransformOperator transformOp =
        new TransformOperator(context, leafOp, upStreamSchema, Collections.singletonList(ref0), upStreamSchema);

    //Filter operator
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);
    FilterOperator filterOp = new FilterOperator(context, transformOp, upStreamSchema, booleanLiteral);

    // Dummy operator
    MultiStageOperator dummyWaitOperator = new DummyMultiStageCallableOperator(context, filterOp, waitTimeInMillis);

    //Mailbox Send operator
    MailboxSendOperator sendOperator =
        new MailboxSendOperator(context, dummyWaitOperator, _exchange, null, null, false);

    operators.push(leafOp);
    operators.push(transformOp);
    operators.push(filterOp);
    operators.push(dummyWaitOperator);
    operators.push(sendOperator);
    return operators;
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
