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
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class OpChainTest {
  private static int _numOperatorsInitialized = 0;

  private final List<TransferableBlock> _blockList = new ArrayList<>();
  private final ExecutorService _executor = Executors.newCachedThreadPool();
  private final AtomicReference<LeafStageTransferableBlockOperator> _leafOpRef = new AtomicReference<>();
  private final MailboxInfos _mailboxInfos =
      new SharedMailboxInfos(new MailboxInfo("localhost", 1234, ImmutableList.of(0)));
  private final WorkerMetadata _workerMetadata =
      new WorkerMetadata(0, ImmutableMap.of(0, _mailboxInfos, 1, _mailboxInfos, 2, _mailboxInfos), ImmutableMap.of());
  private final StageMetadata _stageMetadata =
      new StageMetadata(0, ImmutableList.of(_workerMetadata), ImmutableMap.of());

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

  @BeforeMethod
  public void setUpMethod() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_mailboxService1.getReceivingMailbox(any())).thenReturn(_mailbox1);
    when(_mailboxService2.getReceivingMailbox(any())).thenReturn(_mailbox2);

    try {
      doAnswer(invocation -> {
        TransferableBlock arg = invocation.getArgument(0);
        _blockList.add(arg);
        return true;
      }).when(_exchange).send(any(TransferableBlock.class));
      when(_mailbox2.poll()).then(x -> {
        if (_blockList.isEmpty()) {
          return TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0);
        }
        return _blockList.remove(0);
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
    _exchange.close();
  }

  @AfterClass
  public void tearDown() {
    _executor.shutdown();
  }

  @Test
  public void testStatsCollectionTracingEnabled() {
    OpChainExecutionContext context = OperatorTestUtil.getTracingContext();
    DummyMultiStageOperator dummyMultiStageOperator = new DummyMultiStageOperator(context);

    OpChain opChain = new OpChain(context, dummyMultiStageOperator);
    TransferableBlock eosBlock = drainOpChain(opChain);

    assertTrue(eosBlock.isSuccessfulEndOfStreamBlock(), "Expected end of stream block to be successful");
    MultiStageQueryStats queryStats = eosBlock.getQueryStats();
    assertNotNull(queryStats, "Expected query stats to be non-null");

    @SuppressWarnings("unchecked")
    StatMap<LiteralValueOperator.StatKey> lastOperatorStats =
        (StatMap<LiteralValueOperator.StatKey>) queryStats.getCurrentStats().getLastOperatorStats();
    assertNotEquals(lastOperatorStats.getLong(LiteralValueOperator.StatKey.EXECUTION_TIME_MS), 0L,
        "Expected execution time to be non-zero");
  }

  @Test
  public void testStatsCollectionTracingDisabled() {
    OpChainExecutionContext context = OperatorTestUtil.getNoTracingContext();
    DummyMultiStageOperator dummyMultiStageOperator = new DummyMultiStageOperator(context);

    OpChain opChain = new OpChain(context, dummyMultiStageOperator);
    TransferableBlock eosBlock = drainOpChain(opChain);

    assertTrue(eosBlock.isSuccessfulEndOfStreamBlock(), "Expected end of stream block to be successful");
    MultiStageQueryStats queryStats = eosBlock.getQueryStats();
    assertNotNull(queryStats, "Expected query stats to be non-null");

    @SuppressWarnings("unchecked")
    StatMap<LiteralValueOperator.StatKey> lastOperatorStats =
        (StatMap<LiteralValueOperator.StatKey>) queryStats.getCurrentStats().getLastOperatorStats();
    assertNotEquals(lastOperatorStats.getLong(LiteralValueOperator.StatKey.EXECUTION_TIME_MS), 0L,
        "Expected execution time to be collected");
  }

  private Stack<MultiStageOperator> getFullOpChain(OpChainExecutionContext context, long waitTimeInMillis) {
    Stack<MultiStageOperator> operators = new Stack<>();
    DataSchema upStreamSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{ColumnDataType.INT});
    //Mailbox Receive Operator
    try {
      when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1}),
          TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    } catch (Exception e) {
      fail("Exception while mocking mailbox receive: " + e.getMessage());
    }

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT intCol FROM tbl");
    List<BaseResultsBlock> dataBlocks = ImmutableList.of(
        new SelectionResultsBlock(upStreamSchema, Arrays.asList(new Object[]{1}, new Object[]{2}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafStageTransferableBlockOperator leafOp =
        new LeafStageTransferableBlockOperator(context, ImmutableList.of(mock(ServerQueryRequest.class)),
            upStreamSchema, queryExecutor, _executor);
    _leafOpRef.set(leafOp);

    //Transform operator
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    TransformOperator transformOp =
        new TransformOperator(context, leafOp, upStreamSchema, ImmutableList.of(ref0), upStreamSchema);

    //Filter operator
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    FilterOperator filterOp = new FilterOperator(context, transformOp, upStreamSchema, booleanLiteral);

    // Dummy operator
    MultiStageOperator dummyWaitOperator = new DummyMultiStageCallableOperator(context, filterOp, waitTimeInMillis);

    //Mailbox Send operator
    MailboxSendOperator sendOperator =
        new MailboxSendOperator(context, dummyWaitOperator, ignore -> _exchange, null, null, false);

    operators.push(leafOp);
    operators.push(transformOp);
    operators.push(filterOp);
    operators.push(dummyWaitOperator);
    operators.push(sendOperator);
    return operators;
  }

  private QueryExecutor mockQueryExecutor(List<BaseResultsBlock> dataBlocks, InstanceResponseBlock metadataBlock) {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    when(queryExecutor.execute(any(), any(), any())).thenAnswer(invocation -> {
      LeafStageTransferableBlockOperator operator = _leafOpRef.get();
      for (BaseResultsBlock dataBlock : dataBlocks) {
        operator.addResultsBlock(dataBlock);
      }
      return metadataBlock;
    });
    return queryExecutor;
  }

  private TransferableBlock drainOpChain(OpChain opChain) {
    TransferableBlock resultBlock = opChain.getRoot().nextBlock();
    while (!resultBlock.isEndOfStreamBlock()) {
      resultBlock = opChain.getRoot().nextBlock();
    }
    return resultBlock;
  }

  static class DummyMultiStageOperator extends MultiStageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DummyMultiStageOperator.class);
    private final StatMap<LiteralValueOperator.StatKey> _statMap = new StatMap<>(LiteralValueOperator.StatKey.class);

    public DummyMultiStageOperator(OpChainExecutionContext context) {
      super(context);
    }

    @Override
    public void registerExecution(long time, int numRows) {
      _statMap.merge(LiteralValueOperator.StatKey.EXECUTION_TIME_MS, time);
      _statMap.merge(LiteralValueOperator.StatKey.EMITTED_ROWS, numRows);
    }

    @Override
    protected Logger logger() {
      return LOGGER;
    }

    @Override
    protected TransferableBlock getNextBlock() {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE
      }
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(MultiStageQueryStats.createLiteral(0, _statMap));
    }

    @Override
    public Type getOperatorType() {
      return Type.LITERAL;
    }

    @Override
    public List<MultiStageOperator> getChildOperators() {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public String toExplainString() {
      return "DUMMY";
    }
  }

  static class DummyMultiStageCallableOperator extends MultiStageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DummyMultiStageCallableOperator.class);
    private final MultiStageOperator _upstream;
    private final long _sleepTimeInMillis;
    private final StatMap<TransformOperator.StatKey> _statMap = new StatMap<>(TransformOperator.StatKey.class);

    public DummyMultiStageCallableOperator(OpChainExecutionContext context, MultiStageOperator upstream,
        long sleepTimeInMillis) {
      super(context);
      _upstream = upstream;
      _sleepTimeInMillis = sleepTimeInMillis;
    }

    @Override
    public void registerExecution(long time, int numRows) {
      _statMap.merge(TransformOperator.StatKey.EXECUTION_TIME_MS, time);
      _statMap.merge(TransformOperator.StatKey.EMITTED_ROWS, numRows);
    }

    @Override
    protected Logger logger() {
      return LOGGER;
    }

    @Override
    public List<MultiStageOperator> getChildOperators() {
      return ImmutableList.of(_upstream);
    }

    @Override
    protected TransferableBlock getNextBlock() {
      TransferableBlock block;

      try {
        Thread.sleep(_sleepTimeInMillis);
        do {
          block = _upstream.nextBlock();
        } while (block.isEndOfStreamBlock());

        MultiStageQueryStats queryStats = block.getQueryStats();
        assert queryStats != null;
        queryStats.getCurrentStats().addLastOperator(getOperatorType(), _statMap);
        return TransferableBlockUtils.getEndOfStreamTransferableBlock(queryStats);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Type getOperatorType() {
      return Type.TRANSFORM;
    }

    @Override
    public String toExplainString() {
      return "DUMMY_" + _numOperatorsInitialized++;
    }
  }
}
