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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class OpChainTest {
  private final List<TransferableBlock> _blockList = new ArrayList<>();
  private final ExecutorService _executor = Executors.newCachedThreadPool();

  private AutoCloseable _mocks;
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

    @Override
    public String toExplainString() {
      return "DUMMY";
    }
  }
}
