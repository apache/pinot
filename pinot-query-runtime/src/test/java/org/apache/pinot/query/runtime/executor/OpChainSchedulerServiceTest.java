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
package org.apache.pinot.query.runtime.executor;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class OpChainSchedulerServiceTest {

  private ExecutorService _executor;
  private AutoCloseable _mocks;

  @Mock
  private Operator<TransferableBlock> _operatorA;
  @Mock
  private Operator<TransferableBlock> _operatorB;
  @Mock
  private OpChainScheduler _scheduler;

  @BeforeClass
  public void beforeClass() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _mocks.close();
  }

  @AfterMethod
  public void afterMethod() {
    _executor.shutdownNow();
  }

  private void initExecutor(int numThreads) {
    _executor = Executors.newFixedThreadPool(numThreads);
  }

  private OpChain getChain(Operator<TransferableBlock> operator) {
    return new OpChain(operator, ImmutableList.of(), 123, 1);
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredAfterStart()
      throws InterruptedException {
    // Given:
    initExecutor(1);
    Mockito.when(_scheduler.hasNext()).thenReturn(true);
    Mockito.when(_scheduler.next()).thenReturn(getChain(_operatorA));
    OpChainSchedulerService scheduler = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    // When:
    scheduler.startAsync().awaitRunning();
    scheduler.register(new OpChain(_operatorA, ImmutableList.of(), 123, 1));

    // Then:
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    scheduler.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredBeforeStart()
      throws InterruptedException {
    // Given:
    initExecutor(1);
    Mockito.when(_scheduler.hasNext()).thenReturn(true);
    Mockito.when(_scheduler.next()).thenReturn(getChain(_operatorA));
    OpChainSchedulerService scheduler = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    // When:
    scheduler.register(new OpChain(_operatorA, ImmutableList.of(), 123, 1));
    scheduler.startAsync().awaitRunning();

    // Then:
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    scheduler.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldReRegisterOpChainOnNoOpBlock()
      throws InterruptedException {
    // Given:
    initExecutor(1);
    Mockito.when(_scheduler.hasNext()).thenReturn(true);
    Mockito.when(_scheduler.next()).thenReturn(getChain(_operatorA));
    OpChainSchedulerService scheduler = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock())
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenAnswer(inv -> {
          latch.countDown();
          return TransferableBlockUtils.getEndOfStreamTransferableBlock();
        });

    // When:
    scheduler.startAsync().awaitRunning();
    scheduler.register(new OpChain(_operatorA, ImmutableList.of(), 123, 1));

    // Then:
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    Mockito.verify(_scheduler, Mockito.times(2)).register(Mockito.any(OpChain.class), Mockito.any(boolean.class));
    scheduler.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldYieldOpChainsWhenNoWorkCanBeDone()
      throws InterruptedException {
    // Given:
    initExecutor(1);
    Mockito.when(_scheduler.hasNext()).thenReturn(true);
    Mockito.when(_scheduler.next())
        .thenReturn(getChain(_operatorA))
        .thenReturn(getChain(_operatorB))
        .thenReturn(getChain(_operatorA));
    OpChainSchedulerService scheduler = new OpChainSchedulerService(_scheduler, _executor);

    AtomicBoolean opAReturnedNoOp = new AtomicBoolean(false);
    AtomicBoolean hasOpBRan = new AtomicBoolean(false);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      if (hasOpBRan.get()) {
        latch.countDown();
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      } else {
        opAReturnedNoOp.set(true);
        return TransferableBlockUtils.getNoOpTransferableBlock();
      }
    });

    Mockito.when(_operatorB.nextBlock()).thenAnswer(inv -> {
      hasOpBRan.set(true);
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    // When:
    scheduler.startAsync().awaitRunning();
    scheduler.register(new OpChain(_operatorA, ImmutableList.of(), 123, 1));
    scheduler.register(new OpChain(_operatorB, ImmutableList.of(), 123, 1));

    // Then:
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    Assert.assertTrue(opAReturnedNoOp.get(), "expected opA to be scheduled first");
    scheduler.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldNotCallSchedulerNextWhenHasNextReturnsFalse()
      throws InterruptedException {
    // Given:
    initExecutor(1);
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_scheduler.hasNext()).thenAnswer(inv -> {
      latch.countDown();
      return false;
    });
    OpChainSchedulerService scheduler = new OpChainSchedulerService(_scheduler, _executor);

    // When:
    scheduler.startAsync().awaitRunning();

    // Then:
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected hasNext to be called");
    scheduler.stopAsync().awaitTerminated();
    Mockito.verify(_scheduler, Mockito.never()).next();
  }

  @Test
  public void shouldReevaluateHasNextWhenOnDataAvailableIsCalled()
      throws InterruptedException {
    // Given:
    initExecutor(1);
    CountDownLatch firstHasNext = new CountDownLatch(1);
    CountDownLatch secondHasNext = new CountDownLatch(1);
    Mockito.when(_scheduler.hasNext()).thenAnswer(inv -> {
      firstHasNext.countDown();
      return false;
    }).then(inv -> {
      secondHasNext.countDown();
      return false;
    });

    OpChainSchedulerService scheduler = new OpChainSchedulerService(_scheduler, _executor);

    // When:
    scheduler.startAsync().awaitRunning();
    Assert.assertTrue(firstHasNext.await(10, TimeUnit.SECONDS), "expected hasNext to be called");
    scheduler.onDataAvailable(null);

    // Then:
    Assert.assertTrue(secondHasNext.await(10, TimeUnit.SECONDS), "expected hasNext to be called again");
    scheduler.stopAsync().awaitTerminated();
  }
}
