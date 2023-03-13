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
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;


public class OpChainSchedulerServiceTest {

  private ExecutorService _executor;
  private AutoCloseable _mocks;

  private MultiStageOperator _operatorA;
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

  @BeforeMethod
  public void beforeMethod() {
    _operatorA = Mockito.mock(MultiStageOperator.class);
    _scheduler = Mockito.mock(OpChainScheduler.class);
    clearInvocations(_scheduler);
    clearInvocations(_operatorA);
  }

  private void initExecutor(int numThreads) {
    _executor = Executors.newFixedThreadPool(numThreads);
  }

  private OpChain getChain(MultiStageOperator operator) {
    return new OpChain(operator, ImmutableList.of(), 1, 123, 1);
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredAfterStart()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenReturn(opChain).thenReturn(null);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    schedulerService.startAsync().awaitRunning();
    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredBeforeStart()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenReturn(opChain).thenReturn(null);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    schedulerService.register(opChain);
    schedulerService.startAsync().awaitRunning();

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldYieldOpChainOnNoOpBlock()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenReturn(opChain).thenReturn(null);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_scheduler).yield(Mockito.any());

    schedulerService.startAsync().awaitRunning();
    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldScheduleOpChainEvenIfNoOpChainsInAWhile()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    CountDownLatch latch = new CountDownLatch(3);
    AtomicBoolean returnedOpChain = new AtomicBoolean(false);
    Mockito.when(_operatorA.nextBlock()).thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenAnswer(inv -> {
      latch.countDown();
      if (latch.getCount() == 0 && returnedOpChain.compareAndSet(false, true)) {
        return opChain;
      }
      return null;
    });
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    schedulerService.startAsync().awaitRunning();

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected opChain to be scheduled");
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldCallCloseOnOperatorsThatFinishSuccessfully()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenReturn(opChain).thenReturn(null);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).close();

    schedulerService.startAsync().awaitRunning();
    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldCallCancelOnOperatorsThatReturnErrorBlock()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenReturn(opChain).thenReturn(null);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException("foo")));
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.startAsync().awaitRunning();
    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldCallCancelOnOpChainsWhenItIsCancelledByDispatch()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenAnswer((Answer<OpChain>) invocation -> {
      Thread.sleep(100);
      return opChain;
    });
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    Mockito.when(_operatorA.nextBlock()).thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());
    CountDownLatch deregisterLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      deregisterLatch.countDown();
      return null;
    }).when(_scheduler).deregister(Mockito.same(opChain));
    CountDownLatch awaitLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      awaitLatch.countDown();
      return null;
    }).when(_scheduler).yield(Mockito.any());

    schedulerService.startAsync().awaitRunning();
    schedulerService.register(opChain);

    Assert.assertTrue(awaitLatch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");

    // now cancel the request.
    schedulerService.cancel(123);

    Assert.assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Assert.assertTrue(deregisterLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be deregistered");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
    Mockito.verify(_scheduler, Mockito.times(1)).deregister(Mockito.any());
    schedulerService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldCallCancelOnOpChainsThatThrow()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenReturn(opChain).thenReturn(null);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);

    CountDownLatch cancelLatch = new CountDownLatch(1);
    CountDownLatch deregisterLatch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenThrow(new RuntimeException("foo"));
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());
    Mockito.doAnswer(inv -> {
      deregisterLatch.countDown();
      return null;
    }).when(_scheduler).deregister(Mockito.same(opChain));

    schedulerService.startAsync().awaitRunning();
    schedulerService.register(opChain);

    Assert.assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Assert.assertTrue(deregisterLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be deregistered");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
    Mockito.verify(_scheduler, Mockito.times(1)).deregister(Mockito.any());
    schedulerService.stopAsync().awaitTerminated();
  }
}
