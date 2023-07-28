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
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
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
    clearInvocations(_operatorA);
  }

  private void initExecutor(int numThreads) {
    _executor = Executors.newFixedThreadPool(numThreads);
  }

  private OpChain getChain(MultiStageOperator operator) {
    VirtualServerAddress address = new VirtualServerAddress("localhost", 1234, 1);
    OpChainExecutionContext context = new OpChainExecutionContext(null, 123L, 1, address, 0, null, null, true);
    return new OpChain(context, operator, ImmutableList.of());
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredAfterStart()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredBeforeStart()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    });

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCloseOnOperatorsThatFinishSuccessfully()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).close();

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOperatorsThatReturnErrorBlock()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException("foo")));
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

//  @Test
//  public void shouldCallCancelOnOpChainsWhenItIsCancelledByDispatch()
//      throws InterruptedException {
//    initExecutor(1);
//    OpChain opChain = getChain(_operatorA);
//    Mockito.when(_scheduler.next(Mockito.anyLong(), Mockito.any())).thenAnswer((Answer<OpChain>) invocation -> {
//      Thread.sleep(100);
//      return opChain;
//    });
//    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_scheduler, _executor);
//
//    Mockito.when(_operatorA.nextBlock()).thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());
//
//    CountDownLatch cancelLatch = new CountDownLatch(1);
//    Mockito.doAnswer(inv -> {
//      cancelLatch.countDown();
//      return null;
//    }).when(_operatorA).cancel(Mockito.any());
//    CountDownLatch deregisterLatch = new CountDownLatch(1);
//    Mockito.doAnswer(inv -> {
//      deregisterLatch.countDown();
//      return null;
//    }).when(_scheduler).deregister(Mockito.same(opChain));
//    CountDownLatch awaitLatch = new CountDownLatch(1);
//    Mockito.doAnswer(inv -> {
//      awaitLatch.countDown();
//      return null;
//    }).when(_scheduler).yield(Mockito.any());
//
//    schedulerService.startAsync().awaitRunning();
//    schedulerService.register(opChain);
//
//    Assert.assertTrue(awaitLatch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
//
//    // now cancel the request.
//    schedulerService.cancel(123);
//
//    Assert.assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
//    Assert.assertTrue(deregisterLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be deregistered");
//    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
//    Mockito.verify(_scheduler, Mockito.times(1)).deregister(Mockito.any());
//    schedulerService.stopAsync().awaitTerminated();
//  }

  @Test
  public void shouldCallCancelOnOpChainsThatThrow()
      throws InterruptedException {
    initExecutor(1);
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenThrow(new RuntimeException("foo"));
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.register(opChain);

    Assert.assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }
}
