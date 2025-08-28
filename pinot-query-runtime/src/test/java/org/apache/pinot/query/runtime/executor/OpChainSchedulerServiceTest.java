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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class OpChainSchedulerServiceTest {

  private ExecutorService _executor;
  private AutoCloseable _mocks;

  private MultiStageOperator _operatorA;

  @BeforeClass
  public void beforeClass() {
    _mocks = MockitoAnnotations.openMocks(this);
    _executor = Executors.newCachedThreadPool(new NamedThreadFactory("worker_on_" + getClass().getSimpleName()));
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _mocks.close();
    ExecutorServiceUtils.close(_executor);
  }

  @BeforeMethod
  public void beforeMethod() {
    _operatorA = Mockito.mock(MultiStageOperator.class);
    clearInvocations(_operatorA);
  }

  private OpChain getChain(MultiStageOperator operator) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    OpChainExecutionContext context =
        new OpChainExecutionContext(mailboxService, 123L, Long.MAX_VALUE, Long.MAX_VALUE, Map.of(),
            new StageMetadata(0, ImmutableList.of(workerMetadata), Map.of()), workerMetadata, null, null, true);
    return new OpChain(context, operator);
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredAfterStart()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return SuccessMseBlock.INSTANCE;
    });

    schedulerService.register(opChain);

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredBeforeStart()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return SuccessMseBlock.INSTANCE;
    });

    schedulerService.register(opChain);

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCloseOnOperatorsThatFinishSuccessfully()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).close();

    schedulerService.register(opChain);

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOperatorsThatReturnErrorBlock()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(ErrorMseBlock.fromException(new RuntimeException("foo")));
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.register(opChain);

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOpChainsWhenItIsCancelledByDispatch()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch opChainStarted = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      opChainStarted.countDown();
      while (true) {
        Thread.sleep(1000);
      }
    }).when(_operatorA).nextBlock();

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(_operatorA).calculateStats();

    schedulerService.register(opChain);

    assertTrue(opChainStarted.await(10, TimeUnit.SECONDS), "op chain doesn't seem to be started");

    // now cancel the request.
    schedulerService.cancel(123L);

    assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }

  @Test
  public void shouldCallCancelOnOpChainsThatThrow()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenThrow(new RuntimeException("foo"));
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.register(opChain);

    assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }

  @Test
  public void shouldThrowQueryCancelledExceptionWhenRegisteringOpChainAfterQueryCancellation() {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    // First cancel the query with the same requestId (123L as defined in getChain method)
    schedulerService.cancel(123L);

    // Now try to register an OpChain for the same query - should throw QueryCancelledException
    try {
      schedulerService.register(opChain);
      fail("Expected QueryCancelledException to be thrown when registering OpChain after query cancellation");
    } catch (QueryCancelledException e) {
      assertTrue(e.getMessage().contains("Query has been cancelled before op-chain"));
      assertTrue(e.getMessage().contains("being scheduled"));
    }
  }

  @Test
  public void shouldHandleConcurrentCancellationAndRegistration()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    // Setup mock to slow down the execution so we can test concurrent cancellation
    CountDownLatch registrationStarted = new CountDownLatch(1);
    CountDownLatch cancellationCanProceed = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      registrationStarted.countDown();
      cancellationCanProceed.await();
      return SuccessMseBlock.INSTANCE;
    });
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(_operatorA).calculateStats();

    // Start registration in a separate thread
    CountDownLatch registrationCompleted = new CountDownLatch(1);
    boolean[] registrationSucceeded = {false};
    Thread registrationThread = new Thread(() -> {
      try {
        schedulerService.register(opChain);
        registrationSucceeded[0] = true;
      } catch (QueryCancelledException e) {
        // Expected if cancellation happens before registration
        registrationSucceeded[0] = false;
      } finally {
        registrationCompleted.countDown();
      }
    });

    registrationThread.start();

    // Wait for registration to start
    assertTrue(registrationStarted.await(10, TimeUnit.SECONDS), "Registration should have started");

    // Now cancel the query while it's running
    schedulerService.cancel(123L);

    // Allow the opchain execution to complete
    cancellationCanProceed.countDown();

    // Wait for registration thread to complete
    assertTrue(registrationCompleted.await(10, TimeUnit.SECONDS), "Registration thread should complete");

    // The registration should have succeeded since it started before cancellation
    assertTrue(registrationSucceeded[0], "Registration should have succeeded");

    // Now try to register another OpChain with the same requestId - should fail
    try {
      schedulerService.register(getChain(_operatorA));
      fail("Expected QueryCancelledException for subsequent registration after cancellation");
    } catch (QueryCancelledException e) {
      assertTrue(e.getMessage().contains("Query has been cancelled before op-chain"));
    }
  }
}
