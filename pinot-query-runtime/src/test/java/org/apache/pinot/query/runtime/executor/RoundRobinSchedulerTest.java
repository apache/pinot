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
import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RoundRobinSchedulerTest {
  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;
  private static final int DEFAULT_VIRTUAL_SERVER_ID = 1;
  private static final int DEFAULT_POLL_TIMEOUT_MS = 1;
  private static final int DEFAULT_RELEASE_TIMEOUT_MS = 10;
  private static final long DEFAULT_REQUEST_ID = 123;

  private static final String MAILBOX_1 =
      MailboxIdUtils.toMailboxId(DEFAULT_REQUEST_ID, DEFAULT_SENDER_STAGE_ID, 0, DEFAULT_RECEIVER_STAGE_ID, 1);
  private static final String MAILBOX_2 =
      MailboxIdUtils.toMailboxId(DEFAULT_REQUEST_ID, DEFAULT_SENDER_STAGE_ID, 0, DEFAULT_RECEIVER_STAGE_ID, 2);
  private static final String MAILBOX_3 =
      MailboxIdUtils.toMailboxId(DEFAULT_REQUEST_ID, DEFAULT_SENDER_STAGE_ID, 0, DEFAULT_RECEIVER_STAGE_ID, 3);

  @Mock
  private MultiStageOperator _operator;

  private AutoCloseable _mocks;

  private RoundRobinScheduler _scheduler;

  @BeforeClass
  public void beforeClass() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _mocks.close();
  }

  @AfterTest
  public void afterTest() {
    _scheduler.shutdownNow();
  }

  @Test
  public void testSchedulerHappyPath()
      throws InterruptedException {
    OpChain chain = new OpChain(
        getOpChainExecutionContext(DEFAULT_REQUEST_ID, DEFAULT_RECEIVER_STAGE_ID, DEFAULT_VIRTUAL_SERVER_ID), _operator,
        ImmutableList.of(MAILBOX_1));
    _scheduler = new RoundRobinScheduler(DEFAULT_RELEASE_TIMEOUT_MS);
    _scheduler.register(chain);

    // OpChain is scheduled immediately
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain);
    // No op-chains ready, so scheduler returns null
    Assert.assertNull(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    // When Op-Chain is done executing, yield is called
    _scheduler.yield(chain);
    // When data is received, callback is called
    _scheduler.onDataAvailable(MailboxIdUtils.toOpChainId(MAILBOX_1));
    // next should return the OpChain immediately after the callback
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain);
    // Say the OpChain is done, then a de-register will be called
    _scheduler.deregister(chain);

    // There should be no entries left in the scheduler
    Assert.assertEquals(0,
        _scheduler.aliveChainsSize() + _scheduler.readySize() + _scheduler.seenMailSize() + _scheduler.availableSize());
  }

  @Test
  public void testSchedulerWhenSenderDies()
      throws InterruptedException {
    OpChain chain = new OpChain(
        getOpChainExecutionContext(DEFAULT_REQUEST_ID, DEFAULT_RECEIVER_STAGE_ID, DEFAULT_VIRTUAL_SERVER_ID), _operator,
        ImmutableList.of(MAILBOX_1));
    _scheduler = new RoundRobinScheduler(DEFAULT_RELEASE_TIMEOUT_MS);
    _scheduler.register(chain);

    // OpChain runs immediately after registration
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain);
    // No more OpChains to run
    Assert.assertNull(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    // When op-chain returns a no-op block, suspend it
    _scheduler.yield(chain);

    // Unless a callback is called, the chain would remain suspended. However, the scheduler will automatically
    // promote available OpChains to ready every releaseMs.
    Assert.assertEquals(_scheduler.next(DEFAULT_RELEASE_TIMEOUT_MS + 100, TimeUnit.MILLISECONDS), chain);

    // Assuming the OpChain has timed out, the OpChain will be de-registered
    _scheduler.deregister(chain);

    // There should be no entries left in the scheduler
    Assert.assertEquals(0,
        _scheduler.aliveChainsSize() + _scheduler.readySize() + _scheduler.seenMailSize() + _scheduler.availableSize());
  }

  @Test
  public void testSchedulerWhenParallelismGtOne()
      throws InterruptedException {
    // When parallelism is > 1, multiple OpChains with the same requestId and stageId would be registered in the same
    // scheduler. Data received on a given mailbox should wake up exactly 1 OpChain corresponding to the virtual
    // server-id determined by the Mailbox.
    OpChain chain1 =
        new OpChain(getOpChainExecutionContext(DEFAULT_REQUEST_ID, DEFAULT_RECEIVER_STAGE_ID, 1), _operator,
            ImmutableList.of(MAILBOX_1));
    OpChain chain2 =
        new OpChain(getOpChainExecutionContext(DEFAULT_REQUEST_ID, DEFAULT_RECEIVER_STAGE_ID, 2), _operator,
            ImmutableList.of(MAILBOX_2));
    OpChain chain3 =
        new OpChain(getOpChainExecutionContext(DEFAULT_REQUEST_ID, DEFAULT_RECEIVER_STAGE_ID, 3), _operator,
            ImmutableList.of(MAILBOX_3));

    // Register 3 OpChains. Keep release timeout high to avoid unintended OpChain wake-ups.
    _scheduler = new RoundRobinScheduler(10_000);
    _scheduler.register(chain1);
    _scheduler.register(chain2);
    _scheduler.register(chain3);

    // OpChains are returned in the order in which they were registered
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain1);
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain2);
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain3);
    // No op-chains ready, so scheduler returns null
    Assert.assertNull(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    // When Op-Chains are done executing, yield is called in any order
    _scheduler.yield(chain1);
    _scheduler.yield(chain3);
    _scheduler.yield(chain2);
    // Data may be received in arbitrary order
    _scheduler.onDataAvailable(MailboxIdUtils.toOpChainId(MAILBOX_2));
    _scheduler.onDataAvailable(MailboxIdUtils.toOpChainId(MAILBOX_3));
    _scheduler.onDataAvailable(MailboxIdUtils.toOpChainId(MAILBOX_1));
    // Subsequent polls would be in the order the callback was processed. A callback here is said to be "processed"
    // if it has successfully returned.
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain2);
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain3);
    Assert.assertEquals(_scheduler.next(DEFAULT_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), chain1);
    // De-register may be called in any order again
    _scheduler.deregister(chain3);
    _scheduler.deregister(chain2);
    _scheduler.deregister(chain1);

    // There should be no entries left in the scheduler after everything is done
    Assert.assertEquals(0,
        _scheduler.aliveChainsSize() + _scheduler.readySize() + _scheduler.seenMailSize() + _scheduler.availableSize());
  }

  private OpChainExecutionContext getOpChainExecutionContext(long requestId, int stageId, int virtualServerId) {
    return new OpChainExecutionContext(null, requestId, stageId,
        new VirtualServerAddress("localhost", 1234, virtualServerId), 0, null, null, true);
  }
}
