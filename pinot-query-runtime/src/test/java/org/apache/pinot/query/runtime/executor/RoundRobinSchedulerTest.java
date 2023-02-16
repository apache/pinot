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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RoundRobinSchedulerTest {
  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;

  private static final MailboxIdentifier MAILBOX_1 = new JsonMailboxIdentifier("1_1", "0@foo:2", "0@bar:3",
      DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
  private static final MailboxIdentifier MAILBOX_2 = new JsonMailboxIdentifier("1_2", "0@foo:2", "0@bar:3",
      DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);

  @Mock
  private MultiStageOperator _operator;

  private AutoCloseable _mocks;

  @BeforeClass
  public void beforeClass() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldScheduleNewOpChainsImmediately() {
    // Given:
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, DEFAULT_RECEIVER_STAGE_ID);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, true);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertEquals(scheduler.next(), chain);
  }

  @Test
  public void shouldNotScheduleRescheduledOpChainsImmediately() {
    // Given:
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, DEFAULT_RECEIVER_STAGE_ID);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);

    // Then:
    Assert.assertFalse(scheduler.hasNext());
  }

  @Test
  public void shouldScheduleRescheduledOpChainOnDataAvailable() {
    // Given:
    OpChain chain1 = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, DEFAULT_RECEIVER_STAGE_ID);
    OpChain chain2 = new OpChain(_operator, ImmutableList.of(MAILBOX_2), 123, DEFAULT_RECEIVER_STAGE_ID);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain1, false);
    scheduler.register(chain2, false);
    scheduler.onDataAvailable(MAILBOX_1);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertEquals(scheduler.next(), chain1);
    Assert.assertFalse(scheduler.hasNext());
  }

  @Test
  public void shouldScheduleRescheduledOpChainAfterTimeout() {
    // Given:
    OpChain chain1 = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, DEFAULT_RECEIVER_STAGE_ID);
    AtomicLong ticker = new AtomicLong(0);
    RoundRobinScheduler scheduler = new RoundRobinScheduler(100, ticker::get);

    // When:
    scheduler.register(chain1, false);
    ticker.set(101);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertEquals(scheduler.next(), chain1);
  }

  @Test
  public void shouldScheduleRescheduledOpChainOnDataAvailableBeforeRegister() {
    // Given:
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, DEFAULT_RECEIVER_STAGE_ID);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.onDataAvailable(MAILBOX_1);
    scheduler.register(chain, false);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertEquals(scheduler.next(), chain);
  }

  @Test
  public void shouldNotScheduleRescheduledOpChainOnDataAvailableForDifferentMailbox() {
    // Given:
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, DEFAULT_RECEIVER_STAGE_ID);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);
    scheduler.onDataAvailable(MAILBOX_2);

    // Then:
    Assert.assertFalse(scheduler.hasNext());
  }

  @Test
  public void shouldScheduleRescheduledOpChainOnDataAvailableForAnyMailbox() {
    // Given:
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1, MAILBOX_2), 123, DEFAULT_RECEIVER_STAGE_ID);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);
    scheduler.onDataAvailable(MAILBOX_2);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertEquals(scheduler.next(), chain);
    Assert.assertEquals(scheduler._seenMail.size(), 0);
  }
}
