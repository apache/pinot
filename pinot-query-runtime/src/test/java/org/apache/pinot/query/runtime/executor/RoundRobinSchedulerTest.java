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
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RoundRobinSchedulerTest {

  private static final MailboxIdentifier MAILBOX_1 = new StringMailboxIdentifier("1_1:foo:2:bar:3");
  private static final MailboxIdentifier MAILBOX_2 = new StringMailboxIdentifier("1_2:foo:2:bar:3");

  @Mock
  private Operator<TransferableBlock> _operator;

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
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, 1);
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
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, 1);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);

    // Then:
    Assert.assertFalse(scheduler.hasNext());
  }

  @Test
  public void shouldScheduleRescheduledOpChainOnDataAvailable() {
    // Given:
    OpChain chain1 = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, 1);
    OpChain chain2 = new OpChain(_operator, ImmutableList.of(MAILBOX_2), 123, 1);
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
  public void shouldScheduleRescheduledOpChainOnDataAvailableBeforeRegister() {
    // Given:
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, 1);
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
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1), 123, 1);
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
    OpChain chain = new OpChain(_operator, ImmutableList.of(MAILBOX_1, MAILBOX_2), 123, 1);
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
