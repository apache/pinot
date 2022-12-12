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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.List;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.ScheduledOperator;
import org.apache.pinot.query.runtime.operator.V2Operator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoundRobinSchedulerTest {

  private static final MailboxIdentifier MAILBOX_1 = new StringMailboxIdentifier("1_1:foo:2:bar:3");
  private static final MailboxIdentifier MAILBOX_2 = new StringMailboxIdentifier("1_2:foo:2:bar:3");

  private static OpChain getOpChain(List<MailboxIdentifier> mail) {
    V2Operator mock = Mockito.mock(V2Operator.class);
    Mockito.when(mock.shouldSchedule(Mockito.any()))
        .thenAnswer(inv -> new ScheduledOperator.ScheduleResult(
            Sets.intersection(inv.getArgument(0), ImmutableSet.copyOf(mail))
        ));
    return new OpChain(mock, mail, 123, 1);
  }

  @Test
  public void shouldScheduleNewOpChainsImmediately() {
    // Given:
    OpChain chain = getOpChain(ImmutableList.of(MAILBOX_1));
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
    OpChain chain = getOpChain(ImmutableList.of(MAILBOX_1));
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);

    // Then:
    Assert.assertFalse(scheduler.hasNext());
  }

  @Test
  public void shouldScheduleRescheduledOpChainOnDataAvailable() {
    // Given:
    OpChain chain1 = getOpChain(ImmutableList.of(MAILBOX_1));
    OpChain chain2 = getOpChain(ImmutableList.of(MAILBOX_2));
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
    OpChain chain = getOpChain(ImmutableList.of(MAILBOX_1));
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
    OpChain chain = getOpChain(ImmutableList.of(MAILBOX_1));
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
    OpChain chain = getOpChain(ImmutableList.of(MAILBOX_1, MAILBOX_2));
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);
    scheduler.onDataAvailable(MAILBOX_2);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertEquals(scheduler.next(), chain);
    Assert.assertEquals(scheduler._seenMail.size(), 0);
  }

  @Test
  public void shouldOnlyRemoveMailFromScheduledMailboxes() {
    // Given:
    V2Operator mock = Mockito.mock(V2Operator.class);
    Mockito.when(mock.shouldSchedule(Mockito.any()))
        .thenAnswer(inv -> new ScheduledOperator.ScheduleResult(
            // only mailbox 1 should be scheduled
            ImmutableSet.copyOf(Sets.intersection(inv.getArgument(0), ImmutableSet.of(MAILBOX_1)))
        ));
    OpChain chain = new OpChain(mock, ImmutableList.of(MAILBOX_1, MAILBOX_2), 123, 1);
    RoundRobinScheduler scheduler = new RoundRobinScheduler();

    // When:
    scheduler.register(chain, false);
    scheduler.onDataAvailable(MAILBOX_1);
    scheduler.onDataAvailable(MAILBOX_2);

    // Then:
    Assert.assertTrue(scheduler.hasNext());
    Assert.assertTrue(scheduler._seenMail.contains(MAILBOX_2));
  }
}
