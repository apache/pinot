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
package org.apache.pinot.query.mailbox;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.ReceivingMailbox.ReceivingMailboxStatus;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies blocked writers against real queue state transitions, waiting for the producer to enter its timed wait.
/// Each test owns and shuts down its producer executor.
public class ReceivingMailboxStateTransitionTest {
  private enum Transition {
    SPACE_AVAILABLE, SUCCESS_EOS, ERROR_EOS, EARLY_TERMINATION
  }

  @Test(timeOut = 10_000)
  public void testBlockedWriterResumesAfterTheReaderFreesSpace()
      throws Exception {
    assertBlockedWriterTransition(Transition.SPACE_AVAILABLE);
  }

  @Test(timeOut = 10_000)
  public void testBlockedWriterIsReadBeforeSuccessfulEos()
      throws Exception {
    assertBlockedWriterTransition(Transition.SUCCESS_EOS);
  }

  @Test(timeOut = 10_000)
  public void testErrorEosRejectsTheBlockedWriterAndDiscardsData()
      throws Exception {
    assertBlockedWriterTransition(Transition.ERROR_EOS);
  }

  @Test(timeOut = 10_000)
  public void testEarlyTerminationRejectsTheBlockedWriterButAcceptsEos()
      throws Exception {
    assertBlockedWriterTransition(Transition.EARLY_TERMINATION);
  }

  private static void assertBlockedWriterTransition(Transition transition)
      throws Exception {
    DataSchema schema = new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.INT});
    MseBlock.Data first = new RowHeapDataBlock(List.<Object[]>of(new Object[]{1}), schema);
    MseBlock.Data second = new RowHeapDataBlock(List.<Object[]>of(new Object[]{2}), schema);
    ReceivingMailbox mailbox = new ReceivingMailbox("blocked-" + transition, 1);
    mailbox.registeredReader(mock(ReceivingMailbox.Reader.class));
    assertEquals(mailbox.offer(first, List.of(), 30_000), ReceivingMailboxStatus.SUCCESS);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> producer = new AtomicReference<>();
    try {
      Future<ReceivingMailboxStatus> pending = executor.submit(() -> {
        producer.set(Thread.currentThread());
        return mailbox.offer(second, List.of(), 30_000);
      });
      TestUtils.waitForCondition(ignored -> {
        Thread thread = producer.get();
        return thread != null && thread.getState() == Thread.State.TIMED_WAITING;
      }, 5_000, "Producer did not wait for space in the full mailbox");
      assertFalse(pending.isDone());

      MseBlock.Eos eos = transition == Transition.ERROR_EOS
          ? ErrorMseBlock.fromError(QueryErrorCode.INTERNAL, "upstream failed")
          : SuccessMseBlock.INSTANCE;
      if (transition == Transition.EARLY_TERMINATION) {
        mailbox.earlyTerminate();
      } else if (transition != Transition.SPACE_AVAILABLE) {
        assertEquals(mailbox.offer(eos, List.of(), 30_000), ReceivingMailboxStatus.LAST_BLOCK);
      }

      if (transition == Transition.SPACE_AVAILABLE || transition == Transition.SUCCESS_EOS) {
        assertSame(mailbox.poll().getBlock(), first);
        assertEquals(pending.get(5, TimeUnit.SECONDS), ReceivingMailboxStatus.SUCCESS);
        assertSame(mailbox.poll().getBlock(), second);
      } else {
        assertEquals(pending.get(5, TimeUnit.SECONDS), transition == Transition.ERROR_EOS
            ? ReceivingMailboxStatus.ALREADY_TERMINATED
            : ReceivingMailboxStatus.WAITING_EOS);
      }
      assertEquals(mailbox.getNumPendingBlocks(), 0);

      if (transition == Transition.SPACE_AVAILABLE) {
        assertNull(mailbox.poll());
      } else {
        if (transition == Transition.EARLY_TERMINATION) {
          assertNull(mailbox.poll());
          assertEquals(mailbox.offer(eos, List.of(), 30_000), ReceivingMailboxStatus.LAST_BLOCK);
        }
        assertSame(mailbox.poll().getBlock(), eos);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Producer executor did not stop");
    }
  }
}
