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

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;


/// Verifies expiry cleanup with a controlled cache clock and no network resources or shared metrics registry.
public class MailboxServiceEvictionTest {
  @Test
  @SuppressWarnings("rawtypes") // The scoped CacheBuilder static mock requires its erased class token.
  public void testExpiredMailboxesCancelPendingAndEmptyReaders() {
    AtomicLong nanoseconds = new AtomicLong();
    Ticker ticker = new Ticker() {
      @Override
      public long read() {
        return nanoseconds.get();
      }
    };
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().ticker(ticker);
    try (MockedStatic<CacheBuilder> builders = mockStatic(CacheBuilder.class);
        MockedConstruction<ChannelManager> channels = mockConstruction(ChannelManager.class)) {
      builders.when(CacheBuilder::newBuilder).thenReturn(cacheBuilder);
      MailboxService service = new MailboxService("localhost", 0, InstanceType.CONTROLLER, new PinotConfiguration());
      ReceivingMailbox pending = service.getReceivingMailbox("pending");
      ReceivingMailbox empty = service.getReceivingMailbox("empty");
      pending.registeredReader(() -> {
      });
      empty.registeredReader(() -> {
      });
      DataSchema schema = new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.STRING});
      assertEquals(pending.offer(OperatorTestUtil.block(schema, new Object[]{"queued"}), List.of(), 1000),
          ReceivingMailbox.ReceivingMailboxStatus.SUCCESS);
      assertEquals(pending.getNumPendingBlocks(), 1);

      nanoseconds.set(TimeUnit.SECONDS.toNanos(301));
      ReceivingMailbox replacement = service.getReceivingMailbox("pending");
      ReceivingMailbox emptyReplacement = service.getReceivingMailbox("empty");
      assertNotSame(replacement, pending);
      assertNotSame(emptyReplacement, empty);
      assertEquals(pending.getNumPendingBlocks(), 0);
      assertTrue(pending.poll().getBlock().isError(), "Expiry must unblock readers and discard pending data");
      assertTrue(empty.poll().getBlock().isError(), "An empty dangling mailbox must also unblock its reader");
      service.releaseReceivingMailbox(replacement);
      service.releaseReceivingMailbox(emptyReplacement);
      assertEquals(channels.constructed().size(), 1);
    }
  }
}
