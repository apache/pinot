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
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.channel.GrpcMailboxServer;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Tests service-owned Arrow bootstrap and shutdown without opening network listeners. */
public class MailboxArrowLifecycleTest {
  @Test
  public void testDefaultServiceIsDisabledAndShutdownIsIdempotent() {
    MailboxService mailbox = new MailboxService("localhost", 0, InstanceType.CONTROLLER, new PinotConfiguration());
    try {
      assertFalse(mailbox.isArrowEnabled());
      assertFalse(mailbox.getArrowBuffers().isEnabled());
      assertSame(mailbox.getArrowBuffers(), mailbox.getArrowBuffers());
    } finally {
      mailbox.shutdown();
      mailbox.shutdown();
    }
  }

  @Test
  public void testShutdownDoesNotCloseARunningAttemptAllocator() {
    PinotConfiguration config = new PinotConfiguration(
        Map.of(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW, true));
    MailboxService mailbox = new MailboxService("localhost", 0, InstanceType.CONTROLLER, config);
    ArrowBuffers buffers = mailbox.getArrowBuffers();
    try (ArrowQueryContext context = buffers.newQueryContext("running")) {
      assertTrue(mailbox.isArrowEnabled());
      DataSchema schema = new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
      ArrowBlock block = ArrowBlockConverter.toArrowBlock(
          new RowHeapDataBlock(List.<Object[]>of(new Object[]{7}), schema), context);
      mailbox.shutdown();
      expectThrows(IllegalStateException.class, () -> buffers.newQueryContext("late"));
      assertEquals(block.getDataBlock().getInt(0, 0), 7);
      block.release();
      context.close();
      assertEquals(buffers.getAllocatedMemory(), 0L);
    } finally {
      mailbox.shutdown();
    }
  }

  @Test
  public void testFailedStartupClosesArrowBuffers() {
    PinotConfiguration config = new PinotConfiguration(
        Map.of(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW, true));
    MailboxService mailbox = new MailboxService("localhost", 0, InstanceType.CONTROLLER, config);
    IllegalStateException failure = new IllegalStateException("startup failed");
    try (MockedConstruction<GrpcMailboxServer> servers = mockConstruction(GrpcMailboxServer.class,
        (server, context) -> doThrow(failure).when(server).start())) {
      assertSame(expectThrows(IllegalStateException.class, mailbox::start), failure);
      assertEquals(servers.constructed().size(), 1);
      expectThrows(IllegalStateException.class, () -> mailbox.getArrowBuffers().newQueryContext("late"));
      assertEquals(mailbox.getArrowBuffers().getAllocatedMemory(), 0L);
    } finally {
      mailbox.shutdown();
    }
  }
}
