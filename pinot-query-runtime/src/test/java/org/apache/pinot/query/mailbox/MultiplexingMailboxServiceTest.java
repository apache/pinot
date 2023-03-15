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

import org.apache.pinot.query.routing.VirtualServerAddress;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiplexingMailboxServiceTest {

  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;
  private static final JsonMailboxIdentifier LOCAL_MAILBOX_ID = new JsonMailboxIdentifier(
      "localJobId", new VirtualServerAddress("localhost", 0, 0), new VirtualServerAddress("localhost", 0, 0),
      DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
  private static final JsonMailboxIdentifier NON_LOCAL_MAILBOX_ID = new JsonMailboxIdentifier(
      "localJobId", new VirtualServerAddress("localhost", 0, 0), new VirtualServerAddress("localhost", 1, 0),
      DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);

  @Test
  public void testHappyPath() {
    // Setup mock Grpc and InMemory mailbox service
    GrpcMailboxService grpcMailboxService = Mockito.mock(GrpcMailboxService.class);
    InMemoryMailboxService inMemoryMailboxService = Mockito.mock(InMemoryMailboxService.class);
    Mockito.doReturn("localhost").when(grpcMailboxService).getHostname();
    Mockito.doReturn("localhost").when(inMemoryMailboxService).getHostname();
    Mockito.doReturn(1000).when(grpcMailboxService).getMailboxPort();
    Mockito.doReturn(1000).when(inMemoryMailboxService).getMailboxPort();
    Mockito.doReturn(Mockito.mock(InMemorySendingMailbox.class)).when(inMemoryMailboxService).getSendingMailbox(
        Mockito.any(), Mockito.anyLong());
    Mockito.doReturn(Mockito.mock(InMemoryReceivingMailbox.class)).when(inMemoryMailboxService).getReceivingMailbox(
        Mockito.any());
    Mockito.doReturn(Mockito.mock(GrpcSendingMailbox.class)).when(grpcMailboxService).getSendingMailbox(
        Mockito.any(), Mockito.anyLong());
    Mockito.doReturn(Mockito.mock(GrpcReceivingMailbox.class)).when(grpcMailboxService).getReceivingMailbox(
        Mockito.any());

    // Create multiplex service with mocks
    MultiplexingMailboxService multiplexService = new MultiplexingMailboxService(grpcMailboxService,
        inMemoryMailboxService);

    // Ensure both underlying services are started
    multiplexService.start();
    Mockito.verify(grpcMailboxService, Mockito.times(1)).start();
    Mockito.verify(inMemoryMailboxService, Mockito.times(1)).start();

    // Ensure hostname and ports are returned accurately
    Assert.assertEquals("localhost", multiplexService.getHostname());
    Assert.assertEquals(1000, multiplexService.getMailboxPort());

    Assert.assertTrue(multiplexService.getSendingMailbox(LOCAL_MAILBOX_ID, -1) instanceof InMemorySendingMailbox);
    Assert.assertTrue(multiplexService.getSendingMailbox(NON_LOCAL_MAILBOX_ID, -1) instanceof GrpcSendingMailbox);

    Assert.assertTrue(multiplexService.getReceivingMailbox(LOCAL_MAILBOX_ID) instanceof InMemoryReceivingMailbox);
    Assert.assertTrue(multiplexService.getReceivingMailbox(NON_LOCAL_MAILBOX_ID) instanceof GrpcReceivingMailbox);

    multiplexService.shutdown();
    Mockito.verify(grpcMailboxService, Mockito.times(1)).shutdown();
    Mockito.verify(inMemoryMailboxService, Mockito.times(1)).shutdown();
  }

  @Test
  public void testInConsistentHostPort() {
    // Make the underlying services return different ports
    GrpcMailboxService grpcMailboxService = Mockito.mock(GrpcMailboxService.class);
    InMemoryMailboxService inMemoryMailboxService = Mockito.mock(InMemoryMailboxService.class);
    Mockito.doReturn("localhost").when(grpcMailboxService).getHostname();
    Mockito.doReturn("localhost").when(inMemoryMailboxService).getHostname();
    Mockito.doReturn(1000).when(grpcMailboxService).getMailboxPort();
    Mockito.doReturn(1001).when(inMemoryMailboxService).getMailboxPort();

    try {
      new MultiplexingMailboxService(grpcMailboxService, inMemoryMailboxService);
      Assert.fail("Method call above should have failed");
    } catch (IllegalStateException ignored) {
    }
  }
}
