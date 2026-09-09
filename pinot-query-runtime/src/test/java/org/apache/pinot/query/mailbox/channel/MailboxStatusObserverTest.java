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
package org.apache.pinot.query.mailbox.channel;

import java.util.Map;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/** Pins conservative per-stream IPC negotiation without changing legacy status handling. */
public class MailboxStatusObserverTest {
  @DataProvider
  public Object[][] versions() {
    return new Object[][]{
        {Map.of(), false},
        {Map.of(ChannelUtils.MAILBOX_METADATA_ARROW_IPC_VERSION, "true"), false},
        {Map.of(ChannelUtils.MAILBOX_METADATA_ARROW_IPC_VERSION, "2"), false},
        {Map.of(ChannelUtils.MAILBOX_METADATA_ARROW_IPC_VERSION, "99"), false},
        {Map.of(ChannelUtils.MAILBOX_METADATA_ARROW_IPC_VERSION, ChannelUtils.ARROW_IPC_VERSION), true}
    };
  }

  @Test(dataProvider = "versions")
  public void testOnlyExplicitSupportedVersionEnablesIpc(Map<String, String> metadata, boolean supported) {
    MailboxStatusObserver observer = new MailboxStatusObserver();
    assertFalse(observer.isArrowIpcSupported());
    observer.onNext(MailboxStatus.newBuilder().putAllMetadata(metadata).build());
    assertEquals(observer.isArrowIpcSupported(), supported);
    assertEquals(observer.getBufferSize(), 5);
  }

  @Test
  public void testLegacyFeedbackAndCapabilityWithdrawal() {
    MailboxStatusObserver observer = new MailboxStatusObserver();
    observer.onNext(MailboxStatus.newBuilder()
        .putMetadata(ChannelUtils.MAILBOX_METADATA_ARROW_IPC_VERSION, ChannelUtils.ARROW_IPC_VERSION)
        .putMetadata(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY, "2").build());
    assertTrue(observer.isArrowIpcSupported());
    assertEquals(observer.getBufferSize(), 2);
    observer.onNext(MailboxStatus.newBuilder()
        .putMetadata(ChannelUtils.MAILBOX_METADATA_REQUEST_EARLY_TERMINATE, "true").build());
    assertFalse(observer.isArrowIpcSupported());
    assertTrue(observer.isEarlyTerminated());
    assertEquals(observer.getBufferSize(), 5);
  }
}
