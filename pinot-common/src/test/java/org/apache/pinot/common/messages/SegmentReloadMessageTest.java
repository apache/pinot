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
package org.apache.pinot.common.messages;

import java.util.List;
import org.apache.helix.model.Message;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class SegmentReloadMessageTest {

  @Test
  public void testReloadJobIdDefaultToMessageId() {
    SegmentReloadMessage message = new SegmentReloadMessage("testTable_OFFLINE", false);

    assertFalse(message.shouldForceDownload());
    assertEquals(message.getReloadJobId(), message.getMsgId());
  }

  @Test
  public void testReloadJobIdOverrideAndSegments() {
    List<String> segments = List.of("seg_1", "seg_2");
    SegmentReloadMessage message = new SegmentReloadMessage("testTable_OFFLINE", segments, true, "job-123");

    assertTrue(message.shouldForceDownload());
    assertEquals(message.getSegmentList(), segments);
    assertEquals(message.getReloadJobId(), "job-123");
  }

  @Test
  public void testInvalidSubtypeThrows() {
    Message rawMessage = new Message(Message.MessageType.USER_DEFINE_MSG, "message-id");
    rawMessage.setMsgSubType("INVALID_SUBTYPE");

    assertThrows(IllegalArgumentException.class, () -> new SegmentReloadMessage(rawMessage));
  }
}
