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
package org.apache.pinot.plugin.stream.pulsar;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PulsarMessageIdTest {

  @Test
  public void testNonBatchedMessageId()
      throws Exception {
    testAgainstBuiltInMessageId(new PulsarMessageId(1L << 40, 12345, 3));
  }

  @Test
  public void testNonPartitionedMessageId()
      throws Exception {
    testAgainstBuiltInMessageId(new PulsarMessageId(1L << 40, 12345, -1));
  }

  @Test
  public void testBatchedMessageId()
      throws Exception {
    testAgainstBuiltInMessageId(new PulsarMessageId(1L << 40, 12345, 3, 7, 10));
  }

  /// Deserializes the message id into the built-in Pulsar message id, and checks that the two are interchangeable.
  private static void testAgainstBuiltInMessageId(PulsarMessageId messageId)
      throws Exception {
    byte[] bytes = messageId.toByteArray();
    MessageIdAdv builtInMessageId = (MessageIdAdv) MessageId.fromByteArray(bytes);
    assertEquals(builtInMessageId.getLedgerId(), messageId.getLedgerId());
    assertEquals(builtInMessageId.getEntryId(), messageId.getEntryId());
    assertEquals(builtInMessageId.getPartitionIndex(), messageId.getPartitionIndex());
    assertEquals(builtInMessageId.getBatchIndex(), messageId.getBatchIndex());
    assertEquals(builtInMessageId.getBatchSize(), messageId.getBatchSize());
    assertEquals(builtInMessageId.toByteArray(), bytes);
    assertEquals(builtInMessageId, messageId);
    assertEquals(builtInMessageId.hashCode(), messageId.hashCode());
    assertEquals(builtInMessageId.compareTo(messageId), 0);
    assertEquals(builtInMessageId.toString(), messageId.toString());
  }
}
