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
package org.apache.pinot.plugin.stream.kinesis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class KinesisMessageBatchTest {
  private static final String SHARD_ID = "shard-0000000000";

  @Test
  public void testMessageBatch() {
    int numMessages = 5;
    long baseTimeMs = System.currentTimeMillis();
    List<BytesStreamMessage> messages = new ArrayList<>(numMessages);
    for (int i = 0; i < numMessages; i++) {
      messages.add(createStreamMessage(i, "key-" + i, "value-" + i, baseTimeMs + i));
    }
    KinesisMessageBatch batch =
        new KinesisMessageBatch(messages, new KinesisPartitionGroupOffset(SHARD_ID, Integer.toString(numMessages - 1)),
            false);

    for (int i = 0; i < numMessages; i++) {
      BytesStreamMessage streamMessage = batch.getStreamMessage(i);
      byte[] key = streamMessage.getKey();
      assertNotNull(key);
      assertEquals(new String(key, StandardCharsets.UTF_8), "key-" + i);
      byte[] value = streamMessage.getValue();
      assertEquals(new String(value, StandardCharsets.UTF_8), "value-" + i);
      StreamMessageMetadata metadata = streamMessage.getMetadata();
      assertEquals(metadata.getRecordIngestionTimeMs(), baseTimeMs + i);
      StreamPartitionMsgOffset offset = metadata.getOffset();
      assertTrue(offset instanceof KinesisPartitionGroupOffset);
      assertEquals(((KinesisPartitionGroupOffset) offset).getShardId(), SHARD_ID);
      assertEquals(((KinesisPartitionGroupOffset) offset).getSequenceNumber(), Integer.toString(i));
      assertSame(metadata.getNextOffset(), offset);
    }

    // Batch level operations
    assertEquals(batch.getMessageCount(), numMessages);
    assertEquals(batch.getUnfilteredMessageCount(), numMessages);
    assertFalse(batch.isEndOfPartitionGroup());
    StreamPartitionMsgOffset nextBatchOffset = batch.getOffsetOfNextBatch();
    assertTrue(nextBatchOffset instanceof KinesisPartitionGroupOffset);
    assertEquals(((KinesisPartitionGroupOffset) nextBatchOffset).getShardId(), SHARD_ID);
    assertEquals(((KinesisPartitionGroupOffset) nextBatchOffset).getSequenceNumber(),
        Integer.toString(numMessages - 1));
  }

  private static BytesStreamMessage createStreamMessage(int sequenceNumber, String key, String value,
      long recordIngestionTimeMs) {
    KinesisPartitionGroupOffset offset = new KinesisPartitionGroupOffset(SHARD_ID, Integer.toString(sequenceNumber));
    StreamMessageMetadata metadata =
        new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(recordIngestionTimeMs).setOffset(offset, offset)
            .build();
    return new BytesStreamMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
        metadata);
  }
}
