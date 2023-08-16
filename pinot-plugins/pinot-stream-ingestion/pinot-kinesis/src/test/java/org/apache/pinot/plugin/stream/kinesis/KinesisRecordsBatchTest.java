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
import org.testng.Assert;
import org.testng.annotations.Test;


public class KinesisRecordsBatchTest {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  @Test
  public void testMessageBatchAPIs() {
    int msgCount = 5;
    long baseTimeMs = System.currentTimeMillis();
    List<KinesisStreamMessage> msgList = new ArrayList<>(msgCount);


    for (int i = 0; i < msgCount; i++) {
      msgList.add(createStreamMessage(i, "key-" + i, "value-" + i, baseTimeMs + i));
    }
    KinesisRecordsBatch batch = new KinesisRecordsBatch(msgList, "shard-0000000000", false);

    for (int i = 0; i < msgCount; i++) {
      Assert.assertEquals(batch.getMessageLengthAtIndex(i), 7); // length of characters in "value-$i"
      Assert.assertEquals(batch.getMessageOffsetAtIndex(i), 0);

      KinesisPartitionGroupOffset kinesisPartitionGroupOffset =
          (KinesisPartitionGroupOffset) batch.getNextStreamPartitionMsgOffsetAtIndex(i);
      Assert.assertNotNull(kinesisPartitionGroupOffset);
      Assert.assertNotNull(kinesisPartitionGroupOffset.getShardToStartSequenceMap().get("shard-0000000000"),
          String.valueOf(i)); // why is "next" stream partition msg offset return the exact offset at index i ?

      KinesisStreamMessageMetadata metadata = (KinesisStreamMessageMetadata) batch.getMetadataAtIndex(i);
      Assert.assertNotNull(metadata);
      Assert.assertEquals(metadata.getRecordIngestionTimeMs(), baseTimeMs + i);
      Assert.assertEquals(batch.getMessageBytesAtIndex(i), ("value-" + i).getBytes(StandardCharsets.UTF_8));
    }

    // Batch level operations
    Assert.assertEquals(batch.getMessageCount(), msgCount);
    Assert.assertFalse(batch.isEndOfPartitionGroup());

    KinesisPartitionGroupOffset nextBatchOffset = (KinesisPartitionGroupOffset) batch.getOffsetOfNextBatch();
    Assert.assertNotNull(nextBatchOffset);
    Assert.assertNotNull(nextBatchOffset.getShardToStartSequenceMap().get("shard-0000000000"),
        String.valueOf(msgCount - 1));  // default implementation doesn't make sense ??
    Assert.assertEquals(batch.getUnfilteredMessageCount(), msgCount); // always size of the batch for Kinesis

    // unsupported operations for kinesis batch
    Assert.assertThrows(UnsupportedOperationException.class, () -> {
      batch.getNextStreamMessageOffsetAtIndex(0);
    });

    // batch.getMessageAtIndex(i); // deprecated
  }

  private static KinesisStreamMessage createStreamMessage(int sequenceNumber, String key, String value,
      long recordIngestionTimeMs) {
    KinesisStreamMessageMetadata metadata = new KinesisStreamMessageMetadata(recordIngestionTimeMs, null);
    byte[] valueArray = value != null ? value.getBytes(StandardCharsets.UTF_8) : EMPTY_BYTE_ARRAY;
    return new KinesisStreamMessage(
        key != null ? key.getBytes(StandardCharsets.UTF_8) : EMPTY_BYTE_ARRAY, valueArray,
        String.valueOf(sequenceNumber), metadata, valueArray.length);
  }
}
