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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.easymock.Capture;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;


public class KinesisConsumerTest {
  public static final int TIMEOUT = 1000;
  public static final int NUM_RECORDS = 10;
  public static final String DUMMY_RECORD_PREFIX = "DUMMY_RECORD-";
  public static final String PARTITION_KEY_PREFIX = "PARTITION_KEY-";
  public static final String PLACEHOLDER = "DUMMY";

  private static KinesisConnectionHandler kinesisConnectionHandler;
  private static StreamConsumerFactory streamConsumerFactory;
  private static KinesisClient kinesisClient;
  private List<Record> recordList;

  @BeforeMethod
  public void setupTest() {
    kinesisConnectionHandler = createMock(KinesisConnectionHandler.class);
    kinesisClient = createMock(KinesisClient.class);
    streamConsumerFactory = createMock(StreamConsumerFactory.class);

    recordList = new ArrayList<>();

    for (int i = 0; i < NUM_RECORDS; i++) {
      Record record =
          Record.builder().data(SdkBytes.fromUtf8String(DUMMY_RECORD_PREFIX + i)).partitionKey(PARTITION_KEY_PREFIX + i)
              .sequenceNumber(String.valueOf(i + 1)).build();
      recordList.add(record);
    }
  }

  @Test
  public void testBasicConsumer() {
    Capture<GetRecordsRequest> getRecordsRequestCapture = Capture.newInstance();
    Capture<GetShardIteratorRequest> getShardIteratorRequestCapture = Capture.newInstance();

    GetRecordsResponse getRecordsResponse =
        GetRecordsResponse.builder().nextShardIterator(null).records(recordList).build();
    GetShardIteratorResponse getShardIteratorResponse =
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build();

    expect(kinesisClient.getRecords(capture(getRecordsRequestCapture))).andReturn(getRecordsResponse).anyTimes();
    expect(kinesisClient.getShardIterator(capture(getShardIteratorRequestCapture))).andReturn(getShardIteratorResponse)
        .anyTimes();

    replay(kinesisClient);

    KinesisConsumer kinesisConsumer = new KinesisConsumer(TestUtils.getKinesisConfig(), kinesisClient);

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shardToSequenceMap);
    KinesisRecordsBatch kinesisRecordsBatch = kinesisConsumer.fetchMessages(kinesisCheckpoint, null, TIMEOUT);

    Assert.assertEquals(kinesisRecordsBatch.getMessageCount(), NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      Assert.assertEquals(baToString(kinesisRecordsBatch.getMessageAtIndex(i)), DUMMY_RECORD_PREFIX + i);
    }

    Assert.assertFalse(kinesisRecordsBatch.isEndOfPartitionGroup());
  }

  @Test
  public void testBasicConsumerWithMaxRecordsLimit() {
    int maxRecordsLimit = 20;
    Capture<GetRecordsRequest> getRecordsRequestCapture = Capture.newInstance();
    Capture<GetShardIteratorRequest> getShardIteratorRequestCapture = Capture.newInstance();

    GetRecordsResponse getRecordsResponse =
        GetRecordsResponse.builder().nextShardIterator(PLACEHOLDER).records(recordList).build();
    GetShardIteratorResponse getShardIteratorResponse =
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build();

    expect(kinesisClient.getRecords(capture(getRecordsRequestCapture))).andReturn(getRecordsResponse).anyTimes();
    expect(kinesisClient.getShardIterator(capture(getShardIteratorRequestCapture))).andReturn(getShardIteratorResponse)
        .anyTimes();

    replay(kinesisClient);

    KinesisConfig kinesisConfig = TestUtils.getKinesisConfig();
    kinesisConfig.setMaxRecordsToFetch(maxRecordsLimit);
    KinesisConsumer kinesisConsumer = new KinesisConsumer(kinesisConfig, kinesisClient);

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shardToSequenceMap);
    KinesisRecordsBatch kinesisRecordsBatch = kinesisConsumer.fetchMessages(kinesisCheckpoint, null, TIMEOUT);

    Assert.assertEquals(kinesisRecordsBatch.getMessageCount(), maxRecordsLimit);

    for (int i = 0; i < NUM_RECORDS; i++) {
      Assert.assertEquals(baToString(kinesisRecordsBatch.getMessageAtIndex(i)), DUMMY_RECORD_PREFIX + i);
    }
  }

  @Test
  public void testBasicConsumerWithChildShard() {
    int maxRecordsLimit = 20;

    List<ChildShard> shardList = new ArrayList<>();
    shardList.add(ChildShard.builder().shardId(PLACEHOLDER).parentShards("0").build());

    Capture<GetRecordsRequest> getRecordsRequestCapture = Capture.newInstance();
    Capture<GetShardIteratorRequest> getShardIteratorRequestCapture = Capture.newInstance();

    GetRecordsResponse getRecordsResponse =
        GetRecordsResponse.builder().nextShardIterator(null).records(recordList).childShards(shardList).build();
    GetShardIteratorResponse getShardIteratorResponse =
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build();

    expect(kinesisClient.getRecords(capture(getRecordsRequestCapture))).andReturn(getRecordsResponse).anyTimes();
    expect(kinesisClient.getShardIterator(capture(getShardIteratorRequestCapture))).andReturn(getShardIteratorResponse)
        .anyTimes();

    replay(kinesisClient);

    KinesisConfig kinesisConfig = TestUtils.getKinesisConfig();
    kinesisConfig.setMaxRecordsToFetch(maxRecordsLimit);
    KinesisConsumer kinesisConsumer = new KinesisConsumer(kinesisConfig, kinesisClient);

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shardToSequenceMap);
    KinesisRecordsBatch kinesisRecordsBatch = kinesisConsumer.fetchMessages(kinesisCheckpoint, null, TIMEOUT);

    Assert.assertTrue(kinesisRecordsBatch.isEndOfPartitionGroup());
    Assert.assertEquals(kinesisRecordsBatch.getMessageCount(), NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      Assert.assertEquals(baToString(kinesisRecordsBatch.getMessageAtIndex(i)), DUMMY_RECORD_PREFIX + i);
    }
  }

  public String baToString(byte[] bytes) {
    return SdkBytes.fromByteArray(bytes).asUtf8String();
  }
}
