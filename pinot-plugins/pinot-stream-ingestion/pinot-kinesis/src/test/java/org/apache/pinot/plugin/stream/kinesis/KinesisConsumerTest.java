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
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
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
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;


public class KinesisConsumerTest {
  private static final String STREAM_TYPE = "kinesis";
  private static final String TABLE_NAME_WITH_TYPE = "kinesisTest_REALTIME";
  private static final String STREAM_NAME = "kinesis-test";
  private static final String AWS_REGION = "us-west-2";
  public static final int TIMEOUT = 1000;
  public static final int NUM_RECORDS = 10;
  public static final String DUMMY_RECORD_PREFIX = "DUMMY_RECORD-";
  public static final String PARTITION_KEY_PREFIX = "PARTITION_KEY-";
  public static final String PLACEHOLDER = "DUMMY";
  public static final int MAX_RECORDS_TO_FETCH = 20;

  private static KinesisConnectionHandler kinesisConnectionHandler;
  private static StreamConsumerFactory streamConsumerFactory;
  private static KinesisClient kinesisClient;
  private List<Record> recordList;

  private KinesisConfig getKinesisConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE);
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        STREAM_NAME);
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        StreamConfig.ConsumerType.LOWLEVEL.toString());
    props.put(StreamConfigProperties
            .constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        KinesisConsumerFactory.class.getName());
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    props.put(KinesisConfig.REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, String.valueOf(MAX_RECORDS_TO_FETCH));
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    return new KinesisConfig(new StreamConfig(TABLE_NAME_WITH_TYPE, props));
  }

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

    KinesisConsumer kinesisConsumer = new KinesisConsumer(getKinesisConfig(), kinesisClient);

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisPartitionGroupOffset kinesisPartitionGroupOffset = new KinesisPartitionGroupOffset(shardToSequenceMap);
    KinesisRecordsBatch kinesisRecordsBatch = kinesisConsumer.fetchMessages(kinesisPartitionGroupOffset, null, TIMEOUT);

    Assert.assertEquals(kinesisRecordsBatch.getMessageCount(), NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      Assert.assertEquals(baToString(kinesisRecordsBatch.getMessageAtIndex(i)), DUMMY_RECORD_PREFIX + i);
    }

    Assert.assertFalse(kinesisRecordsBatch.isEndOfPartitionGroup());
  }

  @Test
  public void testBasicConsumerWithMaxRecordsLimit() {
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

    KinesisConfig kinesisConfig = getKinesisConfig();
    KinesisConsumer kinesisConsumer = new KinesisConsumer(kinesisConfig, kinesisClient);

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisPartitionGroupOffset kinesisPartitionGroupOffset = new KinesisPartitionGroupOffset(shardToSequenceMap);
    KinesisRecordsBatch kinesisRecordsBatch = kinesisConsumer.fetchMessages(kinesisPartitionGroupOffset, null, TIMEOUT);

    Assert.assertEquals(kinesisRecordsBatch.getMessageCount(), MAX_RECORDS_TO_FETCH);

    for (int i = 0; i < NUM_RECORDS; i++) {
      Assert.assertEquals(baToString(kinesisRecordsBatch.getMessageAtIndex(i)), DUMMY_RECORD_PREFIX + i);
    }
  }

  @Test
  public void testBasicConsumerWithChildShard() {

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

    KinesisConfig kinesisConfig = getKinesisConfig();
    KinesisConsumer kinesisConsumer = new KinesisConsumer(kinesisConfig, kinesisClient);

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisPartitionGroupOffset kinesisPartitionGroupOffset = new KinesisPartitionGroupOffset(shardToSequenceMap);
    KinesisRecordsBatch kinesisRecordsBatch = kinesisConsumer.fetchMessages(kinesisPartitionGroupOffset, null, TIMEOUT);

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
