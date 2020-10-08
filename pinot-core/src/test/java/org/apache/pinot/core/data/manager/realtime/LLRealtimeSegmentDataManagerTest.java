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
package org.apache.pinot.core.data.manager.realtime;

import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.config.InstanceDataManagerConfig;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMessageDecoder;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.upsert.TableUpsertMetadataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO Re-write this test using the stream abstraction
public class LLRealtimeSegmentDataManagerTest {
  private static final String _segmentDir = "/tmp/" + LLRealtimeSegmentDataManagerTest.class.getSimpleName();
  private static final File _segmentDirFile = new File(_segmentDir);
  private static final String _tableName = "Coffee";
  private static final int _partitionId = 13;
  private static final int _sequenceId = 945;
  private static final long _segTimeMs = 98347869999L;
  private static final LLCSegmentName _segmentName =
      new LLCSegmentName(_tableName, _partitionId, _sequenceId, _segTimeMs);
  private static final String _segmentNameStr = _segmentName.getSegmentName();
  private static final long _startOffsetValue = 19885L;
  private static final LongMsgOffset _startOffset = new LongMsgOffset(_startOffsetValue);
  private static final String _topicName = "someTopic";
  private static final int maxRowsInSegment = 250000;
  private static final long maxTimeForSegmentCloseMs = 64368000L;
  private final Map<Integer, Semaphore> _partitionIdToSemaphoreMap = new ConcurrentHashMap<>();

  private static long _timeNow = System.currentTimeMillis();

  private final String _tableConfigJson =
      "{\n" + "  \"metadata\": {}, \n" + "  \"segmentsConfig\": {\n" + "    \"replicasPerPartition\": \"3\", \n"
          + "    \"replication\": \"3\", \n" + "    \"replicationNumber\": 3, \n"
          + "    \"retentionTimeUnit\": \"DAYS\", \n" + "    \"retentionTimeValue\": \"3\", \n"
          + "    \"schemaName\": \"UnknownSchema\", \n"
          + "    \"segmentAssignmentStrategy\": \"BalanceNumSegmentAssignmentStrategy\", \n"
          + "    \"segmentPushFrequency\": \"daily\", \n" + "    \"segmentPushType\": \"APPEND\", \n"
          + "    \"timeColumnName\": \"minutesSinceEpoch\", \n" + "    \"timeType\": \"MINUTES\"\n" + "  }, \n"
          + "  \"tableIndexConfig\": {\n" + "    \"invertedIndexColumns\": [" + "    ], \n"
          + "    \"lazyLoad\": \"false\", \n" + "    \"loadMode\": \"HEAP\", \n"
          + "    \"segmentFormatVersion\": null, \n" + "    \"sortedColumn\": [], \n" + "    \"streamConfigs\": {\n"
          + "      \"" + StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + "\": \"" + maxRowsInSegment + "\", \n" + "      \"" + StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME
          + "\": \"" + maxTimeForSegmentCloseMs + "\", \n"
          + "      \"stream.fakeStream.broker.list\": \"broker:7777\", \n"
          + "      \"stream.fakeStream.consumer.prop.auto.offset.reset\": \"smallest\", \n"
          + "      \"stream.fakeStream.consumer.type\": \"simple\", \n"
          + "      \"stream.fakeStream.consumer.factory.class.name\": \"" + FakeStreamConsumerFactory.class.getName()
          + "\", \n" + "      \"stream.fakeStream.decoder.class.name\": \"" + FakeStreamMessageDecoder.class.getName()
          + "\", \n"
          + "      \"stream.fakeStream.decoder.prop.schema.registry.rest.url\": \"http://schema-registry-host.corp.ceo:1766/schemas\", \n"
          + "      \"stream.fakeStream.decoder.prop.schema.registry.schema.name\": \"UnknownSchema\", \n"
          + "      \"stream.fakeStream.hlc.zk.connect.string\": \"zoo:2181/kafka-queuing\", \n"
          + "      \"stream.fakeStream.topic.name\": \"" + _topicName + "\", \n"
          + "      \"stream.fakeStream.zk.broker.url\": \"kafka-broker:2181/kafka-queuing\", \n"
          + "      \"streamType\": \"fakeStream\"\n" + "    }\n" + "  }, \n"
          + "  \"tableName\": \"Coffee_REALTIME\", \n" + "  \"tableType\": \"realtime\", \n" + "  \"tenants\": {\n"
          + "    \"broker\": \"shared\", \n" + "    \"server\": \"server-1\"\n" + "  },\n"
          + " \"upsertConfig\": {\"mode\": \"FULL\" } \n"
          + "}";

  private String makeSchema() {
    return "{" + "  \"schemaName\":\"SchemaTest\"," + "  \"metricFieldSpecs\":[" + "    {\"name\":\"m\",\"dataType\":\""
        + "LONG" + "\"}" + "  ]," + "  \"dimensionFieldSpecs\":[" + "    {\"name\":\"d\",\"dataType\":\"" + "STRING"
        + "\",\"singleValueField\":" + "true" + "}" + "  ]," + "  \"timeFieldSpec\":{"
        + "    \"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"time\"},"
        + "    \"defaultNullValue\":12345" + "  },\n"
        + "\"primaryKeyColumns\": [\"event_id\"] \n"
        + "}";
  }

  private TableConfig createTableConfig()
      throws Exception {
    return JsonUtils.stringToObject(_tableConfigJson, TableConfig.class);
  }

  private RealtimeTableDataManager createTableDataManager() {
    final String instanceId = "server-1";
    SegmentBuildTimeLeaseExtender.create(instanceId, new ServerMetrics(new MetricsRegistry()), _tableName);
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.getServerInstance()).thenReturn(instanceId);
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    when(tableDataManager.getStatsHistory()).thenReturn(statsHistory);
    return tableDataManager;
  }

  private LLCRealtimeSegmentZKMetadata createZkMetadata() {

    LLCRealtimeSegmentZKMetadata segmentZKMetadata = new LLCRealtimeSegmentZKMetadata();
    segmentZKMetadata.setSegmentName(_segmentNameStr);
    segmentZKMetadata.setStartOffset(_startOffset.toString());
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    return segmentZKMetadata;
  }

  private FakeLLRealtimeSegmentDataManager createFakeSegmentManager()
      throws Exception {
    LLCRealtimeSegmentZKMetadata segmentZKMetadata = createZkMetadata();
    TableConfig tableConfig = createTableConfig();
    InstanceZKMetadata instanceZKMetadata = new InstanceZKMetadata();
    RealtimeTableDataManager tableDataManager = createTableDataManager();
    String resourceDir = _segmentDir;
    LLCSegmentName llcSegmentName = new LLCSegmentName(_segmentNameStr);
    _partitionIdToSemaphoreMap.putIfAbsent(_partitionId, new Semaphore(1));
    Schema schema = Schema.fromString(makeSchema());
    ServerMetrics serverMetrics = new ServerMetrics(new MetricsRegistry());
    FakeLLRealtimeSegmentDataManager segmentDataManager =
        new FakeLLRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, tableDataManager, resourceDir, schema,
            llcSegmentName, _partitionIdToSemaphoreMap, serverMetrics);
    return segmentDataManager;
  }

  @BeforeClass
  public void setUp() {
    _segmentDirFile.deleteOnExit();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_segmentDirFile);
  }

  @Test
  public void testOffsetParsing() throws Exception {
    final String offset = "34";
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    {
      //  Controller sends catchup response with both offset as well as streamPartitionMsgOffset
      String responseStr =
          "{"
              + "  \"streamPartitionMsgOffset\" : \"" + offset + "\","
              + "  \"offset\" : " + offset + ","
              + "  \"buildTimeSec\" : -1,"
              + "  \"isSplitCommitType\" : false,"
              + "  \"segmentLocation\" : \"file:///a/b\","
              + "  \"status\" : \"CATCH_UP\""
              + "}";
      SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      StreamPartitionMsgOffset extractedOffset = segmentDataManager.extractOffset(response);
      Assert.assertEquals(extractedOffset.compareTo(new LongMsgOffset(offset)), 0);
    }
    {
      //  Controller sends catchup response with offset only
      String responseStr =
          "{"
              + "  \"offset\" : " + offset + ","
              + "  \"buildTimeSec\" : -1,"
              + "  \"isSplitCommitType\" : false,"
              + "  \"segmentLocation\" : \"file:///a/b\","
              + "  \"status\" : \"CATCH_UP\""
              + "}";
      SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      StreamPartitionMsgOffset extractedOffset = segmentDataManager.extractOffset(response);
      Assert.assertEquals(extractedOffset.compareTo(new LongMsgOffset(offset)), 0);
    }
    {
      //  Controller sends catchup response streamPartitionMsgOffset only
      String responseStr =
          "{"
              + "  \"streamPartitionMsgOffset\" : \"" + offset + "\","
              + "  \"buildTimeSec\" : -1,"
              + "  \"isSplitCommitType\" : false,"
              + "  \"segmentLocation\" : \"file:///a/b\","
              + "  \"status\" : \"CATCH_UP\""
              + "}";
      SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      StreamPartitionMsgOffset extractedOffset = segmentDataManager.extractOffset(response);
      Assert.assertEquals(extractedOffset.compareTo(new LongMsgOffset(offset)), 0);
    }
    segmentDataManager.destroy();
  }

  // Test that we are in HOLDING state as long as the controller responds HOLD to our segmentConsumed() message.
  // we should not consume when holding.
  @Test
  public void testHolding()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(_startOffsetValue + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params()
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(endOffset.toString()));
    // And then never consume as long as we get a hold response, 100 times.
    for (int i = 0; i < 100; i++) {
      segmentDataManager._responses.add(response);
    }

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.destroy();
  }

  // Test that we go to commit when the controller responds commit after 2 holds.
  @Test
  public void testCommitAfterHold()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(_startOffsetValue + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response holdResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert
        .assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.destroy();
  }

  @Test
  public void testSegmentBuildException()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(_startOffsetValue + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    segmentDataManager._responses.add(commitResponse);
    segmentDataManager._failSegmentBuild = true;

    consumer.run();
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.ERROR);
    segmentDataManager.destroy();
  }

  // Test hold, catchup. hold, commit
  @Test
  public void testCommitAfterCatchup()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(_startOffsetValue + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params()
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD).
            withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params()
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert
        .assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.destroy();
  }

  @Test
  public void testDiscarded()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(_startOffsetValue + 500);
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response discardResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.DISCARD));
    segmentDataManager._responses.add(discardResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert
        .assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.DISCARDED);
    segmentDataManager.destroy();
  }

  @Test
  public void testRetained()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(_startOffsetValue + 500);
    segmentDataManager._consumeOffsets.add(endOffset);
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStreamPartitionMsgOffset(endOffset.toString()).
        withStatus(SegmentCompletionProtocol.ControllerResponseStatus.KEEP);
    final SegmentCompletionProtocol.Response keepResponse = new SegmentCompletionProtocol.Response(params);
    segmentDataManager._responses.add(keepResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.RETAINED);
    segmentDataManager.destroy();
  }

  @Test
  public void testNotLeader()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(_startOffsetValue + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER));
    // And then never consume as long as we get a Not leader response, 100 times.
    for (int i = 0; i < 100; i++) {
      segmentDataManager._responses.add(response);
    }

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.destroy();
  }

  @Test
  public void testConsumingException()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();

    segmentDataManager._throwExceptionFromConsume = true;
    segmentDataManager._postConsumeStoppedCalled = false;
    consumer.run();
    Assert.assertTrue(segmentDataManager._postConsumeStoppedCalled);
    segmentDataManager.destroy();
  }

  // Tests to go online from consuming state

  // If the state is is COMMITTED or RETAINED, nothing to do
  // If discarded or error state, then downloadAndReplace the segment
  @Test
  public void testOnlineTransitionAfterStop()
      throws Exception {
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    final long finalOffsetValue = _startOffsetValue + 600;
    final LongMsgOffset finalOffset = new LongMsgOffset(finalOffsetValue);
    metadata.setEndOffset(finalOffset.toString());

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.COMMITTED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.RETAINED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.DISCARDED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.ERROR);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    // If holding, but we have overshot the expected final offset, the download and replace
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager.setCurrentOffset(finalOffsetValue + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    // If catching up, but we have overshot the expected final offset, the download and replace
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager.setCurrentOffset(finalOffsetValue + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    // If catching up, but we did not get to the final offset, then download and replace
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(new LongMsgOffset(finalOffsetValue - 1));
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }

    // But then if we get to the exact offset, we get to build and replace, not download
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(finalOffset);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      segmentDataManager.destroy();
    }
  }

  @Test
  public void testEndCriteriaChecking()
      throws Exception {
    // test reaching max row limit
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsIndexed(maxRowsInSegment - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsIndexed(maxRowsInSegment);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_ROW_LIMIT);
      segmentDataManager.destroy();
    }
    // test reaching max time limit
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      _timeNow += maxTimeForSegmentCloseMs + 1;
      // We should still get false, since the number of records in the realtime segment is 0
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      replaceRealtimeSegment(segmentDataManager, 10);
      // Now we can test when we are far ahead in time
      _timeNow += maxTimeForSegmentCloseMs;
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_TIME_LIMIT);
      segmentDataManager.destroy();
    }
    // In catching up state, test reaching final offset
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = _startOffsetValue + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.destroy();
    }
    // In catching up state, test reaching final offset ignoring time
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      _timeNow += maxTimeForSegmentCloseMs;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = _startOffsetValue + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.destroy();
    }
    // When we go from consuming to online state, time and final offset matter.
    // Case 1. We have reached final offset.
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      _timeNow += 1;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      segmentDataManager.setConsumeEndTime(_timeNow + 10);
      final long finalOffset = _startOffsetValue + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.destroy();
    }
    // Case 2. We have reached time limit.
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      final long endTime = _timeNow + 10;
      segmentDataManager.setConsumeEndTime(endTime);
      final long finalOffset = _startOffsetValue + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      _timeNow = endTime - 1;
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      _timeNow = endTime;
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.destroy();
    }
  }

  // Replace the realtime segment with a mock that returns numDocs for raw doc count.
  private void replaceRealtimeSegment(FakeLLRealtimeSegmentDataManager segmentDataManager, int numDocs)
      throws Exception {
    MutableSegmentImpl mockSegmentImpl = mock(MutableSegmentImpl.class);
    when(mockSegmentImpl.getNumDocsIndexed()).thenReturn(numDocs);
    Field segmentImpl = LLRealtimeSegmentDataManager.class.getDeclaredField("_realtimeSegment");
    segmentImpl.setAccessible(true);
    segmentImpl.set(segmentDataManager, mockSegmentImpl);
  }

  // If commit fails, make sure that we do not re-build the segment when we try to commit again.
  @Test
  public void testReuseOfBuiltSegment()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    SegmentCompletionProtocol.Response commitSuccess = new SegmentCompletionProtocol.Response(params);
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed respnse first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    segmentDataManager._responses.add(commitSuccess);
    final long leaseTime = 50000L;

    // The first time we invoke build, it should go ahead and build the segment.
    File segmentTarFile = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertNotNull(segmentTarFile);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit());
    Assert.assertTrue(segmentTarFile.exists());

    segmentDataManager._buildSegmentCalled = false;

    // This time around it should not build the segment.
    File segmentTarFile1 = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertEquals(segmentTarFile1, segmentTarFile);
    Assert.assertTrue(segmentTarFile.exists());
    Assert.assertTrue(segmentDataManager.invokeCommit());
    Assert.assertFalse(segmentTarFile.exists());
    segmentDataManager.destroy();
  }

  // If commit fails, and we still have the file, make sure that we remove the file when we go
  // online.
  @Test
  public void testFileRemovedDuringOnlineTransition()
      throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed response first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    final long leaseTime = 50000L;
    final long finalOffset = _startOffsetValue + 600;
    segmentDataManager.setCurrentOffset(finalOffset);

    // We have set up commit to fail, so we should carry over the segment file.
    File segmentTarFile = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertNotNull(segmentTarFile);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit());
    Assert.assertTrue(segmentTarFile.exists());

    // Now let the segment go ONLINE from CONSUMING, and ensure that the file is removed.
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    metadata.setEndOffset(new LongMsgOffset(finalOffset).toString());
    segmentDataManager._stopWaitTimeMs = 0;
    segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.goOnlineFromConsuming(metadata);
    Assert.assertFalse(segmentTarFile.exists());
    segmentDataManager.destroy();
  }

  @Test
  public void testOnlyOneSegmentHoldingTheSemaphoreForParticularPartition()
      throws Exception {
    long timeout = 10_000L;
    FakeLLRealtimeSegmentDataManager firstSegmentDataManager = createFakeSegmentManager();
    Assert.assertTrue(firstSegmentDataManager.getAcquiredConsumerSemaphore().get());
    Semaphore firstSemaphore = firstSegmentDataManager.getPartitionConsumerSemaphore();
    Assert.assertEquals(firstSemaphore.availablePermits(), 0);
    Assert.assertFalse(firstSemaphore.hasQueuedThreads());

    AtomicReference<FakeLLRealtimeSegmentDataManager> secondSegmentDataManager = new AtomicReference<>(null);

    // Construct the second segment manager, which will be blocked on the semaphore.
    Thread constructSecondSegmentManager = new Thread(() -> {
      try {
        secondSegmentDataManager.set(createFakeSegmentManager());
      } catch (Exception e) {
        throw new RuntimeException("Exception when sleeping for " + timeout + "ms", e);
      }
    });
    constructSecondSegmentManager.start();

    // Wait until the second segment manager gets blocked on the semaphore.
    TestUtils.waitForCondition(aVoid -> {
      if (firstSemaphore.hasQueuedThreads()) {
        // Once verified the second segment gets blocked, release the semaphore.
        firstSegmentDataManager.destroy();
        return true;
      } else {
        return false;
      }
    }, timeout, "Failed to wait for the second segment blocked on semaphore");

    // Wait for the second segment manager finished the construction.
    TestUtils.waitForCondition(aVoid -> secondSegmentDataManager.get() != null, timeout,
        "Failed to acquire the semaphore for the second segment manager in " + timeout + "ms");

    Assert.assertTrue(secondSegmentDataManager.get().getAcquiredConsumerSemaphore().get());
    Semaphore secondSemaphore = secondSegmentDataManager.get().getPartitionConsumerSemaphore();
    Assert.assertEquals(firstSemaphore, secondSemaphore);
    Assert.assertEquals(secondSemaphore.availablePermits(), 0);
    Assert.assertFalse(secondSemaphore.hasQueuedThreads());

    // Call destroy method the 2nd time on the first segment manager, the permits in semaphore won't increase.
    firstSegmentDataManager.destroy();
    Assert.assertEquals(firstSegmentDataManager.getPartitionConsumerSemaphore().availablePermits(), 0);

    // The permit finally gets released in the Semaphore.
    secondSegmentDataManager.get().destroy();
    Assert.assertEquals(secondSegmentDataManager.get().getPartitionConsumerSemaphore().availablePermits(), 1);
  }

  public static class FakeLLRealtimeSegmentDataManager extends LLRealtimeSegmentDataManager {

    public Field _state;
    public Field _shouldStop;
    public Field _stopReason;
    private Field _streamMsgOffsetFactory;
    public LinkedList<LongMsgOffset> _consumeOffsets = new LinkedList<>();
    public LinkedList<SegmentCompletionProtocol.Response> _responses = new LinkedList<>();
    public boolean _commitSegmentCalled = false;
    public boolean _buildSegmentCalled = false;
    public boolean _failSegmentBuild = false;
    public boolean _buildAndReplaceCalled = false;
    public int _stopWaitTimeMs = 100;
    private boolean _downloadAndReplaceCalled = false;
    public boolean _throwExceptionFromConsume = false;
    public boolean _postConsumeStoppedCalled = false;
    public Map<Integer, Semaphore> _semaphoreMap;

    private static InstanceDataManagerConfig makeInstanceDataManagerConfig() {
      InstanceDataManagerConfig dataManagerConfig = mock(InstanceDataManagerConfig.class);
      when(dataManagerConfig.getReadMode()).thenReturn(null);
      when(dataManagerConfig.getAvgMultiValueCount()).thenReturn(null);
      when(dataManagerConfig.getSegmentFormatVersion()).thenReturn(null);
      when(dataManagerConfig.isEnableSplitCommit()).thenReturn(false);
      when(dataManagerConfig.isRealtimeOffHeapAllocation()).thenReturn(false);
      when(dataManagerConfig.getConfig()).thenReturn(new PinotConfiguration());
      return dataManagerConfig;
    }

    public FakeLLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
        RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, Schema schema,
        LLCSegmentName llcSegmentName, Map<Integer, Semaphore> semaphoreMap, ServerMetrics serverMetrics)
        throws Exception {
      super(segmentZKMetadata, tableConfig, realtimeTableDataManager, resourceDataDir,
          new IndexLoadingConfig(makeInstanceDataManagerConfig(), tableConfig), schema, llcSegmentName,
          semaphoreMap.get(llcSegmentName.getPartitionId()), serverMetrics, new TableUpsertMetadataManager());
      _state = LLRealtimeSegmentDataManager.class.getDeclaredField("_state");
      _state.setAccessible(true);
      _shouldStop = LLRealtimeSegmentDataManager.class.getDeclaredField("_shouldStop");
      _shouldStop.setAccessible(true);
      _stopReason = LLRealtimeSegmentDataManager.class.getDeclaredField("_stopReason");
      _stopReason.setAccessible(true);
      _semaphoreMap = semaphoreMap;
      _streamMsgOffsetFactory = LLRealtimeSegmentDataManager.class.getDeclaredField("_streamPartitionMsgOffsetFactory");
      _streamMsgOffsetFactory.setAccessible(true);
      _streamMsgOffsetFactory.set(this, new LongMsgOffsetFactory());
    }

    public String getStopReason() {
      try {
        return (String) _stopReason.get(this);
      } catch (Exception e) {
        Assert.fail();
      }
      return null;
    }

    public PartitionConsumer createPartitionConsumer() {
      PartitionConsumer consumer = new PartitionConsumer();
      return consumer;
    }

    public SegmentBuildDescriptor invokeBuildForCommit(long leaseTime) {
      super.buildSegmentForCommit(leaseTime);
      return getSegmentBuildDescriptor();
    }

    public boolean invokeCommit() {
      SegmentCompletionProtocol.Response response = mock(SegmentCompletionProtocol.Response.class);
      when(response.isSplitCommit()).thenReturn(false);
      return super.commitSegment(response.getControllerVipUrl(), false);
    }

    private void terminateLoopIfNecessary() {
      if (_consumeOffsets.isEmpty() && _responses.isEmpty()) {
        try {
          _shouldStop.set(this, true);
        } catch (Exception e) {
          Assert.fail();
        }
      }
    }

    @Override
    protected void start() {
      // Do nothing.
    }

    @Override
    protected boolean consumeLoop()
        throws Exception {
      if (_throwExceptionFromConsume) {
        throw new PermanentConsumerException(new Throwable("Offset out of range"));
      }
      setCurrentOffset(_consumeOffsets.remove().getOffset());
      terminateLoopIfNecessary();
      return true;
    }

    @Override
    protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
      SegmentCompletionProtocol.Response response = _responses.remove();
      terminateLoopIfNecessary();
      return response;
    }

    @Override
    protected SegmentCompletionProtocol.Response commit(String controllerVipUrl, boolean isSplitCommit) {
      SegmentCompletionProtocol.Response response = _responses.remove();
      return response;
    }

    @Override
    protected void postStopConsumedMsg(String reason) {
      _postConsumeStoppedCalled = true;
    }

    @Override
    protected long now() {
      return _timeNow;
    }

    @Override
    protected void hold() {
      _timeNow += 5000L;
    }

    @Override
    protected boolean buildSegmentAndReplace() {
      _buildAndReplaceCalled = true;
      return true;
    }

    @Override
    protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit) {
      _buildSegmentCalled = true;
      if (_failSegmentBuild) {
        return null;
      }
      if (!forCommit) {
        return new SegmentBuildDescriptor(null, null, getCurrentOffset(), 0, 0, -1);
      }
      File segmentTarFile = new File(_segmentDir, "segmentFile");
      try {
        segmentTarFile.createNewFile();
      } catch (IOException e) {
        Assert.fail("Could not create file " + segmentTarFile);
      }
      return new SegmentBuildDescriptor(segmentTarFile, null, getCurrentOffset(), 0, 0, -1);
    }

    @Override
    protected boolean commitSegment(String controllerVipUrl, boolean isSplitCommit) {
      _commitSegmentCalled = true;
      return true;
    }

    @Override
    protected void downloadSegmentAndReplace(LLCRealtimeSegmentZKMetadata metadata) {
      _downloadAndReplaceCalled = true;
    }

    @Override
    public void stop() {
      _timeNow += _stopWaitTimeMs;
    }

    public void setCurrentOffset(long offset) {
      setOffset(offset, "_currentOffset");
    }

    public void setConsumeEndTime(long endTime) {
      setLong(endTime, "_consumeEndTime");
    }

    public void setNumRowsConsumed(int numRows) {
      setInt(numRows, "_numRowsConsumed");
    }

    public void setNumRowsIndexed(int numRows) {
      setInt(numRows, "_numRowsIndexed");
    }

    public void setFinalOffset(long offset) {
      setOffset(offset, "_finalOffset");
    }

    public boolean invokeEndCriteriaReached() {
      Method endCriteriaReached = null;
      try {
        endCriteriaReached = LLRealtimeSegmentDataManager.class.getDeclaredMethod("endCriteriaReached");
        endCriteriaReached.setAccessible(true);
        Boolean result = (Boolean) endCriteriaReached.invoke(this);
        return result;
      } catch (NoSuchMethodException e) {
        Assert.fail();
      } catch (InvocationTargetException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
      throw new RuntimeException("Cannot get here");
    }

    public void setSegmentMaxRowCount(int numRows) {
      setInt(numRows, "_segmentMaxRowCount");
    }

    private void setLong(long value, String fieldName) {
      try {
        Field field = LLRealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(this, value);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    private void setOffset(long value, String fieldName) {
      try {
        Field field = LLRealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        StreamPartitionMsgOffset offset = (StreamPartitionMsgOffset) field.get(this);
//        if (offset == null) {
        field.set(this, new LongMsgOffset(value));
//        } else {
//          offset.setOffset(value);
//        }
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    private void setInt(int value, String fieldName) {
      try {
        Field field = LLRealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(this, value);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }
  }
}
