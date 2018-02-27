/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.manager.realtime;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaLowLevelStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.Errors;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


// TODO Write more tests for other parts of the class
public class LLRealtimeSegmentDataManagerTest {
  private static final String _segmentDir = "/tmp/" + LLRealtimeSegmentDataManagerTest.class.getSimpleName();
  private static final File _segmentDirFile = new File(_segmentDir);
  private static final String _tableName = "Coffee";
  private static final int _partitionId = 13;
  private static final int _sequenceId = 945;
  private static final long _segTimeMs = 98347869999L;
  private static final LLCSegmentName _segmentName = new LLCSegmentName(_tableName, _partitionId, _sequenceId, _segTimeMs);
  private static final String _segmentNameStr = _segmentName.getSegmentName();
  private static final long _startOffset = 19885L;
  private static final String _topicName = "someTopic";
  private static final int maxRowsInSegment = 250000;
  private static final long maxTimeForSegmentCloseMs = 64368000L;

  private static long _timeNow = System.currentTimeMillis();

  private final String _tableConfigJson = "{\n" + "  \"metadata\": {}, \n" + "  \"segmentsConfig\": {\n"
      + "    \"replicasPerPartition\": \"3\", \n" + "    \"replication\": \"3\", \n"
      + "    \"replicationNumber\": 3, \n" + "    \"retentionTimeUnit\": \"DAYS\", \n"
      + "    \"retentionTimeValue\": \"3\", \n" + "    \"schemaName\": \"UnknownSchema\", \n"
      + "    \"segmentAssignmentStrategy\": \"BalanceNumSegmentAssignmentStrategy\", \n"
      + "    \"segmentPushFrequency\": \"daily\", \n" + "    \"segmentPushType\": \"APPEND\", \n"
      + "    \"timeColumnName\": \"minutesSinceEpoch\", \n" + "    \"timeType\": \"MINUTES\"\n" + "  }, \n"
      + "  \"tableIndexConfig\": {\n" + "    \"invertedIndexColumns\": ["
      + "    ], \n" + "    \"lazyLoad\": \"false\", \n" + "    \"loadMode\": \"HEAP\", \n"
      + "    \"segmentFormatVersion\": null, \n" + "    \"sortedColumn\": [], \n"
      + "    \"streamConfigs\": {\n" + "      \"realtime.segment.flush.threshold.size\": \"" + String.valueOf(maxRowsInSegment) + "\", \n"
      + "      \"" + CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME + "\": \"" + maxTimeForSegmentCloseMs + "\", \n"
      + "      \"stream.kafka.broker.list\": \"broker:7777\", \n"
      + "      \"stream.kafka.consumer.prop.auto.offset.reset\": \"smallest\", \n"
      + "      \"stream.kafka.consumer.type\": \"simple\", \n"
      + "      \"stream.kafka.decoder.class.name\": \"com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder\", \n"
      + "      \"stream.kafka.decoder.prop.schema.registry.rest.url\": \"http://schema-registry-host.corp.ceo:1766/schemas\", \n"
      + "      \"stream.kafka.decoder.prop.schema.registry.schema.name\": \"UnknownSchema\", \n"
      + "      \"stream.kafka.hlc.zk.connect.string\": \"zoo:2181/kafka-queuing\", \n"
      + "      \"stream.kafka.topic.name\": \"" + _topicName + "\", \n"
      + "      \"stream.kafka.zk.broker.url\": \"kafka-broker:2181/kafka-queuing\", \n"
      + "      \"streamType\": \"kafka\"\n" + "    }\n" + "  }, \n" + "  \"tableName\": \"Coffee_REALTIME\", \n"
      + "  \"tableType\": \"realtime\", \n" + "  \"tenants\": {\n" + "    \"broker\": \"shared\", \n"
      + "    \"server\": \"server-1\"\n" + "  }\n" + "}";

  private String makeSchema() {
    return "{"
        + "  \"schemaName\":\"SchemaTest\","
        + "  \"metricFieldSpecs\":["
        + "    {\"name\":\"m\",\"dataType\":\"" + "LONG" + "\"}"
        + "  ],"
        + "  \"dimensionFieldSpecs\":["
        + "    {\"name\":\"d\",\"dataType\":\"" + "STRING" + "\",\"singleValueField\":" + "true" + "}"
        + "  ],"
        + "  \"timeFieldSpec\":{"
        + "    \"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"time\"},"
        + "    \"defaultNullValue\":12345"
        + "  }"
        + "}";
  }

  private TableConfig createTableConfig()
      throws Exception {
    return TableConfig.fromJSONConfig(new JSONObject(_tableConfigJson));
  }

  private RealtimeTableDataManager createTableDataManager() {
    final String instanceId = "server-1";
    SegmentBuildTimeLeaseExtender.create(instanceId);
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.getServerInstance()).thenReturn(instanceId);
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(any(String.class))).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(any(String.class))).thenReturn(32);
    when(tableDataManager.getStatsHistory()).thenReturn(statsHistory);
    when(tableDataManager.getServerMetrics()).thenReturn(new ServerMetrics(new MetricsRegistry()));
    return tableDataManager;
  }

  private LLCRealtimeSegmentZKMetadata createZkMetadata() {

    LLCRealtimeSegmentZKMetadata segmentZKMetadata = new LLCRealtimeSegmentZKMetadata();
    segmentZKMetadata.setTableName(_tableName);
    segmentZKMetadata.setSegmentName(_segmentNameStr);
    segmentZKMetadata.setStartOffset(_startOffset);
    return segmentZKMetadata;
  }

  private FakeLLRealtimeSegmentDataManager createFakeSegmentManager() throws Exception {
    LLCRealtimeSegmentZKMetadata segmentZKMetadata = createZkMetadata();
    TableConfig tableConfig = createTableConfig();
    InstanceZKMetadata instanceZKMetadata = new InstanceZKMetadata();
    RealtimeTableDataManager tableDataManager = createTableDataManager();
    String resourceDir = _segmentDir;
    Schema schema = Schema.fromString(makeSchema());
    ServerMetrics serverMetrics = new ServerMetrics(new MetricsRegistry());
    FakeLLRealtimeSegmentDataManager segmentDataManager = new FakeLLRealtimeSegmentDataManager(segmentZKMetadata,
        tableConfig, instanceZKMetadata, tableDataManager, resourceDir, schema, serverMetrics);
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
  public void testTimeString() throws Exception {
    JSONObject tableConfigJson = new JSONObject(_tableConfigJson);
    JSONObject tableIndexConfig = (JSONObject)tableConfigJson.get("tableIndexConfig");
    JSONObject streamConfigs = (JSONObject)tableIndexConfig.get("streamConfigs");
    {
      streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME, "3h");
      TableConfig tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
      InstanceZKMetadata instanceZKMetadata = new InstanceZKMetadata();
      Schema schema = Schema.fromString(makeSchema());
      KafkaLowLevelStreamProviderConfig config = new KafkaLowLevelStreamProviderConfig();
      config.init(tableConfig, instanceZKMetadata, schema);
      Assert.assertEquals(3 * 3600 * 1000L, config.getTimeThresholdToFlushSegment());
    }

    {
      streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME, "3h30m");
      TableConfig tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
      InstanceZKMetadata instanceZKMetadata = new InstanceZKMetadata();
      Schema schema = Schema.fromString(makeSchema());
      KafkaLowLevelStreamProviderConfig config = new KafkaLowLevelStreamProviderConfig();
      config.init(tableConfig, instanceZKMetadata, schema);
      Assert.assertEquals((3 * 3600  + 30 * 60) * 1000L, config.getTimeThresholdToFlushSegment());
    }

    {
      final long segTime = 898789748357L;
      streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME, String.valueOf(segTime));
      TableConfig tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
      InstanceZKMetadata instanceZKMetadata = new InstanceZKMetadata();
      Schema schema = Schema.fromString(makeSchema());
      KafkaLowLevelStreamProviderConfig config = new KafkaLowLevelStreamProviderConfig();
      config.init(tableConfig, instanceZKMetadata, schema);
      Assert.assertEquals(segTime, config.getTimeThresholdToFlushSegment());
    }
  }

  // Test that we are in HOLDING state as long as the controller responds HOLD to our segmentConsumed() message.
  // we should not consume when holding.
  @Test
  public void testHolding() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long endOffset = _startOffset + 500;
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.HOLD).withOffset(endOffset));
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
  }

  // Test that we go to commit when the controller responds commit after 2 holds.
  @Test
  public void testCommitAfterHold() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long endOffset = _startOffset + 500;
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response holdResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(endOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(endOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
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
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.COMMITTED);
  }

  @Test
  public void testSegmentBuildException() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long endOffset = _startOffset + 500;
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(endOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    segmentDataManager._responses.add(commitResponse);
    segmentDataManager._failSegmentBuild = true;

    consumer.run();
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.ERROR);
  }


  // Test hold, catchup. hold, commit
  @Test
  public void testCommitAfterCatchup() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long firstOffset = _startOffset + 500;
    final long catchupOffset = firstOffset + 10;
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.HOLD).
            withOffset(firstOffset));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP).withOffset(
            catchupOffset));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(catchupOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(catchupOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
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
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.COMMITTED);
  }

  @Test
  public void testDiscarded() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long endOffset = _startOffset + 500;
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response discardResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(endOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.DISCARD));
    segmentDataManager._responses.add(discardResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), LLRealtimeSegmentDataManager.State.DISCARDED);
  }

  @Test
  public void testRetained() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long endOffset = _startOffset + 500;
    segmentDataManager._consumeOffsets.add(endOffset);
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withOffset(endOffset).withStatus(SegmentCompletionProtocol.ControllerResponseStatus.KEEP);
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
  }

  @Test
  public void testNotLeader() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final long endOffset = _startOffset + 500;
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withOffset(endOffset).withStatus(
            SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER));
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
  }

  @Test
  public void testConsumingException() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    LLRealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();

    segmentDataManager._throwExceptionFromConsume = true;
    segmentDataManager._postConsumeStoppedCalled = false;
    consumer.run();
    Assert.assertTrue(segmentDataManager._postConsumeStoppedCalled);
  }

  // Tests to go online from consuming state

  // If the state is is COMMITTED or RETAINED, nothing to do
  // If discarded or error state, then downloadAndReplace the segment
  @Test
  public void testOnlineTransitionAfterStop() throws Exception {
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    final long finalOffset = _startOffset + 600;
    metadata.setEndOffset(finalOffset);

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.COMMITTED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.RETAINED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.DISCARDED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.ERROR);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If holding, but we have overshot the expected final offset, the download and replace
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager.setCurrentOffset(finalOffset + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If catching up, but we have overshot the expected final offset, the download and replace
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager.setCurrentOffset(finalOffset + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If catching up, but we did not get to the final offset, then download and replace
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(finalOffset - 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
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
    }
  }

  @Test
  public void testEndCriteriaChecking() throws Exception {
    // test reaching max row limit
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsConsumed(maxRowsInSegment - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsConsumed(maxRowsInSegment);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_ROW_LIMIT);
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
    }
    // In catching up state, test reaching final offset
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = _startOffset + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset-1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // In catching up state, test reaching final offset ignoring time
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      _timeNow += maxTimeForSegmentCloseMs;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = _startOffset + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset-1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // When we go from consuming to online state, time and final offset matter.
    // Case 1. We have reached final offset.
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      _timeNow += 1;
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      segmentDataManager.setConsumeEndTime(_timeNow + 10);
      final long finalOffset = _startOffset + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // Case 2. We have reached time limit.
    {
      FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
      segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      final long endTime = _timeNow + 10;
      segmentDataManager.setConsumeEndTime(endTime);
      final long finalOffset = _startOffset + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      _timeNow = endTime-1;
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      _timeNow = endTime;
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
  }

  // Replace the realtime segment with a mock that returns numDocs for raw doc count.
  private void replaceRealtimeSegment(FakeLLRealtimeSegmentDataManager segmentDataManager, int numDocs) throws Exception {
    RealtimeSegmentImpl mockSegmentImpl = mock(RealtimeSegmentImpl.class);
    when(mockSegmentImpl.getNumDocsIndexed()).thenReturn(numDocs);
    Field segmentImpl = LLRealtimeSegmentDataManager.class.getDeclaredField("_realtimeSegment");
    segmentImpl.setAccessible(true);
    segmentImpl.set(segmentDataManager, mockSegmentImpl);
  }

  // If commit fails, make sure that we do not re-build the segment when we try to commit again.
  @Test
  public void testReuseOfBuiltSegment() throws Exception {
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
    String segTarFileName = segmentDataManager.invokeBuildForCommit(leaseTime);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit(segTarFileName));
    Assert.assertTrue(new File(segTarFileName).exists());

    segmentDataManager._buildSegmentCalled = false;

    // This time around it should not build the segment.
    String segTarFileName1 = segmentDataManager.invokeBuildForCommit(leaseTime);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertEquals(segTarFileName1, segTarFileName);
    Assert.assertTrue(new File(segTarFileName).exists());
    Assert.assertTrue(segmentDataManager.invokeCommit(segTarFileName1));
    Assert.assertFalse(new File(segTarFileName).exists());
  }

  // If commit fails, and we still have the file, make sure that we remove the file when we go
  // online.
  @Test
  public void testFileRemovedDuringOnlineTransition() throws Exception {
    FakeLLRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed respnse first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    final long leaseTime = 50000L;
    final long finalOffset = _startOffset + 600;
    segmentDataManager.setCurrentOffset(finalOffset);

    // We have set up commit to fail, so we should carry over the segment file.
    String segTarFileName = segmentDataManager.invokeBuildForCommit(leaseTime);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit(segTarFileName));
    Assert.assertTrue(new File(segTarFileName).exists());

    // Now let the segment go ONLINE from CONSUMING, and ensure that the file is removed.
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    metadata.setEndOffset(finalOffset);
    segmentDataManager._stopWaitTimeMs = 0;
    segmentDataManager._state.set(segmentDataManager, LLRealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.goOnlineFromConsuming(metadata);
    Assert.assertFalse(new File(segTarFileName).exists());
  }

  public static class FakeLLRealtimeSegmentDataManager extends LLRealtimeSegmentDataManager {

    public Field _state;
    public Field _shouldStop;
    public Field _stopReason;
    public LinkedList<Long> _consumeOffsets = new LinkedList<>();
    public LinkedList<SegmentCompletionProtocol.Response> _responses = new LinkedList<>();
    public boolean _commitSegmentCalled = false;
    public boolean _buildSegmentCalled = false;
    public boolean _failSegmentBuild = false;
    public boolean _buildAndReplaceCalled = false;
    public int _stopWaitTimeMs = 100;
    private boolean _downloadAndReplaceCalled = false;
    public boolean _throwExceptionFromConsume = false;
    public boolean _postConsumeStoppedCalled = false;

    private static InstanceDataManagerConfig makeInstanceDataManagerConfig() {
      InstanceDataManagerConfig dataManagerConfig = mock(InstanceDataManagerConfig.class);
      when(dataManagerConfig.getReadMode()).thenReturn(null);
      when(dataManagerConfig.getAvgMultiValueCount()).thenReturn(null);
      when(dataManagerConfig.getSegmentFormatVersion()).thenReturn(null);
      when(dataManagerConfig.isEnableDefaultColumns()).thenReturn(false);
      when(dataManagerConfig.isEnableSplitCommit()).thenReturn(false);
      when(dataManagerConfig.isRealtimeOffHeapAllocation()).thenReturn(false);
      return dataManagerConfig;
    }

    public FakeLLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
        InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager,
        String resourceDataDir, Schema schema, ServerMetrics serverMetrics)
        throws Exception {
      super(segmentZKMetadata, tableConfig, instanceZKMetadata, realtimeTableDataManager, resourceDataDir,
          new IndexLoadingConfig(makeInstanceDataManagerConfig(), tableConfig), schema, serverMetrics);
      _state = LLRealtimeSegmentDataManager.class.getDeclaredField("_state");
      _state.setAccessible(true);
      _shouldStop = LLRealtimeSegmentDataManager.class.getDeclaredField("_shouldStop");
      _shouldStop.setAccessible(true);
      _stopReason = LLRealtimeSegmentDataManager.class.getDeclaredField("_stopReason");
      _stopReason.setAccessible(true);
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

    public String invokeBuildForCommit(long leaseTime) {
      return super.buildSegmentForCommit(leaseTime);
    }

    public boolean invokeCommit(String segTarFileName) {
      SegmentCompletionProtocol.Response response = mock(SegmentCompletionProtocol.Response.class);
      when(response.getIsSplitCommit()).thenReturn(false);
      return super.commitSegment(segTarFileName, response);
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
    protected boolean consumeLoop() throws Exception {
      if (_throwExceptionFromConsume) {
        throw new SimpleConsumerWrapper.PermanentConsumerException(Errors.OFFSET_OUT_OF_RANGE);
      }
      setCurrentOffset(_consumeOffsets.remove());
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
    protected SegmentCompletionProtocol.Response postSegmentCommitMsg(File segTarFile) {
      SegmentCompletionProtocol.Response response = _responses.remove();
      return response;
    }

    @Override
    protected void postStopConsumedMsg(String reason) {
      _postConsumeStoppedCalled = true;
    }

    @Override
    protected KafkaLowLevelStreamProviderConfig createStreamProviderConfig() {
      KafkaLowLevelStreamProviderConfig config = mock(KafkaLowLevelStreamProviderConfig.class);
      Mockito.doNothing().when(config).init(any(TableConfig.class), any(InstanceZKMetadata.class), any(Schema.class));
      when(config.getTopicName()).thenReturn(_topicName);
      when(config.getStreamName()).thenReturn(_topicName);
      when(config.getSizeThresholdToFlushSegment()).thenReturn(maxRowsInSegment);
      when(config.getTimeThresholdToFlushSegment()).thenReturn(maxTimeForSegmentCloseMs);
      try {
        when(config.getDecoder()).thenReturn(null);
      } catch (Exception e) {
        Assert.fail("Exception setting up streapProviderConfig");
      }
      return config;
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
    protected String buildSegmentInternal(boolean forCommit) {
      _buildSegmentCalled = true;
      if (_failSegmentBuild) {
        return null;
      }
      if (!forCommit) {
        return _segmentDir;
      }
      final String segTarFileName =  _segmentDir + "/" + "segmentFile";
      File segmentTgzFile = new File(segTarFileName);
      try {
        segmentTgzFile.createNewFile();
      } catch (IOException e) {
        Assert.fail("Could not create file " + segmentTgzFile);
      }
      return segTarFileName;
    }

    @Override
    protected boolean commitSegment(final String segTarFile, SegmentCompletionProtocol.Response response) {
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
      setLong(offset, "_currentOffset");
    }

    public void setConsumeEndTime(long endTime) {
      setLong(endTime, "_consumeEndTime");
    }

    public void setNumRowsConsumed(int numRows) {
      setInt(numRows, "_numRowsConsumed");
    }

    public void setFinalOffset(long offset) {
      setLong(offset, "_finalOffset");
    }

    public boolean invokeEndCriteriaReached() {
      Method endCriteriaReached = null;
      try {
        endCriteriaReached = LLRealtimeSegmentDataManager.class.getDeclaredMethod("endCriteriaReached");
        endCriteriaReached.setAccessible(true);
        Boolean result = (Boolean)endCriteriaReached.invoke(this);
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
