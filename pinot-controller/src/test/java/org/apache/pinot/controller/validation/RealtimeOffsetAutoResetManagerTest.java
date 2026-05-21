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
package org.apache.pinot.controller.validation;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.RealtimeOffsetAutoResetHandler;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RealtimeOffsetAutoResetManagerTest {
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String TOPIC_NAME = "testTopic";
  private static final String TEST_HANDLER_CLASS_NAME =
      "org.apache.pinot.controller.validation.RealtimeOffsetAutoResetManagerTest$TestRealtimeOffsetAutoResetHandler";
  private static final String LEGACY_HANDLER_CLASS_NAME =
      "org.apache.pinot.controller.validation.RealtimeOffsetAutoResetManagerTest$LegacyRealtimeOffsetAutoResetHandler";

  @Mock
  private PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @Mock
  private LeadControllerManager _leadControllerManager;

  @Mock
  private ControllerMetrics _controllerMetrics;

  @Mock
  private RealtimeOffsetAutoResetHandler _mockHandler;

  private AutoCloseable _mocks;
  private RealtimeOffsetAutoResetManager _realtimeOffsetAutoResetManager;
  private ControllerConf _controllerConf;
  Properties _properties;

  @BeforeMethod
  public void setup() {
    _mocks = MockitoAnnotations.openMocks(this);
    _controllerConf = new ControllerConf();
    _controllerConf.setRealtimeOffsetAutoResetBackfillFrequencyInSeconds(60);
    _properties = new Properties();
    _properties.setProperty(Constants.RESET_OFFSET_TOPIC_NAME, TOPIC_NAME);
    _properties.setProperty(Constants.RESET_OFFSET_TOPIC_PARTITION, "0");
    _properties.setProperty(Constants.RESET_OFFSET_FROM, "100");
    _properties.setProperty(Constants.RESET_OFFSET_TO, "200");

    _realtimeOffsetAutoResetManager = new RealtimeOffsetAutoResetManager(_controllerConf,
        _pinotHelixResourceManager, _leadControllerManager, _llcRealtimeSegmentManager, _controllerMetrics);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testPreprocessWithBackfillJobProperties() {

    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(_properties);

    Assert.assertNotNull(context);
    // The static fields should be set
    Assert.assertTrue(context.isShouldTriggerBackfillJobs());
    Assert.assertEquals(
        context.getBackfillJobProperties().get(Constants.RESET_OFFSET_TOPIC_NAME),
        TOPIC_NAME);
    Assert.assertEquals(
        context.getBackfillJobProperties().get(Constants.RESET_OFFSET_TOPIC_PARTITION), "0");
    Assert.assertEquals(context.getBackfillJobProperties().get(Constants.RESET_OFFSET_FROM),
        "100");
    Assert.assertEquals(context.getBackfillJobProperties().get(Constants.RESET_OFFSET_TO),
        "200");
  }

  @Test
  public void testPreprocessWithoutBackfillJobProperties() {
    Properties otherProperties = new Properties();
    otherProperties.setProperty("otherProperty", "value");

    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(otherProperties);

    Assert.assertNotNull(context);
    // The static fields should not be set
    Assert.assertFalse(context.isShouldTriggerBackfillJobs());
    Assert.assertTrue(context.getBackfillJobProperties().isEmpty());
  }

  @Test
  public void testProcessTableWithOfflineTable() {
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());

    _realtimeOffsetAutoResetManager.processTable(OFFLINE_TABLE_NAME, context);

    // Should return early for offline tables, no interactions with handlers
    verify(_pinotHelixResourceManager, never()).getTableConfig(anyString());
  }

  @Test
  public void testProcessTableWithRealtimeTableNoTableConfig() {
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(null);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    verify(_pinotHelixResourceManager).getTableConfig(REALTIME_TABLE_NAME);
    // Should return early when table config is null
  }

  @Test
  public void testProcessTableWithRealtimeTableNoHandlerClass() {
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    TableConfig tableConfig = createTableConfigWithoutHandlerClass();
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    verify(_pinotHelixResourceManager).getTableConfig(REALTIME_TABLE_NAME);
    // Should return early when handler class is not specified
  }

  @Test
  public void testProcessTableWithRealtimeTableInvalidHandlerClass() {
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    TableConfig tableConfig = createTableConfigWithInvalidHandlerClass();
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    verify(_pinotHelixResourceManager).getTableConfig(REALTIME_TABLE_NAME);
    RealtimeOffsetAutoResetHandler handler = _realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME);
    Assert.assertNull(handler);
  }

  @Test
  public void testProcessTableWithRealtimeTableValidHandler() {
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(_properties);

    _realtimeOffsetAutoResetManager.processTable(OFFLINE_TABLE_NAME, context);

    TableConfig tableConfig = createTableConfigWithValidHandlerClass();
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);
    RealtimeOffsetAutoResetHandler handler = _realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME);
    Assert.assertNotNull(handler);
    Assert.assertTrue(((TestRealtimeOffsetAutoResetHandler) handler)._triggedBackfillJob);
  }

  @Test
  public void testProcessTableWithRealtimeTableNoBackfillJobTrigger() {
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());

    TableConfig tableConfig = createTableConfigWithValidHandlerClass();
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    RealtimeOffsetAutoResetHandler handler = _realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME);
    Assert.assertNotNull(handler);
    Assert.assertFalse(((TestRealtimeOffsetAutoResetHandler) handler)._triggedBackfillJob);
  }

  @Test
  public void testNonLeaderCleanup() {
    // Populate a handler for the realtime table
    TableConfig tableConfig = createTableConfigWithValidHandlerClass();
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);
    Assert.assertNotNull(_realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME));

    // nonLeaderCleanup should remove the handler and all state for those tables
    List<String> tableNames = Arrays.asList(REALTIME_TABLE_NAME, OFFLINE_TABLE_NAME);
    _realtimeOffsetAutoResetManager.nonLeaderCleanup(tableNames);

    Assert.assertNull(_realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME));
  }

  @Test
  public void testEnsureCompletedBackfillJobsCleanedUp() {
    TableConfig tableConfig = createTableConfigWithValidHandlerClass();
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    // The test handler should be created and cleanupCompletedBackfillJobs should be called
    // We can verify this by checking that the process completes without exception
  }

  @Test
  public void testGetOrConstructHandlerWithExistingHandler() {
    TableConfig tableConfig = createTableConfigWithValidHandlerClass();
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    // First call to create handler
    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    // Second call should reuse existing handler
    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    // Handler should be created only once and reused
    // We can verify this by checking that the process completes without exception
  }

  // ---- Circuit Breaker Tests ----

  @Test
  public void testMaxConcurrentBackfillsSkipsNewTrigger() {
    // Set max concurrent to 1
    _controllerConf.setProperty(
        ControllerConf.ControllerPeriodicTasksConf.MAX_CONCURRENT_BACKFILLS_PER_CONTROLLER, "1");

    String secondTableName = "anotherTable_REALTIME";
    TableConfig secondTableConfig = createTableConfigWithValidHandlerClass(secondTableName, "secondTopic");
    when(_pinotHelixResourceManager.getTableConfig(secondTableName)).thenReturn(secondTableConfig);

    // Trigger a backfill on the second table so _backfillTopicsByTable gets a "secondTopic" entry
    // The TestRealtimeOffsetAutoResetHandler.cleanupCompletedBackfillJobs returns empty list,
    // so ensureCompletedBackfillJobsCleanedUp will not clean it up.
    // We need the handler to report active backfill topics via ensureBackfillJobsRunning.
    // Simplest: configure the stream config of the second table with isBackfillTopic=true
    // so ensureBackfillJobsRunning picks it up as an active backfill topic.
    Map<String, String> backfillStreamMap = new HashMap<>();
    backfillStreamMap.put("streamType", "kafka");
    backfillStreamMap.put("stream.kafka.topic.name", "secondTopic-backfill");
    backfillStreamMap.put("stream.kafka.consumer.type", "simple");
    backfillStreamMap.put("realtime.segment.isBackfillTopic", "true");
    backfillStreamMap.put("stream.kafka.decoder.class.name", "testDecoder");
    IngestionConfig ingestionConfig = secondTableConfig.getIngestionConfig();
    List<Map<String, String>> streamMaps = new java.util.ArrayList<>(
        ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps());
    streamMaps.add(backfillStreamMap);
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(streamMaps));
    ingestionConfig.getStreamIngestionConfig().setRealtimeOffsetAutoResetHandlerClass(TEST_HANDLER_CLASS_NAME);
    when(_llcRealtimeSegmentManager.isTopicConsumptionPaused(secondTableName, 1)).thenReturn(false);

    // Run processTable on second table with no trigger to populate _backfillTopicsByTable via ensureBackfillJobsRunning
    RealtimeOffsetAutoResetManager.Context noTriggerCtx = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    _realtimeOffsetAutoResetManager.processTable(secondTableName, noTriggerCtx);

    // Now try to trigger backfill on the first table — should be blocked (1 table already backfilling)
    TableConfig firstTableConfig = createTableConfigWithValidHandlerClass();
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(firstTableConfig);
    RealtimeOffsetAutoResetManager.Context firstCtx = _realtimeOffsetAutoResetManager.preprocess(_properties);
    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, firstCtx);

    RealtimeOffsetAutoResetHandler handler = _realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME);
    Assert.assertNotNull(handler);
    Assert.assertFalse(((TestRealtimeOffsetAutoResetHandler) handler)._triggedBackfillJob,
        "backfill should be skipped when max concurrent limit is reached");
  }

  @Test
  public void testInFlightCollisionAtThresholdAutopauses() {
    // threshold=1: on the 2nd trigger for the same partition, auto-pause fires
    _controllerConf.setProperty(
        ControllerConf.ControllerPeriodicTasksConf.MAX_BACKFILL_COLLISIONS_BEFORE_AUTO_PAUSE, "1");

    Map<String, String> mainStreamMap = new HashMap<>();
    mainStreamMap.put("streamType", "kafka");
    mainStreamMap.put("stream.kafka.topic.name", TOPIC_NAME);
    mainStreamMap.put("stream.kafka.consumer.type", "simple");
    mainStreamMap.put("realtime.segment.offsetAutoReset.timeSecThreshold", "1800");
    mainStreamMap.put("stream.kafka.decoder.class.name", "testDecoder");

    List<Map<String, String>> maps = Collections.singletonList(mainStreamMap);
    IngestionConfig ingestionConfig = new IngestionConfig();
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(maps);
    streamIngestionConfig.setRealtimeOffsetAutoResetHandlerClass(TEST_HANDLER_CLASS_NAME);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME).build();
    tableConfig.setIngestionConfig(ingestionConfig);

    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    try {
      doNothing().when(_pinotHelixResourceManager).updateTableConfig(tableConfig);
    } catch (Exception ignored) {
    }

    // First trigger — succeeds, sets _partitionsInFlightCount[partition]=1
    RealtimeOffsetAutoResetManager.Context firstCtx = _realtimeOffsetAutoResetManager.preprocess(_properties);
    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, firstCtx);

    TestRealtimeOffsetAutoResetHandler handler =
        (TestRealtimeOffsetAutoResetHandler) _realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME);
    Assert.assertNotNull(handler);
    Assert.assertTrue(handler._triggedBackfillJob, "first trigger should succeed");

    // Reset flag to detect whether second trigger fires
    handler._triggedBackfillJob = false;

    // Second trigger for the same partition — collision #1 >= threshold=1 → auto-pause, skip trigger
    RealtimeOffsetAutoResetManager.Context secondCtx = _realtimeOffsetAutoResetManager.preprocess(_properties);
    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, secondCtx);

    Assert.assertFalse(handler._triggedBackfillJob,
        "collision at threshold should skip the backfill trigger");
    Assert.assertEquals(mainStreamMap.get(StreamConfigProperties.OFFSET_AUTO_RESET_PAUSE), "true",
        "pause flag should be set on the main topic stream config");
  }

  private TableConfig createTableConfigWithValidHandlerClass(String tableName, String topicName) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", topicName);
    streamConfigMap.put("stream.kafka.consumer.type", "simple");
    streamConfigMap.put("realtime.segment.offsetAutoReset.timeSecThreshold", "1800");
    streamConfigMap.put("stream.kafka.decoder.class.name", "testDecoder");
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigMap));
    streamIngestionConfig.setRealtimeOffsetAutoResetHandlerClass(TEST_HANDLER_CLASS_NAME);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  private TableConfig createTableConfigWithoutHandlerClass() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME).build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC_NAME);
    streamConfigMap.put("stream.kafka.consumer.type", "simple");
    streamConfigMap.put("realtime.segment.offsetAutoReset.timeSecThreshold", "1800");
    streamConfigMap.put("stream.kafka.decoder.class.name", "testDecoder");
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigMap));
    streamIngestionConfig.setRealtimeOffsetAutoResetHandlerClass(null);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  private TableConfig createTableConfigWithInvalidHandlerClass() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME).build();
    IngestionConfig ingestionConfig = new IngestionConfig();

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC_NAME);
    streamConfigMap.put("stream.kafka.consumer.type", "simple");
    streamConfigMap.put("realtime.segment.offsetAutoReset.timeSecThreshold", "1800");
    streamConfigMap.put("stream.kafka.decoder.class.name", "testDecoder");
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigMap));
    streamIngestionConfig.setRealtimeOffsetAutoResetHandlerClass("InvalidClass");
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  private TableConfig createTableConfigWithValidHandlerClass() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME).build();
    IngestionConfig ingestionConfig = new IngestionConfig();

    // Add stream configs
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC_NAME);
    streamConfigMap.put("stream.kafka.consumer.type", "simple");
    streamConfigMap.put("realtime.segment.offsetAutoReset.timeSecThreshold", "1800");
    streamConfigMap.put("stream.kafka.decoder.class.name", "testDecoder");

    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigMap));
    streamIngestionConfig.setRealtimeOffsetAutoResetHandlerClass(TEST_HANDLER_CLASS_NAME);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  @Test
  public void testGetOrConstructHandlerWithLegacyConstructorFallback() {
    // Verify that handlers compiled against the old 2-arg constructor contract still load correctly
    TableConfig tableConfig = createTableConfigWithLegacyHandlerClass();
    RealtimeOffsetAutoResetManager.Context context = _realtimeOffsetAutoResetManager.preprocess(new Properties());
    when(_pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);

    _realtimeOffsetAutoResetManager.processTable(REALTIME_TABLE_NAME, context);

    RealtimeOffsetAutoResetHandler handler = _realtimeOffsetAutoResetManager.getTableHandler(REALTIME_TABLE_NAME);
    Assert.assertNotNull(handler, "Legacy handler should be instantiated via 2-arg constructor fallback");
    Assert.assertTrue(handler instanceof LegacyRealtimeOffsetAutoResetHandler);
    LegacyRealtimeOffsetAutoResetHandler legacyHandler = (LegacyRealtimeOffsetAutoResetHandler) handler;
    Assert.assertNotNull(legacyHandler._llcRealtimeSegmentManager, "init() should have been called via constructor");
    Assert.assertNotNull(legacyHandler._pinotHelixResourceManager, "init() should have been called via constructor");
  }

  private TableConfig createTableConfigWithLegacyHandlerClass() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME).build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC_NAME);
    streamConfigMap.put("stream.kafka.consumer.type", "simple");
    streamConfigMap.put("realtime.segment.offsetAutoReset.timeSecThreshold", "1800");
    streamConfigMap.put("stream.kafka.decoder.class.name", "testDecoder");
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigMap));
    streamIngestionConfig.setRealtimeOffsetAutoResetHandlerClass(LEGACY_HANDLER_CLASS_NAME);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  /**
   * Test implementation of RealtimeOffsetAutoResetHandler for testing purposes
   */
  public static class TestRealtimeOffsetAutoResetHandler implements RealtimeOffsetAutoResetHandler {

    public PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
    public PinotHelixResourceManager _pinotHelixResourceManager;
    public boolean _triggedBackfillJob = false;

    public TestRealtimeOffsetAutoResetHandler() {
    }

    @Override
    public void init(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
        PinotHelixResourceManager pinotHelixResourceManager) {
      _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
      _pinotHelixResourceManager = pinotHelixResourceManager;
    }

    @Override
    public boolean triggerBackfillJob(String tableNameWithType, StreamConfig streamConfig, String topicName,
        int partitionId, long fromOffset, long toOffset) {
      _triggedBackfillJob = true;
      return true;
    }

    @Override
    public void ensureBackfillJobsRunning(String tableNameWithType, Collection<String> topicNames) {
      // Test implementation - do nothing
    }

    @Override
    public Collection<String> cleanupCompletedBackfillJobs(String tableNameWithType, Collection<String> topicNames) {
      // Test implementation - return empty collection
      return Collections.emptyList();
    }

    @Override
    public void close() {
    }
  }

  /**
   * Legacy handler that only has a 2-arg constructor (the old SPI contract).
   * Used to verify backward-compatibility fallback in getOrConstructHandler().
   */
  public static class LegacyRealtimeOffsetAutoResetHandler implements RealtimeOffsetAutoResetHandler {

    public PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
    public PinotHelixResourceManager _pinotHelixResourceManager;

    public LegacyRealtimeOffsetAutoResetHandler(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
        PinotHelixResourceManager pinotHelixResourceManager) {
      init(llcRealtimeSegmentManager, pinotHelixResourceManager);
    }

    @Override
    public void init(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
        PinotHelixResourceManager pinotHelixResourceManager) {
      _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
      _pinotHelixResourceManager = pinotHelixResourceManager;
    }

    @Override
    public boolean triggerBackfillJob(String tableNameWithType, StreamConfig streamConfig, String topicName,
        int partitionId, long fromOffset, long toOffset) {
      return true;
    }

    @Override
    public void ensureBackfillJobsRunning(String tableNameWithType, Collection<String> topicNames) {
    }

    @Override
    public Collection<String> cleanupCompletedBackfillJobs(String tableNameWithType, Collection<String> topicNames) {
      return Collections.emptyList();
    }

    @Override
    public void close() {
    }
  }
}
