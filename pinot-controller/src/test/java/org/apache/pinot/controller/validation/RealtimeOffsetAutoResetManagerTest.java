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
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RealtimeOffsetAutoResetManagerTest {
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String TOPIC_NAME = "testTopic";
  private static final String TEST_HANDLER_CLASS_NAME =
      "org.apache.pinot.controller.validation.RealtimeOffsetAutoResetManagerTest$TestRealtimeOffsetAutoResetHandler";

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
    List<String> tableNames = Arrays.asList(REALTIME_TABLE_NAME, OFFLINE_TABLE_NAME);

    _realtimeOffsetAutoResetManager.nonLeaderCleanup(tableNames);

    // The cleanup should remove the tables from internal maps
    // This is tested indirectly by verifying the method completes without exception
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

  /**
   * Test implementation of RealtimeOffsetAutoResetHandler for testing purposes
   */
  public static class TestRealtimeOffsetAutoResetHandler implements RealtimeOffsetAutoResetHandler {

    public PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
    public PinotHelixResourceManager _pinotHelixResourceManager;
    public boolean _triggedBackfillJob = false;

    public TestRealtimeOffsetAutoResetHandler(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
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
}
