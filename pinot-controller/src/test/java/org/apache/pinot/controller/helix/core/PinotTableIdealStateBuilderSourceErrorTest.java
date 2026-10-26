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
package org.apache.pinot.controller.helix.core;

import java.net.ConnectException;
import java.util.List;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorReporter;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorRule;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorSubReason;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;


/// The controller records a stream source failure it gives up on while fetching a table's stream metadata.
public class PinotTableIdealStateBuilderSourceErrorTest {
  private static final IngestionSourceErrorRule REFUSED_RULE =
      (node, below, evidence) -> node instanceof RefusedException
          ? IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.AUTHORIZATION, "refused", null) : null;

  /// A source's refusal. A registered rule recognises it, since the built-in rules need SDKs this module doesn't ship.
  private static class RefusedException extends RuntimeException {
    RefusedException() {
      super("refused");
    }
  }

  private ControllerMetrics _controllerMetrics;

  @BeforeClass
  public void setUp() {
    ControllerMetrics.register(new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    _controllerMetrics = ControllerMetrics.get();
    IngestionSourceErrorClassifier.registerRule(REFUSED_RULE);
  }

  @AfterClass
  public void tearDown() {
    IngestionSourceErrorClassifier.unregisterRule(REFUSED_RULE);
  }

  private Long gauge(ControllerGauge gauge, String tableNameWithType) {
    return _controllerMetrics.getGaugeValue(gauge.getGaugeName() + "." + tableNameWithType);
  }

  private static StreamConfig streamConfig(String topic, String tableNameWithType) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicName()).thenReturn(topic);
    when(streamConfig.getTableNameWithType()).thenReturn(tableNameWithType);
    return streamConfig;
  }

  /// Answers each topic's metadata fetch with `answer`.
  private static StreamConsumerFactory factory(Answer<List<PartitionGroupMetadata>> answer)
      throws Exception {
    StreamMetadataProvider provider = mock(StreamMetadataProvider.class);
    when(provider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class), anyList(), anyInt(),
        anyBoolean())).thenAnswer(answer);
    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(provider);
    return factory;
  }

  @Test
  public void fetchThatFailsAfterItsRetriesIsRecorded()
      throws Exception {
    String table = "refusedTable_REALTIME";
    StreamConsumerFactory factory = factory(invocation -> {
      throw new RefusedException();
    });
    try (MockedStatic<StreamConsumerFactoryProvider> provider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      provider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);
      expectThrows(RuntimeException.class, () -> PinotTableIdealStateBuilder.getStreamMetadataList(
          List.of(streamConfig("topic", table)), List.of(), List.of(), false));
    }
    assertNotNull(gauge(ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, table));
    assertNull(gauge(ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, table));
  }

  @Test
  public void topicSkippedInAMultiTopicFetchIsRecorded()
      throws Exception {
    String table = "multiTopicTable_REALTIME";
    StreamConsumerFactory healthy = factory(invocation -> List.of());
    StreamConsumerFactory unreachable = factory(invocation -> {
      throw new PermanentConsumerException(new ConnectException("Connection refused"));
    });
    StreamConfig unreachableTopic = streamConfig("unreachable", table);
    try (MockedStatic<StreamConsumerFactoryProvider> provider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      provider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(healthy);
      provider.when(() -> StreamConsumerFactoryProvider.create(argThat(config -> config == unreachableTopic)))
          .thenReturn(unreachable);
      List<StreamMetadata> streamMetadata = PinotTableIdealStateBuilder.getStreamMetadataList(
          List.of(unreachableTopic, streamConfig("healthy", table)), List.of(), List.of(), false);
      assertEquals(streamMetadata.size(), 1);
    }
    assertNotNull(gauge(ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, table));
  }

  @Test
  public void topicSkippedBeforeAnotherTopicFailsIsStillRecorded()
      throws Exception {
    String table = "skippedThenRefusedTable_REALTIME";
    StreamConsumerFactory unreachable = factory(invocation -> {
      throw new PermanentConsumerException(new ConnectException("Connection refused"));
    });
    StreamConsumerFactory refused = factory(invocation -> {
      throw new RefusedException();
    });
    StreamConfig unreachableTopic = streamConfig("unreachable", table);
    try (MockedStatic<StreamConsumerFactoryProvider> provider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      provider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(refused);
      provider.when(() -> StreamConsumerFactoryProvider.create(argThat(config -> config == unreachableTopic)))
          .thenReturn(unreachable);
      expectThrows(RuntimeException.class, () -> PinotTableIdealStateBuilder.getStreamMetadataList(
          List.of(unreachableTopic, streamConfig("refused", table)), List.of(), List.of(), false));
    }
    assertNotNull(gauge(ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, table));
    assertNotNull(gauge(ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, table));
  }

  @Test
  public void failureThePluginSwallowsAndReportsIsRecorded()
      throws Exception {
    String table = "reportedTable_REALTIME";
    // A plugin that answers a denied metadata call with no partitions, and reports the denial
    StreamConsumerFactory factory = factory(invocation -> {
      IngestionSourceErrorReporter.report(new RefusedException());
      return List.of();
    });
    try (MockedStatic<StreamConsumerFactoryProvider> provider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      provider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);
      PinotTableIdealStateBuilder.getStreamMetadataList(List.of(streamConfig("topic", table)), List.of(), List.of(),
          false);
    }
    assertNotNull(gauge(ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, table));
  }
}
