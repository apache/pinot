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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka40.KafkaConsumerFactory;
import org.apache.pinot.plugin.stream.kafka40.KafkaPartitionLevelConsumer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Integration test for the low-level Kafka 4.x consumer plugin (`pinot-kafka-4.0`).
///
/// Scoped deliberately to what is specific to the consumer plugin: that a real Kafka 4.x consumer
/// ingests every row end to end, and that exceptions thrown while creating a consumer and while
/// fetching messages drive the affected segment to OFFLINE and are then repaired by the
/// `RealtimeSegmentValidationManager`.
///
/// Query-engine correctness (generated queries, query-file queries, reload, dictionary and index
/// changes) is intentionally **not** re-tested here — [LLCRealtimeClusterIntegrationTest] already
/// covers it against the Kafka 3.x plugin, and those tests exercise the query engine rather than
/// the stream consumer, so running them a second time costs several minutes without adding
/// coverage.
public class LLCRealtimeKafka4ClusterIntegrationTest extends BaseClusterIntegrationTest {

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    // Make sure the realtime segment validation manager does not run by itself, only when we invoke it.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, "2h");
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        3600);
  }

  /// Publishes null-payload records ahead of the Avro data so the Kafka 4.x consumer is exercised against
  /// tombstones. This is consumer behavior, so it stays in scope for this test; the Kafka 3.x consumer gets
  /// the same coverage from [LLCRealtimeClusterIntegrationTest]. Tombstones produce no rows, so the expected
  /// doc count is unaffected.
  @Override
  protected boolean injectTombstones() {
    return true;
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    // Consuming segments allocate off-heap here, matching how this test ran while it extended
    // LLCRealtimeClusterIntegrationTest.
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
  }

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = super.getStreamConfigMap();
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(
        streamConfigMap.get(StreamConfigProperties.STREAM_TYPE),
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), ExceptingKafka4ConsumerFactory.class.getName());
    ExceptingKafka4ConsumerFactory.init(getHelixClusterName(), _helixAdmin, getTableName());
    return streamConfigMap;
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(getStreamConfigMap())));
    return ingestionConfig;
  }

  @Nullable
  @Override
  protected Map<String, String> getStreamConfigs() {
    return null;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();

    List<File> avroFiles = unpackAvroData(_tempDir);
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);
    waitForAllRealtimePartitionsConsuming(TableNameBuilder.REALTIME.tableNameWithType(getTableName()), 120_000L);

    pushAvroIntoKafka(avroFiles);

    // The consumer factory above deliberately fails one partition during consumer creation and again during
    // fetch, which drives those segments to OFFLINE. The validation manager is what repairs them.
    runValidationJob(600_000L);

    // Barrier for every test in this class: testSegmentFlushSize inspects committed segments, so it must not
    // observe a table that is still catching up.
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    waitForEVToDisappear(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  /// Every row produced to Kafka is queryable, i.e. consumption recovered from the injected consumer-creation
  /// and fetch failures without losing or duplicating data.
  @Test
  public void testAllDocsConsumedAfterConsumerFailures()
      throws Exception {
    waitForAllDocsLoaded(600_000L);
  }

  /// The per-partition flush threshold recorded on committed segments matches the configured table-level size.
  @Test
  public void testSegmentFlushSize() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, realtimeTableName);
    assertTrue(segmentsZKMetadata.size() > 0, "No segment ZK metadata found for table: " + realtimeTableName);
    for (SegmentZKMetadata segMetadata : segmentsZKMetadata) {
      if (segMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.UPLOADED) {
        assertEquals(segMetadata.getSizeThresholdToFlushSegment(),
            getRealtimeSegmentFlushSize() / getNumKafkaPartitions());
      }
    }
  }

  /// Waits for each segment the consumer factory failed on to land in OFFLINE, then triggers the
  /// `RealtimeSegmentValidationManager` so it can bring the segment back.
  private void runValidationJob(long timeoutMs)
      throws Exception {
    int partition = ExceptingKafka4ConsumerFactory.PARTITION_FOR_EXCEPTIONS;
    if (partition < 0) {
      return;
    }
    int[] seqNumbers = {ExceptingKafka4ConsumerFactory.SEQ_NUM_FOR_CREATE_EXCEPTION,
        ExceptingKafka4ConsumerFactory.SEQ_NUM_FOR_CONSUME_EXCEPTION};
    Arrays.sort(seqNumbers);
    for (int seqNum : seqNumbers) {
      if (seqNum < 0) {
        continue;
      }
      TestUtils.waitForCondition(() -> isOffline(partition, seqNum), 5000L, timeoutMs,
          "Failed to find offline segment in partition " + partition + " seqNum ", Duration.ofMillis(timeoutMs / 10));
      getOrCreateAdminClient().getClusterClient().runPeriodicTask("RealtimeSegmentValidationManager");
    }
  }

  private boolean isOffline(int partition, int seqNum) {
    ExternalView ev = _helixAdmin.getResourceExternalView(getHelixClusterName(),
        TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    if (ev == null) {
      return false;
    }
    for (String segmentNameStr : ev.getPartitionSet()) {
      if (LLCSegmentName.isLLCSegment(segmentNameStr)) {
        LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
        if (segmentName.getSequenceNumber() == seqNum && segmentName.getPartitionGroupId() == partition
            && ev.getStateMap(segmentNameStr).values()
            .contains(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Kafka 4.x consumer factory that throws while creating a consumer for one segment and while fetching for
  /// another, so the test can verify the recovery path.
  public static class ExceptingKafka4ConsumerFactory extends KafkaConsumerFactory {

    public static final int PARTITION_FOR_EXCEPTIONS = 1; // Setting this to -1 disables all exceptions thrown.
    public static final int SEQ_NUM_FOR_CREATE_EXCEPTION = 1;
    public static final int SEQ_NUM_FOR_CONSUME_EXCEPTION = 3;

    private static HelixAdmin _helixAdmin;
    private static String _helixClusterName;
    private static String _tableName;

    public ExceptingKafka4ConsumerFactory() {
      super();
    }

    public static void init(String helixClusterName, HelixAdmin helixAdmin, String tableName) {
      _helixAdmin = helixAdmin;
      _helixClusterName = helixClusterName;
      _tableName = tableName;
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      /*
       * The segment data manager is creating a consumer to consume rows into a segment.
       * Check the partition and sequence number of the segment and decide whether it
       * qualifies for:
       * - Throwing exception during create OR
       * - Throwing exception during consumption.
       * Make sure that this still works if retries are added in RealtimeSegmentDataManager
       */
      int partition = partitionGroupConsumptionStatus.getPartitionGroupId();
      boolean exceptionDuringConsume = false;
      int seqNum = getSegmentSeqNum(partition);
      if (partition == PARTITION_FOR_EXCEPTIONS) {
        if (seqNum == SEQ_NUM_FOR_CREATE_EXCEPTION) {
          throw new RuntimeException("TestException during consumer creation");
        } else if (seqNum == SEQ_NUM_FOR_CONSUME_EXCEPTION) {
          exceptionDuringConsume = true;
        }
      }
      return new ExceptingKafka4Consumer(clientId, _streamConfig, partition, exceptionDuringConsume);
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus, RetryPolicy retryPolicy) {
      return createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);
    }

    private int getSegmentSeqNum(int partition) {
      IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName,
          TableNameBuilder.REALTIME.tableNameWithType(_tableName));
      AtomicInteger seqNum = new AtomicInteger(-1);
      is.getPartitionSet().forEach(segmentNameStr -> {
        if (LLCSegmentName.isLLCSegment(segmentNameStr)) {
          if (is.getInstanceStateMap(segmentNameStr).values().contains(
              CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
            LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
            if (segmentName.getPartitionGroupId() == partition) {
              seqNum.set(segmentName.getSequenceNumber());
            }
          }
        }
      });
      assertTrue(seqNum.get() >= 0, "No consuming segment found in partition: " + partition);
      return seqNum.get();
    }

    public static class ExceptingKafka4Consumer extends KafkaPartitionLevelConsumer {
      private final boolean _exceptionDuringConsume;

      public ExceptingKafka4Consumer(String clientId, StreamConfig streamConfig, int partition,
          boolean exceptionDuringConsume) {
        super(clientId, streamConfig, partition);
        _exceptionDuringConsume = exceptionDuringConsume;
      }

      @Override
      public KafkaMessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
        if (_exceptionDuringConsume) {
          throw new RuntimeException("TestException during consumption");
        }
        return super.fetchMessages(startOffset, timeoutMs);
      }
    }
  }
}
