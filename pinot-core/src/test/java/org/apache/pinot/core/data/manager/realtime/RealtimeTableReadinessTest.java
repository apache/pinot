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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests consumer readiness against real partial-upsert rows. The fixture controls Helix publication synchronously;
/// readiness and indexing use production code, without starting stream-consumer or Helix threads. Each test confines
/// its fixture and metadata to the invoking thread.
public class RealtimeTableReadinessTest {
  private static final String RAW_TABLE = "readiness";
  private static final String TABLE = RAW_TABLE + "_REALTIME";
  private static final String SERVER = "Server_test_1234";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String TIMESTAMP = "timestamp";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE)
      .addSingleValueDimension(KEY, DataType.STRING).addMetric(VALUE, DataType.LONG)
      .addDateTime(TIMESTAMP, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
      .setPrimaryKeyColumns(List.of(KEY)).build();

  @Test
  public void testRebalanceWaitsForMissingBaseBeforePartialUpsert() throws Exception {
    try (Fixture table = new Fixture(UpsertConfig.Mode.PARTIAL)) {
      // The server already consumed another partition before this rebalance assigned partition 1.
      LLCSegmentName existing = name(0, 0);
      table.assign(existing, ONLINE);
      table.load(existing, row("C", 1, 0));
      LLCSegmentName existingConsumer = name(0, 1);
      table.assign(existingConsumer, CONSUMING);
      assertTrue(table.createReadyToConsumeDataSupplier(existingConsumer).getAsBoolean());

      LLCSegmentName n1 = name(1, 1);
      LLCSegmentName n2 = name(1, 2);
      LLCSegmentName n3 = name(1, 3);
      table.assign(n1, ONLINE);
      table.assign(n2, ONLINE);
      table.assign(n3, CONSUMING);
      table.load(n2, row("A", 1, 2));
      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(n3);
      assertFalse(ready.getAsBoolean(), "Loading the immediate predecessor must not hide the missing older base");
      assertEquals(table.queryRows(), Map.of("A", 1L, "C", 1L));

      table.load(n1, row("B", 1, 1));
      assertTrue(ready.getAsBoolean());
      table.consume(n3, row("B", 1, 3));
      assertEquals(table.queryRows(), Map.of("A", 1L, "B", 2L, "C", 1L),
          "The first replayed increment must see B's historical value");
    }
  }

  @Test
  public void testEachConsumerRechecksAlreadyLoadedPartition() throws Exception {
    try (Fixture table = new Fixture(UpsertConfig.Mode.PARTIAL)) {
      LLCSegmentName n1 = name(0, 1);
      LLCSegmentName n2 = name(0, 2);
      table.assign(n1, ONLINE);
      table._currentState.setState(n1.getSegmentName(), ONLINE);
      table.assign(n2, CONSUMING);
      assertTrue(table.createReadyToConsumeDataSupplier(n2).getAsBoolean());

      LLCSegmentName n3 = name(0, 3);
      table.assign(n2, ONLINE);
      table.assign(n3, CONSUMING);
      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(n3);
      assertFalse(ready.getAsBoolean(), "Earlier readiness must not cover a newly assigned historical segment");
      table._currentState.setState(n2.getSegmentName(), ONLINE);
      assertTrue(ready.getAsBoolean());
    }
  }

  @Test
  public void testReleasedPredecessorDoesNotHideOlderMissingSegment() throws Exception {
    try (Fixture table = new Fixture(UpsertConfig.Mode.PARTIAL)) {
      LLCSegmentName n1 = name(0, 1);
      LLCSegmentName n2 = name(0, 2);
      LLCSegmentName n3 = name(0, 3);
      table.assign(n1, ONLINE);
      table.assign(n2, ONLINE);
      table.assign(n3, CONSUMING);
      RealtimeSegmentDataManager predecessor = mock(RealtimeSegmentDataManager.class);
      table.registerLocal(n2, predecessor);
      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(n3);
      assertFalse(ready.getAsBoolean());
      when(predecessor.hasReleasedConsumerSemaphore()).thenReturn(true);
      assertFalse(ready.getAsBoolean(), "Pauseless release only makes its own segment ready");
      table._currentState.setState(n1.getSegmentName(), ONLINE);
      assertTrue(ready.getAsBoolean(), "A released predecessor need not finish building before rollover");
      when(predecessor.hasReleasedConsumerSemaphore()).thenReturn(false);
      assertFalse(ready.getAsBoolean(), "A locally present but still-consuming predecessor is not ready");
    }
  }

  @Test
  public void testServerReadinessDoesNotCacheSegmentReadiness() throws Exception {
    try (Fixture table = new Fixture(UpsertConfig.Mode.PARTIAL)) {
      LLCSegmentName consumer = name(0, 2);
      table.assign(consumer, CONSUMING);
      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(consumer);
      table._serverReady.set(false);
      assertFalse(ready.getAsBoolean());
      LLCSegmentName base = name(0, 1);
      table.assign(base, ONLINE);
      table._serverReady.set(true);
      assertFalse(ready.getAsBoolean(), "History assigned while the server was waiting must still be checked");
      table._currentState.setState(base.getSegmentName(), ONLINE);
      assertTrue(ready.getAsBoolean());
      table._serverReady.set(false);
      assertFalse(ready.getAsBoolean());
    }
  }

  @DataProvider
  public Object[][] readinessReads() {
    PropertyKey.Builder keys = new PropertyKey.Builder("test-cluster");
    return new Object[][]{{keys.idealStates(TABLE)}, {keys.liveInstance(SERVER)},
        {keys.currentState(SERVER, "test-session", TABLE)}};
  }

  @Test(dataProvider = "readinessReads")
  public void testReadinessRetriesTransientZooKeeperFailure(PropertyKey key) throws Exception {
    try (Fixture table = new Fixture(UpsertConfig.Mode.PARTIAL)) {
      LLCSegmentName base = name(0, 1);
      LLCSegmentName consumer = name(0, 2);
      table.assign(base, ONLINE);
      table._currentState.setState(base.getSegmentName(), ONLINE);
      table.assign(consumer, CONSUMING);
      HelixDataAccessor accessor = table.getHelixManager().getHelixDataAccessor();
      HelixProperty recovered = accessor.getProperty(key);
      when(accessor.<HelixProperty>getProperty(key))
          .thenThrow(new ZkTimeoutException("Transient " + key.getPath() + " failure")).thenReturn(recovered);

      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(consumer);
      assertFalse(ready.getAsBoolean(), "A transient read failure must hold consumption without terminating it");
      assertTrue(ready.getAsBoolean(), "The same consumer must become ready after the read recovers");
    }
  }

  @Test
  public void testDedupWaitsForHistoricalSegments() throws Exception {
    try (Fixture table = new Fixture(UpsertConfig.Mode.NONE, true)) {
      LLCSegmentName base = name(0, 1);
      LLCSegmentName consumer = name(0, 2);
      table.assign(base, ONLINE);
      table.assign(consumer, CONSUMING);
      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(consumer);
      assertTrue(table.isDedupEnabled());
      assertFalse(ready.getAsBoolean());
      table._currentState.setState(base.getSegmentName(), ONLINE);
      assertTrue(ready.getAsBoolean());
    }
  }

  @DataProvider
  public Object[][] independentModes() {
    return new Object[][]{{UpsertConfig.Mode.FULL}, {UpsertConfig.Mode.NONE}};
  }

  @Test(dataProvider = "independentModes")
  public void testOtherModesDoNotWaitForHistoricalSegments(UpsertConfig.Mode mode) throws Exception {
    try (Fixture table = new Fixture(mode)) {
      table.assign(name(0, 1), ONLINE);
      LLCSegmentName consumer = name(0, 2);
      table.assign(consumer, CONSUMING);
      BooleanSupplier ready = table.createReadyToConsumeDataSupplier(consumer);
      assertTrue(ready.getAsBoolean());
      table._serverReady.set(false);
      assertFalse(ready.getAsBoolean());
      table._serverReady.set(true);
      assertTrue(ready.getAsBoolean());
    }
  }

  private static LLCSegmentName name(int partition, int sequence) {
    return new LLCSegmentName(RAW_TABLE, partition, sequence, 0);
  }

  private static GenericRow row(String key, long value, long timestamp) {
    GenericRow row = new GenericRow();
    row.putValue(KEY, key);
    row.putValue(VALUE, value);
    row.putValue(TIMESTAMP, timestamp);
    return row;
  }

  /// Initializes real table and metadata managers; mocks external services and unrelated background workers.
  /// All fixture state and production metadata operations remain confined to the invoking test thread.
  private static class Fixture extends RealtimeTableDataManager implements AutoCloseable {
    private final File _directory;
    private final TableConfig _config;
    private final IdealState _assignment = new IdealState(TABLE);
    private final CurrentState _currentState = new CurrentState(TABLE);
    private final AtomicBoolean _serverReady;
    private final List<IndexSegment> _segments = new ArrayList<>();

    Fixture(UpsertConfig.Mode mode) throws Exception {
      this(mode, false);
    }

    Fixture(UpsertConfig.Mode mode, boolean dedup) throws Exception {
      this(mode, dedup, new AtomicBoolean(true));
    }

    Fixture(UpsertConfig.Mode mode, boolean dedup, AtomicBoolean serverReady) throws Exception {
      super(null, serverReady::get, () -> true,
          ServerIngestionOomProtectionManager.createServerThrottleState(null, ServerMetrics.get()));
      _serverReady = serverReady;
      _directory = Files.createTempDirectory("consumer-readiness").toFile();
      UpsertConfig upsert = new UpsertConfig(mode);
      upsert.setComparisonColumns(List.of(TIMESTAMP));
      if (mode == UpsertConfig.Mode.PARTIAL) {
        upsert.setPartialUpsertStrategies(Map.of(VALUE, UpsertConfig.Strategy.INCREMENT));
      }
      _config = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
          .setTimeColumnName(TIMESTAMP).setUpsertConfig(upsert).setNullHandlingEnabled(true).build();
      if (dedup) {
        _config.setDedupConfig(new DedupConfig());
      }
      _instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
      when(_instanceDataManagerConfig.getUpsertConfig()).thenReturn(new PinotConfiguration());
      when(_instanceDataManagerConfig.getDedupConfig()).thenReturn(new PinotConfiguration());
      _instanceId = SERVER;
      _tableNameWithType = TABLE;
      _tableDataDir = _directory.getAbsolutePath();
      _indexDir = _directory;
      _cachedTableConfigAndSchema = Pair.of(_config, SCHEMA);
      _logger = LoggerFactory.getLogger(RealtimeTableReadinessTest.class);
      _helixManager = mock(HelixManager.class);
      HelixDataAccessor accessor = mock(HelixDataAccessor.class);
      PropertyKey.Builder keys = new PropertyKey.Builder("test-cluster");
      LiveInstance liveInstance = mock(LiveInstance.class);
      when(_helixManager.getInstanceName()).thenReturn(SERVER);
      when(_helixManager.getHelixDataAccessor()).thenReturn(accessor);
      when(accessor.keyBuilder()).thenReturn(keys);
      when(accessor.getProperty(keys.idealStates(TABLE))).thenReturn(_assignment);
      when(accessor.getProperty(keys.liveInstance(SERVER))).thenReturn(liveInstance);
      when(liveInstance.getEphemeralOwner()).thenReturn("test-session");
      when(accessor.getProperty(keys.currentState(SERVER, "test-session", TABLE))).thenReturn(_currentState);
      try (MockedStatic<SegmentBuildTimeLeaseExtender> lease = mockStatic(SegmentBuildTimeLeaseExtender.class);
          MockedConstruction<IngestionDelayTracker> tracker = mockConstruction(IngestionDelayTracker.class)) {
        doInit();
      }
    }

    void assign(LLCSegmentName segment, String state) {
      _assignment.setInstanceStateMap(segment.getSegmentName(), Map.of(SERVER, state));
    }

    void registerLocal(LLCSegmentName name, RealtimeSegmentDataManager segment) {
      _segmentDataManagerMap.put(name.getSegmentName(), segment);
    }

    void load(LLCSegmentName name, GenericRow row) throws Exception {
      File output = new File(_directory, "built");
      FileUtils.forceMkdir(output);
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(_config, SCHEMA);
      config.setOutDir(output.getAbsolutePath());
      config.setSegmentName(name.getSegmentName());
      config.setSegmentTimeValueCheck(false);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(List.of(row)));
      driver.build();
      ImmutableSegmentImpl segment = (ImmutableSegmentImpl) ImmutableSegmentLoader.load(
          new File(output, name.getSegmentName()), ReadMode.mmap);
      _segments.add(segment);
      _tableUpsertMetadataManager.getOrCreatePartitionManager(name.getPartitionGroupId()).addSegment(segment);
      _currentState.setState(name.getSegmentName(), ONLINE);
    }

    void consume(LLCSegmentName name, GenericRow row) throws Exception {
      RealtimeSegmentStatsHistory stats = mock(RealtimeSegmentStatsHistory.class);
      when(stats.getEstimatedCardinality(anyString())).thenReturn(10);
      when(stats.getEstimatedAvgColSize(anyString())).thenReturn(16);
      String segmentName = name.getSegmentName();
      RealtimeSegmentConfig config = new RealtimeSegmentConfig.Builder()
          .setTableNameWithType(TABLE).setSegmentName(segmentName).setStreamName("testStream")
          .setSchema(SCHEMA).setTimeColumnName(TIMESTAMP).setCapacity(10).setAvgNumMultiValues(1)
          .setSegmentZKMetadata(new SegmentZKMetadata(segmentName))
          .setMemoryManager(new DirectMemoryManager(segmentName)).setStatsHistory(stats)
          .setDefaultNullHandlingEnabled(true)
          .setPartitionUpsertMetadataManager(
              _tableUpsertMetadataManager.getOrCreatePartitionManager(name.getPartitionGroupId()))
          .setConsumerDir(getConsumerDir()).build();
      MutableSegmentImpl segment = new MutableSegmentImpl(config, null);
      _segments.add(segment);
      segment.index(row.copy(), null);
    }

    Map<String, Long> queryRows() throws IOException {
      Map<String, Long> rows = new TreeMap<>();
      for (IndexSegment segment : _segments) {
        try (PinotSegmentColumnReader keys = new PinotSegmentColumnReader(segment, KEY);
            PinotSegmentColumnReader values = new PinotSegmentColumnReader(segment, VALUE)) {
          for (int docId : segment.getValidDocIds().getMutableRoaringBitmap().toArray()) {
            String key = (String) keys.getValue(docId);
            assertNull(rows.put(key, ((Number) values.getValue(docId)).longValue()), "Duplicate visible key: " + key);
          }
        }
      }
      return rows;
    }

    @Override
    public void close() throws IOException {
      try {
        if (_tableUpsertMetadataManager != null) {
          _tableUpsertMetadataManager.stop();
          _tableUpsertMetadataManager.close();
        }
        if (getTableDedupMetadataManager() != null) {
          getTableDedupMetadataManager().stop();
          getTableDedupMetadataManager().close();
        }
      } finally {
        _segments.forEach(IndexSegment::destroy);
        FileUtils.deleteDirectory(_directory);
      }
    }
  }
}
