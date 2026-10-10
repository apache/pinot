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
package org.apache.pinot.controller.helix.core.retention;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.apache.pinot.controller.helix.core.SegmentDeletionManager;
import org.apache.pinot.controller.util.BrokerServiceHelper;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.helix.core.retention.RetentionManager.DEFAULT_UNTRACKED_SEGMENTS_DELETION_BATCH_SIZE;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class RetentionManagerTest {
  private static final String HELIX_CLUSTER_NAME = "TestRetentionManager";
  private static final String TEST_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TEST_TABLE_NAME);

  // Variables for real file test
  private Path _tempDir;
  private File _tableDir;

  protected RetentionManager createRetentionManager(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      BrokerServiceHelper brokerServiceHelper) {
    return new RetentionManager(pinotHelixResourceManager, leadControllerManager, config, controllerMetrics,
        brokerServiceHelper);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    // Setup for real file test
    _tempDir = Files.createTempDirectory("pinot-retention-test");
    _tableDir = new File(_tempDir.toFile(), TEST_TABLE_NAME);
    _tableDir.mkdirs();

    final long pastMillisSinceEpoch = 1343001600000L;
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // Clean up the temporary directory after each test
    if (_tempDir != null) {
      FileUtils.deleteDirectory(_tempDir.toFile());
    }
  }

  private void testDifferentTimeUnits(long pastTimeStamp, TimeUnit timeUnit, long dayAfterTomorrowTimeStamp,
      String untrackedSegmentsDeletionBatchSize, int untrackedSegmentsInDeepstoreSize) {
    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();
    // Create metadata for 10 segments really old, that will be removed by the retention manager.
    final int numOlderSegments = 10;
    List<String> removedSegments = new ArrayList<>();
    for (int i = 0; i < numOlderSegments; i++) {
      SegmentZKMetadata segmentZKMetadata = mockSegmentZKMetadata(pastTimeStamp, pastTimeStamp, timeUnit);
      segmentsZKMetadata.add(segmentZKMetadata);
      removedSegments.add(segmentZKMetadata.getSegmentName());
    }
    // Create metadata for 5 segments that will not be removed.
    for (int i = 0; i < 5; i++) {
      SegmentZKMetadata segmentZKMetadata =
          mockSegmentZKMetadata(dayAfterTomorrowTimeStamp, dayAfterTomorrowTimeStamp, timeUnit);
      segmentsZKMetadata.add(segmentZKMetadata);
    }

    // Create actual segment files with specific modification times
    // 1. A file that should be kept (in ZK metadata)
    File segment1File = new File(_tableDir, segmentsZKMetadata.get(0).getSegmentName());
    createFileWithContent(segment1File, "segment1 data");
    setFileModificationTime(segment1File, timeUnit.toMillis(pastTimeStamp));

    // 2. A file that should be kept (in ZK metadata)
    File segment2File = new File(_tableDir, segmentsZKMetadata.get(10).getSegmentName());
    createFileWithContent(segment2File, "segment2 data");
    setFileModificationTime(segment2File, timeUnit.toMillis(pastTimeStamp));

    // 3. A file that should not be deleted (not in ZK metadata but recent)
    File segment3File = new File(_tableDir, "segment3.tar.gz");
    createFileWithContent(segment3File, "segment3 data");
    setFileModificationTime(segment3File, timeUnit.toMillis(dayAfterTomorrowTimeStamp));

    int deletionBatchSize = untrackedSegmentsDeletionBatchSize == null ? DEFAULT_UNTRACKED_SEGMENTS_DELETION_BATCH_SIZE
        : Integer.parseInt(untrackedSegmentsDeletionBatchSize);

    // Create additional untracked segment files to test batch size limit
    if (untrackedSegmentsInDeepstoreSize > 0) {
      // Create more untracked segments
      for (int i = 0; i < untrackedSegmentsInDeepstoreSize; i++) {
        String segmentName = "extraSegment" + i;
        File segmentFile = new File(_tableDir, segmentName);
        createFileWithContent(segmentFile, "extra segment " + i + " data");
        setFileModificationTime(segmentFile, timeUnit.toMillis(pastTimeStamp));
        if (i < deletionBatchSize) {
          // Add segments to the removed list till we reach untrackedSegmentsDeletionBatchSize
          removedSegments.add(segmentName);
        }
      }
    }

    final TableConfig tableConfig = createOfflineTableConfig();
    // Set untrackedSegmentsDeletionBatchSize if not null
    if (untrackedSegmentsDeletionBatchSize != null) {
      tableConfig.getValidationConfig().setUntrackedSegmentsDeletionBatchSize(untrackedSegmentsDeletionBatchSize);
    }

    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    // Use appropriate setup based on test case
    // In case of untrackedSegmentsDeletionBatchSize < untrackedSegmentsInDeepstoreSize, we cannot guarantee which
    // files/ segments will be picked for deletion as there is not ordering/ sorting done before selecting
    // untrackedSegmentsDeletionBatchSize out of untrackedSegmentsInDeepstoreSize to delete.
    // For the case untrackedSegmentsDeletionBatchSize < untrackedSegmentsInDeepstoreSize we just check the size of the
    // segments that will get deleted.
    // if the untrackedSegmentsDeletionBatchSize all the segments will be deleted as the batch size by default is 100
    if (deletionBatchSize >= untrackedSegmentsInDeepstoreSize) {
      // Use original setup for the case when all the segments will be included
      setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);
    } else {
      // Use batch size specific setup
      setupPinotHelixResourceManagerForBatchSize(tableConfig, numOlderSegments,
          deletionBatchSize, segmentsZKMetadata,
          pinotHelixResourceManager, leadControllerManager);
    }

    when(pinotHelixResourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getDataDir()).thenReturn(_tempDir.toString());

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    conf.setUntrackedSegmentDeletionEnabled(true);
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, conf, null, null);
    RetentionManager retentionManager =
        createRetentionManager(pinotHelixResourceManager, leadControllerManager, conf, controllerMetrics,
            brokerServiceHelper);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is called
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager,
        ControllerConf.ControllerPeriodicTasksConf.DEFAULT_AGED_SEGMENTS_DELETION_BATCH_SIZE);

    // Verify deleteSegments is called
    verify(pinotHelixResourceManager, times(1)).deleteSegments(eq(OFFLINE_TABLE_NAME), anyList());
  }

  @Test
  public void testRetentionWithMinutesNoBatchSizeAndSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    final long pastMinutesSinceEpoch = 22383360L;
    testDifferentTimeUnits(pastMinutesSinceEpoch, TimeUnit.MINUTES, minutesSinceEpochTimeStamp, null, 4);
  }

  @Test
  public void testRetentionWithMinutesNoBatchSizeAndMoreSegmentsInDeepStore() {
    // For this test the default batch size will get picked
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    final long pastMinutesSinceEpoch = 22383360L;
    testDifferentTimeUnits(pastMinutesSinceEpoch, TimeUnit.MINUTES, minutesSinceEpochTimeStamp, null, 105);
  }


  @Test
  public void testRetentionWithMinutesWithBatchSizeAndLessSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    final long pastMinutesSinceEpoch = 22383360L;
    testDifferentTimeUnits(pastMinutesSinceEpoch, TimeUnit.MINUTES, minutesSinceEpochTimeStamp, "5", 3);
  }

  @Test
  public void testRetentionWithMinutesWithBatchSizeAndMoreSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    final long pastMinutesSinceEpoch = 22383360L;
    testDifferentTimeUnits(pastMinutesSinceEpoch, TimeUnit.MINUTES, minutesSinceEpochTimeStamp, "5", 10);
  }


  @Test
  public void testRetentionWithSecondsNoBatchSizeAndSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    final long pastSecondsSinceEpoch = 1343001600L;
    testDifferentTimeUnits(pastSecondsSinceEpoch, TimeUnit.SECONDS, secondsSinceEpochTimeStamp, null, 4);
  }

  @Test
  public void testRetentionWithSecondsWithBatchSizeAndLessSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    final long pastSecondsSinceEpoch = 1343001600L;
    testDifferentTimeUnits(pastSecondsSinceEpoch, TimeUnit.SECONDS, secondsSinceEpochTimeStamp, "5", 3);
  }

  @Test
  public void testRetentionWithSecondsWithBatchSizeAndMoreSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    final long pastSecondsSinceEpoch = 1343001600L;
    testDifferentTimeUnits(pastSecondsSinceEpoch, TimeUnit.SECONDS, secondsSinceEpochTimeStamp, "5", 10);
  }

  @Test
  public void testRetentionWithMillisNoBatchSizeAndSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long millisSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    final long pastMillisSinceEpoch = 1343001600000L;
    testDifferentTimeUnits(pastMillisSinceEpoch, TimeUnit.MILLISECONDS, millisSinceEpochTimeStamp, null, 4);
  }

  @Test
  public void testRetentionWithMillisWithBatchSizeAndLessSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long millisSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    final long pastMillisSinceEpoch = 1343001600000L;
    testDifferentTimeUnits(pastMillisSinceEpoch, TimeUnit.MILLISECONDS, millisSinceEpochTimeStamp, "5", 3);
  }

  @Test
  public void testRetentionWithMillisWithBatchSizeAndMoreSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long millisSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    final long pastMillisSinceEpoch = 1343001600000L;
    testDifferentTimeUnits(pastMillisSinceEpoch, TimeUnit.MILLISECONDS, millisSinceEpochTimeStamp, "5", 10);
  }

  @Test
  public void testRetentionWithHoursNoBatchSizeAndSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    final long pastHoursSinceEpoch = 373056L;
    testDifferentTimeUnits(pastHoursSinceEpoch, TimeUnit.HOURS, hoursSinceEpochTimeStamp, null, 4);
  }

  @Test
  public void testRetentionWithHoursWithBatchSizeAndLessSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    final long pastHoursSinceEpoch = 373056L;
    testDifferentTimeUnits(pastHoursSinceEpoch, TimeUnit.HOURS, hoursSinceEpochTimeStamp, "5", 3);
  }

  @Test
  public void testRetentionWithHoursWithBatchSizeAndMoreSegmentsInDeepStore() {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    final long pastHoursSinceEpoch = 373056L;
    testDifferentTimeUnits(pastHoursSinceEpoch, TimeUnit.HOURS, hoursSinceEpochTimeStamp, "5", 10);
  }


  @Test
  public void testRetentionWithDaysNoBatchSizeAndSegmentsInDeepStore() {
    final long daysSinceEpochTimeStamp = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long pastDaysSinceEpoch = 15544L;
    testDifferentTimeUnits(pastDaysSinceEpoch, TimeUnit.DAYS, daysSinceEpochTimeStamp, null, 4);
  }

  @Test
  public void testRetentionWithDaysWithBatchSizeAndLessSegmentsInDeepStore() {
    final long daysSinceEpochTimeStamp = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long pastDaysSinceEpoch = 15544L;
    testDifferentTimeUnits(pastDaysSinceEpoch, TimeUnit.DAYS, daysSinceEpochTimeStamp, "5", 3);
  }

  @Test
  public void testRetentionWithDaysWithBatchSizeAndMoreSegmentsInDeepStore() {
    final long daysSinceEpochTimeStamp = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long pastDaysSinceEpoch = 15544L;
    testDifferentTimeUnits(pastDaysSinceEpoch, TimeUnit.DAYS, daysSinceEpochTimeStamp, "5", 10);
  }

  @Test
  public void testOffByDefaultForUntrackedSegmentsDeletion() {
    long pastTimeStamp = 15544L;
    TimeUnit timeUnit = TimeUnit.DAYS;
    long dayAfterTomorrowTimeStamp = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;

    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();
    // Create metadata for 10 segments really old, that will be removed by the retention manager.
    final int numOlderSegments = 10;
    List<String> removedSegments = new ArrayList<>();
    for (int i = 0; i < numOlderSegments; i++) {
      SegmentZKMetadata segmentZKMetadata = mockSegmentZKMetadata(pastTimeStamp, pastTimeStamp, timeUnit);
      segmentsZKMetadata.add(segmentZKMetadata);
      removedSegments.add(segmentZKMetadata.getSegmentName());
    }
    // Create metadata for 5 segments that will not be removed.
    for (int i = 0; i < 5; i++) {
      SegmentZKMetadata segmentZKMetadata =
          mockSegmentZKMetadata(dayAfterTomorrowTimeStamp, dayAfterTomorrowTimeStamp, timeUnit);
      segmentsZKMetadata.add(segmentZKMetadata);
    }

    // Create actual segment files with specific modification times
    // 1. A file that should be kept (in ZK metadata)
    File segment1File = new File(_tableDir, segmentsZKMetadata.get(0).getSegmentName());
    createFileWithContent(segment1File, "segment1 data");
    setFileModificationTime(segment1File, timeUnit.toMillis(pastTimeStamp));

    // 2. A file that should be kept (in ZK metadata)
    File segment2File = new File(_tableDir, segmentsZKMetadata.get(10).getSegmentName());
    createFileWithContent(segment2File, "segment2 data");
    setFileModificationTime(segment2File, timeUnit.toMillis(pastTimeStamp));

    // 3. A file that should not be deleted as the deletion of untracked segments is off by default
    File segment3File = new File(_tableDir, "segment3.tar.gz");
    createFileWithContent(segment3File, "segment3 data");
    setFileModificationTime(segment3File, timeUnit.toMillis(pastTimeStamp));

    final TableConfig tableConfig = createOfflineTableConfig();

    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);

    when(pinotHelixResourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getDataDir()).thenReturn(_tempDir.toString());

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, conf, null, null);
    RetentionManager retentionManager = createRetentionManager(pinotHelixResourceManager, leadControllerManager, conf,
        controllerMetrics, brokerServiceHelper);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is called
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager,
        ControllerConf.ControllerPeriodicTasksConf.DEFAULT_AGED_SEGMENTS_DELETION_BATCH_SIZE);

    // Verify deleteSegments is called
    verify(pinotHelixResourceManager, times(1)).deleteSegments(eq(OFFLINE_TABLE_NAME), anyList());
  }

  @Test
  public void testManageRetentionForHybridTable() {
    // setup
    String tableName = "myTable";
    String realtimeTableName = "myTable_REALTIME";
    String offlineTableName = "myTable_OFFLINE";
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "APPEND", "DAILY", false));
    TableConfig realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(realtimeTableName)
        .setTimeColumnName("ms").setTimeType("MILLISECONDS").setRetentionTimeValue("7").setRetentionTimeUnit("DAYS")
        .setIngestionConfig(ingestionConfig)
        .build();

    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(offlineTableName)
        .setTimeColumnName("ms").setTimeType("MILLISECONDS").setRetentionTimeValue("90").setRetentionTimeUnit("DAYS")
        .build();
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setTimeColumnName("ms");
    offlineTableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    when(mockPinotHelixResourceManager.getOfflineTableConfig(offlineTableName)).thenReturn(offlineTableConfig);

    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    when(mockPinotHelixResourceManager.getPropertyStore()).thenReturn(mockPropertyStore);

    // create realtime table schema
    Schema schema = new Schema();
    schema.setSchemaName(tableName);
    schema.addField(new DateTimeFieldSpec("ms", FieldSpec.DataType.LONG, "EPOCH|MILLISECONDS|1", "MILLISECONDS|1"));
    String realtimeTableSchemaJson = schema.toSingleLineJsonString();
    ZNRecord tableZNRecord = new ZNRecord(tableName);
    tableZNRecord.setSimpleField("schemaJSON", realtimeTableSchemaJson);
    when(mockPropertyStore.get("/SCHEMAS/" + tableName, null, AccessOption.PERSISTENT)).thenReturn(tableZNRecord);

    InstanceConfig instanceConfig = new InstanceConfig("Broker_localhost_1234");
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("8000");

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerBrokerProtocol("http");

    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    when(mockResourceManager.getBrokerInstancesConfigsFor(offlineTableConfig.getTableName()))
        .thenReturn(List.of(instanceConfig));

    CompletionServiceHelper mockServiceHelper = mock(CompletionServiceHelper.class);

    // Mock responses
    Map<String, String> responseMap = new HashMap<>();
    responseMap.put("http://localhost:8000/debug/timeBoundary/" + offlineTableName,
        "{ \"timeColumn\": \"ts\", \"timeValue\": 7776000000}");
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        new CompletionServiceHelper.CompletionServiceResponse();
    serviceResponse._httpResponses = responseMap;
    when(mockServiceHelper.doMultiGetRequest(anyList(), anyString(), anyBoolean(), anyMap(), anyInt(), anyString()))
        .thenReturn(serviceResponse);

    // create segment ZK metadata for realtime table
    SegmentZKMetadata realtimeSeg1 = new SegmentZKMetadata("realtime_seg1");
    realtimeSeg1.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

    SegmentZKMetadata realtimeSeg2 = new SegmentZKMetadata("realtime_seg2");
    realtimeSeg2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    realtimeSeg2.setTimeUnit(TimeUnit.MILLISECONDS);
    realtimeSeg2.setEndTime(86_400_000 * 8);

    List<SegmentZKMetadata> realtimeSegments = Arrays.asList(realtimeSeg1, realtimeSeg2);
    when(mockPinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName)).thenReturn(realtimeSegments);

    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, controllerConf, null, null);
    brokerServiceHelper.setCompletionServiceHelper(mockServiceHelper);

    // test
    RetentionManager retentionManager =
        createRetentionManager(mockPinotHelixResourceManager, null, controllerConf, mock(ControllerMetrics.class),
            brokerServiceHelper);
    retentionManager.manageRetentionForHybridTable(realtimeTableConfig, offlineTableConfig);

    // verify
    verify(mockPinotHelixResourceManager, times(1)).deleteSegments(eq(realtimeTableName), anyList());
  }

  private TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("365").setNumReplicas(2).build();
  }

  private TableConfig createRealtimeTableConfig1(int replicaCount) {
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TEST_TABLE_NAME).setStreamConfigs(streamConfigs)
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5").setNumReplicas(replicaCount).build();
  }

  private void setupPinotHelixResourceManager(TableConfig tableConfig, final List<String> removedSegments,
      PinotHelixResourceManager resourceManager, LeadControllerManager leadControllerManager) {
    String tableNameWithType = tableConfig.getTableName();
    when(resourceManager.getAllTables()).thenReturn(List.of(tableNameWithType));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    SegmentDeletionManager deletionManager = mock(SegmentDeletionManager.class);
    // Ignore the call to SegmentDeletionManager.removeAgedDeletedSegments. we only test that the call is made once per
    // run of the retention manager
    doAnswer(invocationOnMock -> null).when(deletionManager)
        .removeAgedDeletedSegments(leadControllerManager,
            ControllerConf.ControllerPeriodicTasksConf.DEFAULT_AGED_SEGMENTS_DELETION_BATCH_SIZE);
    when(resourceManager.getSegmentDeletionManager()).thenReturn(deletionManager);

    // If and when PinotHelixResourceManager.deleteSegments() is invoked, make sure that the segments deleted
    // are exactly the same as the ones we expect to be deleted.
    doAnswer(invocationOnMock -> {
      Object[] args = invocationOnMock.getArguments();
      String tableNameArg = (String) args[0];
      assertEquals(tableNameArg, tableNameWithType);
      List<String> segmentListArg = (List<String>) args[1];
      assertEquals(segmentListArg.size(), removedSegments.size());
      for (String segmentName : removedSegments) {
        assertTrue(segmentListArg.contains(segmentName));
      }
      return null;
    }).when(resourceManager).deleteSegments(anyString(), anyList());
  }

  private void setupPinotHelixResourceManagerForBatchSize(TableConfig tableConfig, int numOlderSegments,
      int untrackedSegmentsDeletionBatchSize, List<SegmentZKMetadata> segmentsZKMetadata,
      PinotHelixResourceManager resourceManager, LeadControllerManager leadControllerManager) {

    String tableNameWithType = tableConfig.getTableName();
    when(resourceManager.getAllTables()).thenReturn(List.of(tableNameWithType));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    SegmentDeletionManager deletionManager = mock(SegmentDeletionManager.class);
    doAnswer(invocationOnMock -> null).when(deletionManager)
        .removeAgedDeletedSegments(leadControllerManager,
            ControllerConf.ControllerPeriodicTasksConf.DEFAULT_AGED_SEGMENTS_DELETION_BATCH_SIZE);
    when(resourceManager.getSegmentDeletionManager()).thenReturn(deletionManager);

    // Set up verification for deleteSegments with focus on the count and segment inclusion rules
    doAnswer(invocationOnMock -> {
      Object[] args = invocationOnMock.getArguments();
      String tableNameArg = (String) args[0];
      assertEquals(tableNameArg, tableNameWithType);
      List<String> segmentListArg = (List<String>) args[1];

      // Verify all the old metadata segments are included
      for (int i = 0; i < numOlderSegments; i++) {
        assertTrue(segmentListArg.contains(segmentsZKMetadata.get(i).getSegmentName()));
      }

      // Verify segment3 (recent untracked segment) is NOT included
      assertFalse(segmentListArg.contains("segment3.tar.gz"));

      // Calculate expected total segments that should be deleted
      // ZK metadata segments + untracked segments up to the batch size limit
      int expectedTotalSegments = numOlderSegments + untrackedSegmentsDeletionBatchSize;

      // Verify the total count is as expected
      assertEquals(expectedTotalSegments, segmentListArg.size());

      return null;
    }).when(resourceManager).deleteSegments(anyString(), anyList());
  }


  // This test makes sure that we clean up the segments marked OFFLINE in realtime for more than 7 days
  @Test
  public void testRealtimeLLCCleanup() {
    final int initialNumSegments = 8;
    final long now = System.currentTimeMillis();

    final int replicaCount = 1;

    TableConfig tableConfig = createRealtimeTableConfig1(replicaCount);
    List<String> removedSegments = new ArrayList<>();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager =
        setupSegmentMetadata(tableConfig, now, initialNumSegments, removedSegments);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);
    when(pinotHelixResourceManager.getDataDir()).thenReturn(_tempDir.toString());

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);

    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, conf, null, null);
    RetentionManager retentionManager = createRetentionManager(pinotHelixResourceManager, leadControllerManager, conf,
        controllerMetrics, brokerServiceHelper);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager,
        ControllerConf.ControllerPeriodicTasksConf.DEFAULT_AGED_SEGMENTS_DELETION_BATCH_SIZE);

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), anyList());
  }

  // This test makes sure that we do not clean up last llc completed segments
  @Test
  public void testRealtimeLastLLCCleanup() {
    final long now = System.currentTimeMillis();
    final int replicaCount = 1;

    TableConfig tableConfig = createRealtimeTableConfig1(replicaCount);
    List<String> removedSegments = new ArrayList<>();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager =
        setupSegmentMetadataForPausedTable(tableConfig, now, removedSegments);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);
    when(pinotHelixResourceManager.getDataDir()).thenReturn(_tempDir.toString());

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, conf, null, null);
    RetentionManager retentionManager =
        createRetentionManager(pinotHelixResourceManager, leadControllerManager, conf, controllerMetrics,
            brokerServiceHelper);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager,
        ControllerConf.ControllerPeriodicTasksConf.DEFAULT_AGED_SEGMENTS_DELETION_BATCH_SIZE);

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), anyList());
  }

  private PinotHelixResourceManager setupSegmentMetadata(TableConfig tableConfig, final long now, final int nSegments,
      List<String> segmentsToBeDeleted) {
    final int replicaCount = tableConfig.getReplication();

    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();

    IdealState idealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(REALTIME_TABLE_NAME, replicaCount);

    final int kafkaPartition = 5;
    final long millisInDays = TimeUnit.DAYS.toMillis(1);
    final String serverName = "Server_localhost_0";
    // If we set the segment creation time to a certain value and compare it as being X ms old,
    // then we could get unpredictable results depending on whether it takes more or less than
    // one millisecond to get to RetentionManager time comparison code. To be safe, set the
    // milliseconds off by 1/2 day.
    long segmentCreationTime = now - (nSegments + 1) * millisInDays + millisInDays / 2;
    for (int seq = 1; seq <= nSegments; seq++) {
      segmentCreationTime += millisInDays;
      LLCSegmentName llcSegmentName = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, seq, segmentCreationTime);
      final String segName = llcSegmentName.getSegmentName();
      SegmentZKMetadata segmentZKMetadata = createSegmentZKMetadata(segName, replicaCount, segmentCreationTime);
      if (seq == nSegments) {
        // create consuming segment
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "CONSUMING");
        segmentsZKMetadata.add(segmentZKMetadata);
      } else if (seq == 1) {
        // create IN_PROGRESS metadata absent from ideal state, older than 5 days
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        segmentsZKMetadata.add(segmentZKMetadata);
        segmentsToBeDeleted.add(segmentZKMetadata.getSegmentName());
      } else if (seq == nSegments - 1) {
        // create IN_PROGRESS metadata absent from ideal state, younger than 5 days
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        segmentsZKMetadata.add(segmentZKMetadata);
      } else if (seq % 2 == 0) {
        // create ONLINE segment
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
        idealState.setPartitionState(segName, serverName, "ONLINE");
        segmentsZKMetadata.add(segmentZKMetadata);
      } else {
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "OFFLINE");
        segmentsZKMetadata.add(segmentZKMetadata);
        if (now - segmentCreationTime > RetentionManager.OLD_LLC_SEGMENTS_RETENTION_IN_MILLIS) {
          segmentsToBeDeleted.add(segmentZKMetadata.getSegmentName());
        }
      }
    }

    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);

    return pinotHelixResourceManager;
  }

  private PinotHelixResourceManager setupSegmentMetadataForPausedTable(TableConfig tableConfig, final long now,
      List<String> segmentsToBeDeleted) {
    final int replicaCount = tableConfig.getReplication();

    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();

    IdealState idealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(REALTIME_TABLE_NAME, replicaCount);

    final int kafkaPartition = 5;
    final long millisInDays = TimeUnit.DAYS.toMillis(1);
    final String serverName = "Server_localhost_0";
    LLCSegmentName llcSegmentName0 = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, 0, now);
    SegmentZKMetadata segmentZKMetadata0 = createSegmentZKMetadata(llcSegmentName0.getSegmentName(), replicaCount, now);
    segmentZKMetadata0.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadata0.setStartTime(now - 30 * millisInDays);
    segmentZKMetadata0.setEndTime(now - 20 * millisInDays);
    segmentZKMetadata0.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentsZKMetadata.add(segmentZKMetadata0);
    idealState.setPartitionState(llcSegmentName0.getSegmentName(), serverName, "ONLINE");
    segmentsToBeDeleted.add(llcSegmentName0.getSegmentName());

    LLCSegmentName llcSegmentName1 = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, 1, now);
    SegmentZKMetadata segmentZKMetadata1 = createSegmentZKMetadata(llcSegmentName1.getSegmentName(), replicaCount, now);
    segmentZKMetadata1.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadata1.setStartTime(now - 20 * millisInDays);
    segmentZKMetadata1.setEndTime(now - 10 * millisInDays);
    segmentZKMetadata1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentsZKMetadata.add(segmentZKMetadata1);
    idealState.setPartitionState(llcSegmentName1.getSegmentName(), serverName, "ONLINE");

    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);
    when(pinotHelixResourceManager.getLastLLCCompletedSegments(REALTIME_TABLE_NAME)).thenCallRealMethod();
    when(pinotHelixResourceManager.getLastLLCCompletedSegments(anyList())).thenCallRealMethod();

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);

    return pinotHelixResourceManager;
  }

  private SegmentZKMetadata createSegmentZKMetadata(String segmentName, int replicaCount, long segmentCreationTime) {
    SegmentZKMetadata segmentMetadata = new SegmentZKMetadata(segmentName);
    segmentMetadata.setCreationTime(segmentCreationTime);
    segmentMetadata.setStartOffset(new LongMsgOffset(0L).toString());
    segmentMetadata.setEndOffset(new LongMsgOffset(-1L).toString());

    segmentMetadata.setNumReplicas(replicaCount);
    return segmentMetadata;
  }

  private SegmentZKMetadata mockSegmentZKMetadata(long startTime, long endTime, TimeUnit timeUnit) {
    long creationTime = System.currentTimeMillis();
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getSegmentName()).thenReturn(TEST_TABLE_NAME + creationTime);
    when(segmentZKMetadata.getCreationTime()).thenReturn(creationTime);
    when(segmentZKMetadata.getStartTimeMs()).thenReturn(timeUnit.toMillis(startTime));
    when(segmentZKMetadata.getEndTimeMs()).thenReturn(timeUnit.toMillis(endTime));
    when(segmentZKMetadata.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.DONE);
    return segmentZKMetadata;
  }

  /// Helper method to create a file with content
  private void createFileWithContent(File file, String content) {
    try {
      Files.write(file.toPath(), content.getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Helper method to set file modification time
  private void setFileModificationTime(File file, long timestamp) {
    FileTime fileTime = FileTime.fromMillis(timestamp);
    try {
      Files.setLastModifiedTime(file.toPath(), fileTime);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCreationTimeFallbackOnChange() {
    ControllerConf conf = new ControllerConf();
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, conf, null, null);
    RetentionManager retentionManager =
        createRetentionManager(mockResourceManager, mock(LeadControllerManager.class), conf, controllerMetrics,
            brokerServiceHelper);

    // Default should be false
    assertFalse(retentionManager.isRetentionCreationTimeFallbackEnabled());

    // Simulate cluster config change to enable
    String configKey = ControllerConf.ControllerPeriodicTasksConf.ENABLE_RETENTION_CREATION_TIME_FALLBACK;
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(configKey, "true");
    retentionManager.onChange(Set.of(configKey), clusterConfigs);
    assertTrue(retentionManager.isRetentionCreationTimeFallbackEnabled());

    // Simulate cluster config change to disable
    clusterConfigs.put(configKey, "false");
    retentionManager.onChange(Set.of(configKey), clusterConfigs);
    assertFalse(retentionManager.isRetentionCreationTimeFallbackEnabled());

    // Invalid value should keep current value
    clusterConfigs.put(configKey, "invalid");
    retentionManager.onChange(Set.of(configKey), clusterConfigs);
    assertFalse(retentionManager.isRetentionCreationTimeFallbackEnabled());

    // Simulate config key deletion (null value) while feature is enabled — should revert to default (false)
    clusterConfigs.put(configKey, "true");
    retentionManager.onChange(Set.of(configKey), clusterConfigs);
    assertTrue(retentionManager.isRetentionCreationTimeFallbackEnabled());

    // Now delete the config key: changedConfigs contains the key, but clusterConfigs.get() returns null
    Map<String, String> configsWithDeletedKey = new HashMap<>();
    configsWithDeletedKey.put(configKey, null);
    retentionManager.onChange(Set.of(configKey), configsWithDeletedKey);
    assertFalse(retentionManager.isRetentionCreationTimeFallbackEnabled());
  }

  @Test
  public void testRetentionWithInvalidEndTimeAndCreationTimeFallback() {
    long now = System.currentTimeMillis();
    // Creation time must exceed the table's retention period (365 days) to be purgeable
    long fourHundredDaysAgoMs = now - TimeUnit.DAYS.toMillis(400);

    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();

    // Segment with invalid end time but old creation time — should be deleted when fallback is enabled
    SegmentZKMetadata invalidEndTimeSeg = mock(SegmentZKMetadata.class);
    when(invalidEndTimeSeg.getSegmentName()).thenReturn("seg_invalid_endtime");
    when(invalidEndTimeSeg.getEndTimeMs()).thenReturn(-1L);
    when(invalidEndTimeSeg.getCreationTime()).thenReturn(fourHundredDaysAgoMs);
    when(invalidEndTimeSeg.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.DONE);
    segmentsZKMetadata.add(invalidEndTimeSeg);

    // Segment with valid end time that is recent — should NOT be deleted
    SegmentZKMetadata recentSeg = mock(SegmentZKMetadata.class);
    when(recentSeg.getSegmentName()).thenReturn("seg_recent");
    when(recentSeg.getEndTimeMs()).thenReturn(now);
    when(recentSeg.getCreationTime()).thenReturn(now);
    when(recentSeg.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.DONE);
    segmentsZKMetadata.add(recentSeg);

    final TableConfig tableConfig = createOfflineTableConfig();
    List<String> expectedDeletedSegments = List.of("seg_invalid_endtime");

    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    setupPinotHelixResourceManager(tableConfig, expectedDeletedSegments, pinotHelixResourceManager,
        leadControllerManager);

    when(pinotHelixResourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getDataDir()).thenReturn(_tempDir.toString());

    // Test with fallback ENABLED
    ControllerConf conf = new ControllerConf();
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    conf.setProperty(ControllerConf.ControllerPeriodicTasksConf.ENABLE_RETENTION_CREATION_TIME_FALLBACK, "true");
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, conf, null, null);
    RetentionManager retentionManager =
        createRetentionManager(pinotHelixResourceManager, leadControllerManager, conf, controllerMetrics,
            brokerServiceHelper);
    retentionManager.start();
    retentionManager.run();

    // Verify deleteSegments is called — setupPinotHelixResourceManager's doAnswer
    // already asserts the correct segments via TestNG assertions
    verify(pinotHelixResourceManager, times(1)).deleteSegments(eq(OFFLINE_TABLE_NAME), anyList());
  }

  @Test
  public void testSizeOnlyRetentionDeletesOldestSegmentsUntilWithinLimit() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("newest", 60, now),
        createSizeRetentionSegment("oldest", 60, now - 2),
        createSizeRetentionSegment("middle", 60, now - 1));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

    // Exercise the complete table pass: a time-retention configuration is deliberately absent.
    retentionManager.processTable(OFFLINE_TABLE_NAME);

    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest", "middle"));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
  }

  @Test
  public void testSizeRetentionKeepsSegmentsAtOrBelowLimit() {
    for (String limit : List.of("100B", "101B")) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, limit);
      tableConfig.getValidationConfig().setReplication("3");
      long now = System.currentTimeMillis();
      List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 40, now - 1),
          createSizeRetentionSegment("newest", 60, now));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = mock(ControllerMetrics.class);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

      retentionManager.manageSizeBasedRetention(tableConfig);

      // Stored archive sizes count once, regardless of table replication.
      verify(resourceManager, never()).deleteSegments(anyString(), anyList());
      verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
    }
  }

  @Test
  public void testSizeRetentionKeepsNewestOfflineSegmentWhenItAloneExceedsLimit() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("newest", 120, now),
        createSizeRetentionSegment("oldest", 40, now - 1));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionNeverDeletesOnlyOfflineSegment() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
    List<SegmentZKMetadata> segments =
        List.of(createSizeRetentionSegment("only", 100, System.currentTimeMillis()));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionOrderingWithCreationAndPushTimeFallbacks() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "20B");
    long now = System.currentTimeMillis();
    SegmentZKMetadata creationFallback = createSizeRetentionSegment("creationFallback", 10, -1);
    creationFallback.setCreationTime(now - 1);
    SegmentZKMetadata pushFallback = createSizeRetentionSegment("pushFallback", 10, -1);
    pushFallback.setCreationTime(-1);
    pushFallback.setPushTime(now - 4);
    List<SegmentZKMetadata> segments = List.of(creationFallback,
        createSizeRetentionSegment("b", 10, now - 3), pushFallback,
        createSizeRetentionSegment("oldest", 10, now - 5), createSizeRetentionSegment("a", 10, now - 3));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest", "pushFallback", "a"));
  }

  @Test
  public void testSizeRetentionProtectsIncompleteAndLastCompletedRealtimeSegments() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.REALTIME, "50B");
    long now = System.currentTimeMillis();
    SegmentZKMetadata oldest = createSizeRetentionSegment(
        new LLCSegmentName(TEST_TABLE_NAME, 0, 4, now - 3).getSegmentName(), 20, now - 3);
    oldest.setStatus(Status.DONE);
    SegmentZKMetadata lastCompleted = createSizeRetentionSegment(
        new LLCSegmentName(TEST_TABLE_NAME, 0, 5, now - 1).getSegmentName(), 40, now - 1);
    lastCompleted.setStatus(Status.DONE);
    SegmentZKMetadata consuming = createSizeRetentionSegment("consuming", -1, now - 5);
    consuming.setStatus(Status.IN_PROGRESS);
    SegmentZKMetadata committing = createSizeRetentionSegment("committing", 999, now - 4);
    committing.setStatus(Status.COMMITTING);
    SegmentZKMetadata uploaded = createSizeRetentionSegment("uploaded", 60, now - 2);
    List<SegmentZKMetadata> segments = List.of(lastCompleted, committing, uploaded, consuming, oldest);
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(REALTIME_TABLE_NAME, List.of(oldest.getSegmentName(), "uploaded"));
  }

  @Test
  public void testSizeRetentionSkipsTableWhenCompletedSegmentSizeIsUnknown() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "50B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 60, now - 2),
        createSizeRetentionSegment("unknown", -1, now - 1), createSizeRetentionSegment("newest", 60, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionIgnoresMetadataOutsideIdealState() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 40, now - 2),
        createSizeRetentionSegment("orphan", -1, now - 1), createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager);
    when(resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false)).thenReturn(List.of("oldest", "newest"));

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
  }

  @Test
  public void testSizeRetentionStopsAtInProgressLineageEvenWhenExclusiveDeleteIsDisabled() {
    for (boolean exclusiveDelete : List.of(true, false)) {
      for (String retentionSize : List.of("50B", "160B")) {
        TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, retentionSize);
        long now = System.currentTimeMillis();
        List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("olderLive", 60, now - 3),
            createSizeRetentionSegment("source", 80, now - 2),
            createSizeRetentionSegment("destination", -1, now - 1),
            createSizeRetentionSegment("newerLive", 20, now - 1),
            createSizeRetentionSegment("newestLive", 60, now));
        PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
        ControllerMetrics metrics = mock(ControllerMetrics.class);
        ControllerConf conf = new ControllerConf();
        conf.setProperty(ControllerConf.LINEAGE_EXCLUSIVE_DELETE_ENABLED, exclusiveDelete);
        RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics,
            conf, mock(BrokerServiceHelper.class));
        setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "source", "destination",
            LineageEntryState.IN_PROGRESS);

        retentionManager.manageSizeBasedRetention(tableConfig);

        // Evict the older prefix, then stop at the source instead of jumping over it to newer live data.
        verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("olderLive"));
        verify(resourceManager, never()).deleteSegmentsForLineageCleanup(anyString(), anyList());
        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, retentionSize.equals("160B") ? 0 : 1);
      }
    }
  }

  @Test
  public void testSizeRetentionExcludesCompletedReplacedSegmentsFromAccounting() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("olderLive", 60, now - 3),
        createSizeRetentionSegment("source", -1, now - 2),
        createSizeRetentionSegment("destination", 40, now - 1), createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    SegmentLineage lineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    lineage.addLineageEntry("replacement",
        new LineageEntry(List.of("source"), List.of("destination"), LineageEntryState.COMPLETED, now));
    when(resourceManager.getPropertyStore().get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(lineage.toZNRecord());

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).getSegmentsFor(OFFLINE_TABLE_NAME, false);
    verify(resourceManager, never()).getSegmentsFor(OFFLINE_TABLE_NAME, true);
    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("olderLive"));
    verify(resourceManager.getPropertyStore(), times(2))
        .get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
  }

  @Test
  public void testSizeRetentionSeesLineageStartedWhileReadingIdealState() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "50B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("olderLive", 60, now - 3),
        createSizeRetentionSegment("source", 80, now - 2),
        createSizeRetentionSegment("destination", -1, now - 1), createSizeRetentionSegment("newerLive", 20, now - 1),
        createSizeRetentionSegment("newestLive", 60, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    SegmentLineage lineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    lineage.addLineageEntry("replacement",
        new LineageEntry(List.of("source"), List.of("destination"), LineageEntryState.IN_PROGRESS, now));
    AtomicReference<ZNRecord> lineageRecord = new AtomicReference<>();
    ZkHelixPropertyStore<ZNRecord> propertyStore = resourceManager.getPropertyStore();
    when(propertyStore.get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenAnswer(invocation -> lineageRecord.get());
    when(resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false)).thenAnswer(invocation -> {
      lineageRecord.set(lineage.toZNRecord());
      return segments.stream().map(SegmentZKMetadata::getSegmentName).toList();
    });

    retentionManager.manageSizeBasedRetention(tableConfig);

    // The newer lineage snapshot sees replacement bytes exposed by the just-read ideal state.
    var readOrder = inOrder(resourceManager, propertyStore);
    readOrder.verify(resourceManager).getSegmentsFor(OFFLINE_TABLE_NAME, false);
    readOrder.verify(propertyStore).get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT));
    readOrder.verify(resourceManager).getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    readOrder.verify(propertyStore).get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT));
    readOrder.verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("olderLive"));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionStopsBeforeExcludedLineageShadowSegment() {
    for (LineageEntryState state : LineageEntryState.values()) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
      long now = System.currentTimeMillis();
      String shadow = state == LineageEntryState.COMPLETED ? "source" : "destination";
      String live = state == LineageEntryState.COMPLETED ? "destination" : "source";
      List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 40, now - 4),
          createSizeRetentionSegment(shadow, -1, now - 3), createSizeRetentionSegment("newerLive", 40, now - 2),
          createSizeRetentionSegment(live, 80, now - 1), createSizeRetentionSegment("newest", 40, now));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = mock(ControllerMetrics.class);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
      setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "source", "destination", state);

      retentionManager.manageSizeBasedRetention(tableConfig);

      // Shadow bytes do not count, but their position still prevents deleting newer queryable data.
      verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
      verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    }
  }

  @Test
  public void testSizeRetentionDoesNotEvictWhenOldestSegmentIsLineageOwned() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("source", 80, now - 2),
        createSizeRetentionSegment("destination", -1, now - 1), createSizeRetentionSegment("newerLive", 40, now - 1),
        createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "source", "destination",
        LineageEntryState.IN_PROGRESS);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionCannotCrossUnlocatableLineageShadow() {
    for (boolean missingMetadata : List.of(true, false)) {
      for (String retentionSize : List.of("50B", "200B")) {
        TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, retentionSize);
        long now = System.currentTimeMillis();
        List<SegmentZKMetadata> segments = new ArrayList<>(List.of(createSizeRetentionSegment("oldest", 40, now - 3),
            createSizeRetentionSegment("newerLive", 40, now - 2),
            createSizeRetentionSegment("destination", 40, now - 1), createSizeRetentionSegment("newest", 40, now)));
        if (!missingMetadata) {
          SegmentZKMetadata undatedSource = createSizeRetentionSegment("source", -1, -1);
          undatedSource.setCreationTime(-1);
          undatedSource.setPushTime(-1);
          segments.add(undatedSource);
        }
        PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
        ControllerMetrics metrics = mock(ControllerMetrics.class);
        RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
        when(resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false))
            .thenReturn(List.of("oldest", "newerLive", "source", "destination", "newest"));
        setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "source", "destination",
            LineageEntryState.COMPLETED);

        retentionManager.manageSizeBasedRetention(tableConfig);

        // Without the active shadow's timestamp the prefix cannot be located safely.
        // An already satisfied cap is healthy.
        verify(resourceManager, never()).deleteSegments(anyString(), anyList());
        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, retentionSize.equals("200B") ? 0 : 1);
      }
    }
  }

  @Test
  public void testSizeRetentionLineageBarrierUsesTimestampFallbacksAndNameTieBreak() {
    for (boolean usePushTime : List.of(true, false)) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
      long now = System.currentTimeMillis();
      SegmentZKMetadata shadow = createSizeRetentionSegment("b", -1, -1);
      shadow.setCreationTime(usePushTime ? -1 : now);
      shadow.setPushTime(now);
      List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("a", 20, now), shadow,
          createSizeRetentionSegment("z", 20, now), createSizeRetentionSegment("newest", 20, now + 1));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = mock(ControllerMetrics.class);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
      setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "b", "absentDestination",
          LineageEntryState.COMPLETED);

      retentionManager.manageSizeBasedRetention(tableConfig);

      verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("a"));
      verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    }
  }

  @Test
  public void testSizeRetentionIgnoresLineageWithoutActiveBlockedSegments() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "40B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 40, now - 2),
        createSizeRetentionSegment("middle", 40, now - 1), createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "absentSource", "absentDestination",
        LineageEntryState.IN_PROGRESS);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest", "middle"));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
  }

  @Test
  public void testSizeRetentionRevertedSourceCreatesBarrier() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "40B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 40, now - 3),
        createSizeRetentionSegment("source", 40, now - 2), createSizeRetentionSegment("destination", -1, now - 1),
        createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "source", "destination", LineageEntryState.REVERTED);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionCompletedDestinationCreatesBarrierWithSourceAbsentOrNewer() {
    for (boolean sourceAbsent : List.of(true, false)) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
      long now = System.currentTimeMillis();
      List<SegmentZKMetadata> segments = new ArrayList<>(List.of(createSizeRetentionSegment("oldest", 40, now - 3),
          createSizeRetentionSegment("destination", 40, now - 2), createSizeRetentionSegment("newerLive", 40, now - 1),
          createSizeRetentionSegment("newest", 40, now)));
      if (!sourceAbsent) {
        segments.add(createSizeRetentionSegment("source", -1, now + 1));
      }
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = mock(ControllerMetrics.class);
      ControllerConf conf = new ControllerConf();
      conf.setProperty(ControllerConf.LINEAGE_EXCLUSIVE_DELETE_ENABLED, false);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics,
          conf, mock(BrokerServiceHelper.class));
      setupSizeRetentionLineage(resourceManager, OFFLINE_TABLE_NAME, "source", "destination",
          LineageEntryState.COMPLETED, now - TimeUnit.DAYS.toMillis(400));

      retentionManager.manageSizeBasedRetention(tableConfig);

      verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
      verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    }
  }

  @Test
  public void testSizeRetentionRejectsPrefixWhenLineageMembershipChangesWithExclusiveDeleteDisabled() {
    for (LineageEntryState state : LineageEntryState.values()) {
      for (boolean selectedTarget : List.of(true, false)) {
        TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "60B");
        long now = System.currentTimeMillis();
        List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("lateMarker", 0, now - 3),
            createSizeRetentionSegment("oldest", 60, now - 2), createSizeRetentionSegment("middle", 60, now - 1),
            createSizeRetentionSegment("newest", 60, now));
        PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
        ControllerMetrics metrics = mock(ControllerMetrics.class);
        ControllerConf conf = new ControllerConf();
        conf.setProperty(ControllerConf.LINEAGE_EXCLUSIVE_DELETE_ENABLED, false);
        RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics,
            conf, mock(BrokerServiceHelper.class));
        SegmentLineage lineage = new SegmentLineage(OFFLINE_TABLE_NAME);
        lineage.addLineageEntry("replacement", new LineageEntry(List.of("absentSource"),
            List.of(selectedTarget ? "oldest" : "lateMarker"), state, now));
        when(resourceManager.getPropertyStore().get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
            .thenReturn(null, lineage.toZNRecord());

        retentionManager.manageSizeBasedRetention(tableConfig);

        // An earlier boundary change invalidates the batch even when no selected target is a new lineage member.
        verify(resourceManager, never()).deleteSegments(anyString(), anyList());
        verify(resourceManager.getPropertyStore(), times(2))
            .get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT));
        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
      }
    }
  }

  @Test
  public void testSizeRetentionReadsFreshLineageAndDeletesUnderUpdaterLock() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "60B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 60, now - 1),
        createSizeRetentionSegment("newest", 60, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    Object updaterLock = resourceManager.getLineageUpdaterLock(OFFLINE_TABLE_NAME);
    AtomicInteger lineageReads = new AtomicInteger();
    when(resourceManager.getPropertyStore().get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenAnswer(invocation -> {
          if (lineageReads.incrementAndGet() == 2) {
            assertTrue(Thread.holdsLock(updaterLock));
          }
          return null;
        });
    when(resourceManager.deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"))).thenAnswer(invocation -> {
      assertTrue(Thread.holdsLock(updaterLock));
      return PinotResourceManagerResponse.SUCCESS;
    });

    retentionManager.manageSizeBasedRetention(tableConfig);

    assertEquals(lineageReads.get(), 2);
    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
  }

  @Test
  public void testSizeRetentionRejectsPrefixWhenLineageStateOrTimestampChanges() {
    for (boolean stateChanges : List.of(true, false)) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "160B");
      long now = System.currentTimeMillis();
      List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 60, now - 3),
          createSizeRetentionSegment("source", 80, now - 2), createSizeRetentionSegment("destination", 40, now - 1),
          createSizeRetentionSegment("newest", 60, now));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = mock(ControllerMetrics.class);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
      SegmentLineage planningLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
      planningLineage.addLineageEntry("replacement",
          new LineageEntry(List.of("source"), List.of("destination"), LineageEntryState.IN_PROGRESS, now));
      SegmentLineage latestLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
      latestLineage.addLineageEntry("replacement", new LineageEntry(List.of("source"), List.of("destination"),
          stateChanges ? LineageEntryState.COMPLETED : LineageEntryState.IN_PROGRESS, stateChanges ? now : now + 1));
      when(resourceManager.getPropertyStore().get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
          .thenReturn(planningLineage.toZNRecord(), latestLineage.toZNRecord());

      retentionManager.manageSizeBasedRetention(tableConfig);

      // The members are unchanged, but a state transition changes which side is queryable and how bytes are counted.
      verify(resourceManager, never()).deleteSegments(anyString(), anyList());
      verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    }
  }

  @Test
  public void testSizeRetentionPublishesBlockedGaugeWhenFreshLineageReadThrows() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "60B");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 60, now - 1),
        createSizeRetentionSegment("newest", 60, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    when(resourceManager.getPropertyStore().get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(null).thenThrow(new IllegalStateException("Fresh lineage read failed"));

    assertThrows(IllegalStateException.class, () -> retentionManager.manageSizeBasedRetention(tableConfig));

    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionRecoveryProtectionCannotHideLineageBarrier() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.REALTIME, "1B");
    long now = System.currentTimeMillis();
    String sourceName = new LLCSegmentName(TEST_TABLE_NAME, 0, 5, now - 3).getSegmentName();
    SegmentZKMetadata source = createSizeRetentionSegment(sourceName, 80, now - 3);
    source.setStatus(Status.DONE);
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 40, now - 4), source,
        createSizeRetentionSegment("destination", -1, now - 2), createSizeRetentionSegment("newerLive", 40, now - 1),
        createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    setupSizeRetentionLineage(resourceManager, REALTIME_TABLE_NAME, sourceName, "destination",
        LineageEntryState.IN_PROGRESS);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(REALTIME_TABLE_NAME, List.of("oldest"));
    verifySizeRetentionGauge(metrics, REALTIME_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionForHybridStopsAtLineageBarrier() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.REALTIME, "1B");
    long boundary = System.currentTimeMillis();
    SegmentZKMetadata source = createSizeRetentionSegment("source", 80, -1);
    source.setCreationTime(boundary - 2);
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 20, boundary - 3), source,
        createSizeRetentionSegment("coveredNewer", 20, boundary - 1),
        createSizeRetentionSegment("newest", 20, boundary + 1),
        createSizeRetentionSegment("destination", -1, boundary + 2));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    BrokerServiceHelper brokerServiceHelper = mock(BrokerServiceHelper.class);
    ControllerConf conf = new ControllerConf();
    conf.setProperty(ControllerConf.ENABLE_HYBRID_TABLE_RETENTION_STRATEGY, true);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics,
        conf, brokerServiceHelper);
    setupSizeRetentionOfflineCounterpart(resourceManager);
    setupSizeRetentionLineage(resourceManager, REALTIME_TABLE_NAME, "source", "destination",
        LineageEntryState.IN_PROGRESS);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(REALTIME_TABLE_NAME, List.of("oldest"));
    verify(resourceManager, never()).getOfflineTableConfig(anyString());
    verify(brokerServiceHelper, never()).getTimeBoundaryInfo(any(TableConfig.class));
    verifySizeRetentionGauge(metrics, REALTIME_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionKeepsNewestOfflineSegmentUsingFallbackAndNameTieBreak() {
    for (boolean usePushTime : List.of(true, false)) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
      long now = System.currentTimeMillis();
      SegmentZKMetadata newest = createSizeRetentionSegment("z", 20, -1);
      newest.setCreationTime(usePushTime ? -1 : now);
      newest.setPushTime(now);
      SegmentZKMetadata sameTimestamp = createSizeRetentionSegment("a", 20, now);
      List<SegmentZKMetadata> segments = List.of(newest, sameTimestamp,
          createSizeRetentionSegment("oldest", 20, now - 1));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager);

      retentionManager.manageSizeBasedRetention(tableConfig);

      verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest", "a"));
    }
  }

  @Test
  public void testSizeRetentionGaugeClearsForEmptyTableAndWhenLeadershipIsLost() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, List.of(), resourceManager, metrics);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);

    retentionManager.nonLeaderCleanup(List.of(OFFLINE_TABLE_NAME));

    verify(metrics).removeTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.SIZE_RETENTION_BLOCKED);
  }

  @Test
  public void testSizeRetentionGaugeRecoversWhenUnknownSizeBecomesAvailable() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    long now = System.currentTimeMillis();
    SegmentZKMetadata unknown = createSizeRetentionSegment("unknown", -1, now - 1);
    List<SegmentZKMetadata> segments = List.of(unknown, createSizeRetentionSegment("newest", 40, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    unknown.setSizeInBytes(60);
    clearInvocations(metrics);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
  }

  @Test
  public void testSizeRetentionGaugeRegistryLifecycle() {
    for (boolean refreshTable : List.of(true, false)) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
      IngestionConfig refreshConfig = new IngestionConfig();
      refreshConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", null));
      tableConfig.setIngestionConfig(refreshTable ? refreshConfig : null);
      tableConfig.getValidationConfig().setRetentionSize(refreshTable ? "100B" : null);
      long now = System.currentTimeMillis();
      List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 60, now - 1),
          createSizeRetentionSegment("newest", 60, now));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = spy(new ControllerMetrics("sizeRetentionLifecycle.",
          PinotMetricUtils.getPinotMetricsRegistry()));
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
      try {
        assertFalse(
            MetricValueUtils.tableGaugeExists(metrics, OFFLINE_TABLE_NAME, ControllerGauge.SIZE_RETENTION_BLOCKED));

        retentionManager.manageSizeBasedRetention(tableConfig);

        verifySizeRetentionGaugeRemoved(metrics);
        verify(resourceManager, never()).deleteSegments(anyString(), anyList());
        clearInvocations(metrics);
        tableConfig.setIngestionConfig(null);
        tableConfig.getValidationConfig().setRetentionSize("200B");

        retentionManager.manageSizeBasedRetention(tableConfig);

        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
        assertSizeRetentionGaugeValue(metrics, 0);
        clearInvocations(metrics);
        tableConfig.getValidationConfig().setRetentionSize("100B");
        when(resourceManager.deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest")))
            .thenReturn(PinotResourceManagerResponse.failure("Targets became lineage-locked"),
                PinotResourceManagerResponse.SUCCESS);

        retentionManager.manageSizeBasedRetention(tableConfig);

        verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
        assertSizeRetentionGaugeValue(metrics, 1);
        clearInvocations(metrics);

        retentionManager.manageSizeBasedRetention(tableConfig);

        verify(resourceManager, times(2)).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
        assertSizeRetentionGaugeValue(metrics, 0);
        clearInvocations(metrics, resourceManager);
        tableConfig.getValidationConfig().setRetentionSize("invalid");

        retentionManager.manageSizeBasedRetention(tableConfig);

        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
        assertSizeRetentionGaugeValue(metrics, 1);
        clearInvocations(metrics);
        tableConfig.setIngestionConfig(refreshTable ? refreshConfig : null);
        tableConfig.getValidationConfig().setRetentionSize(refreshTable ? "100B" : null);

        retentionManager.manageSizeBasedRetention(tableConfig);

        verifySizeRetentionGaugeRemoved(metrics);
        verify(resourceManager, never()).deleteSegments(anyString(), anyList());
        clearInvocations(metrics);
        tableConfig.setIngestionConfig(null);
        tableConfig.getValidationConfig().setRetentionSize("200B");

        retentionManager.manageSizeBasedRetention(tableConfig);

        verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 0);
        assertSizeRetentionGaugeValue(metrics, 0);
        clearInvocations(metrics);

        retentionManager.nonLeaderCleanup(List.of(OFFLINE_TABLE_NAME));

        verifySizeRetentionGaugeRemoved(metrics);
      } finally {
        metrics.removeTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.SIZE_RETENTION_BLOCKED);
      }
    }
  }

  @Test
  public void testSizeRetentionPublishesBlockedGaugeOnceWhenMetadataReadThrows() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "100B");
    List<SegmentZKMetadata> segments =
        List.of(createSizeRetentionSegment("only", 60, System.currentTimeMillis()));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    when(resourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenThrow(new IllegalStateException("Read failed"));

    assertThrows(IllegalStateException.class, () -> retentionManager.manageSizeBasedRetention(tableConfig));

    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
  }

  @Test
  public void testSizeRetentionForHybridEvictsOldestWithoutOfflineCoverage() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.REALTIME, "20B");
    long boundary = System.currentTimeMillis();
    SegmentZKMetadata invalidEndTime = createSizeRetentionSegment("invalidEndTime", 20, -1);
    invalidEndTime.setCreationTime(boundary - 2);
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("before", 20, boundary - 1),
        createSizeRetentionSegment("at", 20, boundary), createSizeRetentionSegment("after", 20, boundary + 1),
        createSizeRetentionSegment("newest", 20, boundary + 2), invalidEndTime);
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    BrokerServiceHelper brokerServiceHelper = mock(BrokerServiceHelper.class);
    when(brokerServiceHelper.getTimeBoundaryInfo(any(TableConfig.class)))
        .thenReturn(new TimeBoundaryInfo("ms", Long.toString(boundary)));
    ControllerConf conf = new ControllerConf();
    conf.setProperty(ControllerConf.ENABLE_HYBRID_TABLE_RETENTION_STRATEGY, true);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics,
        conf, brokerServiceHelper);
    setupSizeRetentionOfflineCounterpart(resourceManager);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(REALTIME_TABLE_NAME, List.of("invalidEndTime", "before", "at", "after"));
    verify(resourceManager, never()).getOfflineTableConfig(anyString());
    verify(brokerServiceHelper, never()).getTimeBoundaryInfo(any(TableConfig.class));
    verifySizeRetentionGauge(metrics, REALTIME_TABLE_NAME, 0);
  }

  @Test
  public void testSizeRetentionDoesNotConsultOfflineTimeBoundary() {
    for (boolean hybridStrategy : List.of(true, false)) {
      for (boolean offlineCounterpart : List.of(true, false)) {
        TableConfig tableConfig = createSizeRetentionTableConfig(TableType.REALTIME, "20B");
        long now = System.currentTimeMillis();
        List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("oldest", 20, now - 1),
            createSizeRetentionSegment("newest", 20, now));
        PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
        ControllerMetrics metrics = mock(ControllerMetrics.class);
        BrokerServiceHelper brokerServiceHelper = mock(BrokerServiceHelper.class);
        when(brokerServiceHelper.getTimeBoundaryInfo(any(TableConfig.class)))
            .thenThrow(new IllegalStateException("Time boundary unavailable"));
        ControllerConf conf = new ControllerConf();
        conf.setProperty(ControllerConf.ENABLE_HYBRID_TABLE_RETENTION_STRATEGY, hybridStrategy);
        RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics,
            conf, brokerServiceHelper);
        if (offlineCounterpart) {
          setupSizeRetentionOfflineCounterpart(resourceManager);
        }

        retentionManager.manageSizeBasedRetention(tableConfig);

        verify(resourceManager).deleteSegments(REALTIME_TABLE_NAME, List.of("oldest"));
        verify(resourceManager, never()).getOfflineTableConfig(anyString());
        verify(brokerServiceHelper, never()).getTimeBoundaryInfo(any(TableConfig.class));
        verifySizeRetentionGauge(metrics, REALTIME_TABLE_NAME, 0);
      }
    }
  }

  @Test
  public void testSizeRetentionRechecksMetadataAfterTimeRetention() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "70B");
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("365");
    long now = System.currentTimeMillis();
    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(
        createSizeRetentionSegment("expired", 80, now - TimeUnit.DAYS.toMillis(400)),
        createSizeRetentionSegment("recent", 60, now - 1), createSizeRetentionSegment("newest", 30, now)));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager);
    when(resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false)).thenAnswer(
        invocation -> segments.stream().map(SegmentZKMetadata::getSegmentName).toList());
    doAnswer(invocation -> {
      List<String> deletedSegments = invocation.getArgument(1);
      segments.removeIf(segment -> deletedSegments.contains(segment.getSegmentName()));
      return PinotResourceManagerResponse.SUCCESS;
    }).when(resourceManager).deleteSegments(eq(OFFLINE_TABLE_NAME), anyList());

    retentionManager.processTable(OFFLINE_TABLE_NAME);

    var deletionOrder = inOrder(resourceManager);
    deletionOrder.verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("expired"));
    deletionOrder.verify(resourceManager).getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    deletionOrder.verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("recent"));
    assertEquals(segments.stream().map(SegmentZKMetadata::getSegmentName).toList(), List.of("newest"));
  }

  @Test
  public void testSizeRetentionSkipsTableWhenActiveMetadataIsMissing() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "50B");
    List<SegmentZKMetadata> segments =
        List.of(createSizeRetentionSegment("oversized", 100, System.currentTimeMillis()));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);
    when(resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false)).thenReturn(List.of("oversized", "missing"));

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager, never()).deleteSegments(anyString(), anyList());
    verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
  }

  @Test
  public void testSizeRetentionKeepsUndatedAndEmptySegments() {
    TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "50B");
    long now = System.currentTimeMillis();
    SegmentZKMetadata undated = createSizeRetentionSegment("undated", 80, -1);
    undated.setCreationTime(-1);
    List<SegmentZKMetadata> segments = List.of(createSizeRetentionSegment("empty", 0, now - 1), undated,
        createSizeRetentionSegment("oldest", 60, now - 2), createSizeRetentionSegment("newest", 60, now));
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager);

    retentionManager.manageSizeBasedRetention(tableConfig);

    verify(resourceManager).deleteSegments(OFFLINE_TABLE_NAME, List.of("oldest"));
  }

  @Test
  public void testSizeRetentionSkipsMalformedStoredConfiguration() {
    for (String retentionSize : List.of("invalid", "0B", "-1B", "")) {
      TableConfig tableConfig = createSizeRetentionTableConfig(TableType.OFFLINE, retentionSize);
      List<SegmentZKMetadata> segments =
          List.of(createSizeRetentionSegment("oversized", 100, System.currentTimeMillis()));
      PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
      ControllerMetrics metrics = mock(ControllerMetrics.class);
      RetentionManager retentionManager = createSizeRetentionManager(tableConfig, segments, resourceManager, metrics);

      retentionManager.manageSizeBasedRetention(tableConfig);

      verify(resourceManager, never()).deleteSegments(anyString(), anyList());
      verifySizeRetentionGauge(metrics, OFFLINE_TABLE_NAME, 1);
    }
  }

  private TableConfig createSizeRetentionTableConfig(TableType tableType, String retentionSize) {
    TableConfig tableConfig = new TableConfigBuilder(tableType).setTableName(TEST_TABLE_NAME).build();
    tableConfig.getValidationConfig().setRetentionSize(retentionSize);
    return tableConfig;
  }

  private void verifySizeRetentionGauge(ControllerMetrics metrics, String tableName, long value) {
    verify(metrics).setOrUpdateTableGauge(tableName, ControllerGauge.SIZE_RETENTION_BLOCKED, value);
    // Publishing only the final result prevents an otherwise healthy pass from exposing a transient blocked value.
    verify(metrics, times(1)).setOrUpdateTableGauge(eq(tableName), eq(ControllerGauge.SIZE_RETENTION_BLOCKED),
        anyLong());
  }

  private void assertSizeRetentionGaugeValue(ControllerMetrics metrics, long value) {
    assertTrue(MetricValueUtils.tableGaugeExists(metrics, OFFLINE_TABLE_NAME, ControllerGauge.SIZE_RETENTION_BLOCKED));
    assertEquals(MetricValueUtils.getTableGaugeValue(metrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SIZE_RETENTION_BLOCKED), value);
  }

  private void verifySizeRetentionGaugeRemoved(ControllerMetrics metrics) {
    verify(metrics).removeTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.SIZE_RETENTION_BLOCKED);
    verify(metrics, never()).setOrUpdateTableGauge(eq(OFFLINE_TABLE_NAME),
        eq(ControllerGauge.SIZE_RETENTION_BLOCKED), anyLong());
    assertFalse(MetricValueUtils.tableGaugeExists(metrics, OFFLINE_TABLE_NAME, ControllerGauge.SIZE_RETENTION_BLOCKED));
  }

  private SegmentZKMetadata createSizeRetentionSegment(String segmentName, long sizeInBytes, long endTimeMs) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(segmentName);
    metadata.setSizeInBytes(sizeInBytes);
    metadata.setTimeUnit(TimeUnit.MILLISECONDS);
    metadata.setEndTime(endTimeMs);
    metadata.setCreationTime(System.currentTimeMillis());
    return metadata;
  }

  private RetentionManager createSizeRetentionManager(TableConfig tableConfig, List<SegmentZKMetadata> segments,
      PinotHelixResourceManager resourceManager) {
    return createSizeRetentionManager(tableConfig, segments, resourceManager, mock(ControllerMetrics.class));
  }

  private RetentionManager createSizeRetentionManager(TableConfig tableConfig, List<SegmentZKMetadata> segments,
      PinotHelixResourceManager resourceManager, ControllerMetrics metrics) {
    return createSizeRetentionManager(tableConfig, segments, resourceManager, metrics, new ControllerConf(),
        mock(BrokerServiceHelper.class));
  }

  private RetentionManager createSizeRetentionManager(TableConfig tableConfig, List<SegmentZKMetadata> segments,
      PinotHelixResourceManager resourceManager, ControllerMetrics metrics, ControllerConf conf,
      BrokerServiceHelper brokerServiceHelper) {
    String tableName = tableConfig.getTableName();
    when(resourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(resourceManager.getSegmentsZKMetadata(tableName)).thenReturn(segments);
    when(resourceManager.getSegmentsFor(tableName, false))
        .thenReturn(segments.stream().map(SegmentZKMetadata::getSegmentName).toList());
    when(resourceManager.deleteSegments(eq(tableName), anyList())).thenReturn(PinotResourceManagerResponse.SUCCESS);
    when(resourceManager.getPropertyStore()).thenReturn(mock(ZkHelixPropertyStore.class));
    when(resourceManager.getLineageUpdaterLock(tableName)).thenReturn(new Object());
    when(resourceManager.getLastLLCCompletedSegments(anyList())).thenCallRealMethod();
    when(resourceManager.getLastLLCCompletedSegments(tableName)).thenCallRealMethod();
    conf.setUntrackedSegmentDeletionEnabled(false);
    return createRetentionManager(resourceManager, mock(LeadControllerManager.class), conf,
        metrics, brokerServiceHelper);
  }

  private void setupSizeRetentionOfflineCounterpart(PinotHelixResourceManager resourceManager) {
    TableConfig offlineConfig = createSizeRetentionTableConfig(TableType.OFFLINE, "1B");
    when(resourceManager.getOfflineTableConfig(TEST_TABLE_NAME)).thenReturn(offlineConfig);
  }

  private void setupSizeRetentionLineage(PinotHelixResourceManager resourceManager, String tableName,
      String source, String destination, LineageEntryState state) {
    setupSizeRetentionLineage(resourceManager, tableName, source, destination, state, System.currentTimeMillis());
  }

  private void setupSizeRetentionLineage(PinotHelixResourceManager resourceManager, String tableName,
      String source, String destination, LineageEntryState state, long timestamp) {
    SegmentLineage lineage = new SegmentLineage(tableName);
    lineage.addLineageEntry("replacement",
        new LineageEntry(List.of(source), List.of(destination), state, timestamp));
    when(resourceManager.getPropertyStore().get(anyString(), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(lineage.toZNRecord());
  }
}
