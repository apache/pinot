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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.HLCSegmentName;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.core.data.manager.TableDataManagerTestUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManagerParams;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class RealtimeTableDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "RealtimeTableDataManagerTest");
  private static final String TABLE_NAME = "table01";
  private static final String TABLE_NAME_WITH_TYPE = "table01_REALTIME";
  private static final File TABLE_DATA_DIR = new File(TEMP_DIR, TABLE_NAME_WITH_TYPE);
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES = {"A", "D", "E", "B", "C"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES = {10000L, 20000L, 50000L, 40000L, 30000L};

  @BeforeMethod
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    TableDataManagerTestUtils.initSegmentFetcher();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testAddSegmentUseBackupCopy()
      throws Exception {
    RealtimeTableDataManager tmgr = new RealtimeTableDataManager(null);
    TableDataManagerConfig tableDataManagerConfig = createTableDataManagerConfig();
    ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
    TableConfig tableConfig = setupTableConfig(propertyStore);
    Schema schema = setupSchema(propertyStore);
    tmgr.init(tableDataManagerConfig, "server01", propertyStore,
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null, null,
        new TableDataManagerParams(0, false, -1));

    // Create a dummy local segment.
    String segName = "seg01";
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segName);
    segmentZKMetadata.setStatus(Status.DONE);
    File localSegDir = createSegment(tableConfig, schema, segName);
    long segCrc = TableDataManagerTestUtils.getCRC(localSegDir, SegmentVersion.v3);
    segmentZKMetadata.setCrc(segCrc);

    // Move the segment to the backup location.
    File backup = new File(TABLE_DATA_DIR, segName + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    localSegDir.renameTo(backup);
    assertEquals(localSegDir, new File(TABLE_DATA_DIR, segName));
    assertFalse(localSegDir.exists());
    IndexLoadingConfig indexLoadingConfig =
        TableDataManagerTestUtils.createIndexLoadingConfig("default", tableConfig, schema);
    tmgr.addSegment(segName, indexLoadingConfig, segmentZKMetadata);
    // Segment data is put back the default location, and backup location is deleted.
    assertTrue(localSegDir.exists());
    assertFalse(backup.exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(new File(TABLE_DATA_DIR, segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentNoBackupCopy()
      throws Exception {
    RealtimeTableDataManager tmgr = new RealtimeTableDataManager(null);
    TableDataManagerConfig tableDataManagerConfig = createTableDataManagerConfig();
    ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
    TableConfig tableConfig = setupTableConfig(propertyStore);
    Schema schema = setupSchema(propertyStore);
    tmgr.init(tableDataManagerConfig, "server01", propertyStore,
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null, null,
        new TableDataManagerParams(0, false, -1));

    // Create a raw segment and put it in deep store backed by local fs.
    String segName = "seg01";
    SegmentZKMetadata segmentZKMetadata =
        TableDataManagerTestUtils.makeRawSegment(segName, createSegment(tableConfig, schema, segName),
            new File(TEMP_DIR, segName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION), true);
    segmentZKMetadata.setStatus(Status.DONE);

    // Local segment dir doesn't exist, thus downloading from deep store.
    File localSegDir = new File(TABLE_DATA_DIR, segName);
    assertFalse(localSegDir.exists());
    IndexLoadingConfig indexLoadingConfig =
        TableDataManagerTestUtils.createIndexLoadingConfig("default", tableConfig, schema);
    tmgr.addSegment(segName, indexLoadingConfig, segmentZKMetadata);
    // Segment data is put on default location.
    assertTrue(localSegDir.exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(new File(TABLE_DATA_DIR, segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentDefaultTierByTierBasedDirLoader()
      throws Exception {
    RealtimeTableDataManager tmgr1 = new RealtimeTableDataManager(null);
    TableDataManagerConfig tableDataManagerConfig = createTableDataManagerConfig();
    ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
    TableConfig tableConfig = setupTableConfig(propertyStore);
    Schema schema = setupSchema(propertyStore);
    tmgr1.init(tableDataManagerConfig, "server01", propertyStore,
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null, null,
        new TableDataManagerParams(0, false, -1));

    // Create a raw segment and put it in deep store backed by local fs.
    String segName = "seg_tiered_01";
    SegmentZKMetadata segmentZKMetadata =
        TableDataManagerTestUtils.makeRawSegment(segName, createSegment(tableConfig, schema, segName),
            new File(TEMP_DIR, segName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION), true);
    segmentZKMetadata.setStatus(Status.DONE);

    // Local segment dir doesn't exist, thus downloading from deep store.
    File localSegDir = new File(TABLE_DATA_DIR, segName);
    assertFalse(localSegDir.exists());

    // Add segment
    IndexLoadingConfig indexLoadingConfig =
        TableDataManagerTestUtils.createIndexLoadingConfig("tierBased", tableConfig, schema);
    tmgr1.addSegment(segName, indexLoadingConfig, segmentZKMetadata);
    assertTrue(localSegDir.exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(new File(TABLE_DATA_DIR, segName));
    assertEquals(llmd.getTotalDocs(), 5);

    // Now, repeat initialization of the table data manager
    tmgr1.shutDown();
    RealtimeTableDataManager tmgr2 = new RealtimeTableDataManager(null);
    tableDataManagerConfig = createTableDataManagerConfig();
    propertyStore = mock(ZkHelixPropertyStore.class);
    tableConfig = setupTableConfig(propertyStore);
    schema = setupSchema(propertyStore);
    tmgr2.init(tableDataManagerConfig, "server01", propertyStore,
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null, null,
        new TableDataManagerParams(0, false, -1));

    // Reinitialize index loading config and try adding the segment
    indexLoadingConfig =
        TableDataManagerTestUtils.createIndexLoadingConfig("tierBased", tableConfig, schema);
    tmgr2.addSegment(segName, indexLoadingConfig, segmentZKMetadata);

    // Make sure that the segment hasn't been moved
    assertTrue(localSegDir.exists());
    llmd = new SegmentMetadataImpl(new File(TABLE_DATA_DIR, segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }


  @Test
  public void testAllowDownload() {
    RealtimeTableDataManager mgr = new RealtimeTableDataManager(null);

    String groupId = "myTable_REALTIME_1234567_0";
    String partitionRange = "ALL";
    String sequenceNumber = "1234567";
    HLCSegmentName hlc = new HLCSegmentName(groupId, partitionRange, sequenceNumber);
    assertFalse(mgr.allowDownload(hlc.getSegmentName(), null));

    LLCSegmentName llc = new LLCSegmentName("tbl01", 0, 1000000, System.currentTimeMillis());
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getStatus()).thenReturn(Status.IN_PROGRESS);
    assertFalse(mgr.allowDownload(llc.getSegmentName(), zkmd));

    when(zkmd.getStatus()).thenReturn(Status.DONE);
    when(zkmd.getDownloadUrl()).thenReturn("");
    assertFalse(mgr.allowDownload(llc.getSegmentName(), zkmd));

    when(zkmd.getDownloadUrl()).thenReturn("remote");
    assertTrue(mgr.allowDownload(llc.getSegmentName(), zkmd));
  }

  private static File createSegment(TableConfig tableConfig, Schema schema, String segName)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TABLE_DATA_DIR.getAbsolutePath());
    config.setSegmentName(segName);
    config.setSegmentVersion(SegmentVersion.v3);
    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < STRING_VALUES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, STRING_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TABLE_DATA_DIR, segName);
  }

  private static TableDataManagerConfig createTableDataManagerConfig() {
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableName()).thenReturn(TABLE_NAME_WITH_TYPE);
    when(tableDataManagerConfig.getDataDir()).thenReturn(TABLE_DATA_DIR.getAbsolutePath());
    return tableDataManagerConfig;
  }

  private static TableConfig setupTableConfig(ZkHelixPropertyStore propertyStore)
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setSchemaName(TABLE_NAME).build();
    ZNRecord tableConfigZNRecord = TableConfigUtils.toZNRecord(tableConfig);
    when(propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForResourceConfig(TABLE_NAME_WITH_TYPE), null,
        AccessOption.PERSISTENT)).thenReturn(tableConfigZNRecord);
    return tableConfig;
  }

  private static Schema setupSchema(ZkHelixPropertyStore propertyStore) {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(LONG_COLUMN, FieldSpec.DataType.LONG).build();
    ZNRecord schemaZNRecord = SchemaUtils.toZNRecord(schema);
    when(propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSchema(TABLE_NAME), null,
        AccessOption.PERSISTENT)).thenReturn(schemaZNRecord);
    return schema;
  }

  @Test
  public void testSetDefaultTimeValueIfInvalid() {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    long currentTimeMs = System.currentTimeMillis();
    when(segmentZKMetadata.getCreationTime()).thenReturn(currentTimeMs);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("timeColumn").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("timeColumn", FieldSpec.DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
    RealtimeTableDataManager.setDefaultTimeValueIfInvalid(tableConfig, schema, segmentZKMetadata);
    DateTimeFieldSpec timeFieldSpec = schema.getSpecForTimeColumn("timeColumn");
    assertNotNull(timeFieldSpec);
    assertEquals(timeFieldSpec.getDefaultNullValue(), currentTimeMs);

    schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("timeColumn", FieldSpec.DataType.INT, "SIMPLE_DATE_FORMAT|yyyyMMdd", "1:DAYS").build();
    RealtimeTableDataManager.setDefaultTimeValueIfInvalid(tableConfig, schema, segmentZKMetadata);
    timeFieldSpec = schema.getSpecForTimeColumn("timeColumn");
    assertNotNull(timeFieldSpec);
    assertEquals(timeFieldSpec.getDefaultNullValue(),
        Integer.parseInt(DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC).print(currentTimeMs)));
  }
}
