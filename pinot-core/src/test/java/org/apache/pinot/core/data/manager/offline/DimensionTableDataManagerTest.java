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
package org.apache.pinot.core.data.manager.offline;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManagerParams;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderTest;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


@SuppressWarnings("unchecked")
public class DimensionTableDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), LoaderTest.class.getName());
  private static final String TABLE_NAME = "dimBaseballTeams";
  private static final String AVRO_DATA_PATH = "data/dimBaseballTeams.avro";

  private File _indexDir;
  private IndexLoadingConfig _indexLoadingConfig;
  private SegmentMetadata _segmentMetadata;
  private SegmentZKMetadata _segmentZKMetadata;

  @BeforeClass
  public void setUp()
      throws Exception {
    // prepare segment data
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    // create segment
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, TEMP_DIR, TABLE_NAME);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    String segmentName = driver.getSegmentName();
    _indexDir = new File(TEMP_DIR, segmentName);
    _indexLoadingConfig = new IndexLoadingConfig();
    _segmentMetadata = new SegmentMetadataImpl(_indexDir);
    _segmentZKMetadata = new SegmentZKMetadata(segmentName);
    _segmentZKMetadata.setCrc(Long.parseLong(_segmentMetadata.getCrc()));
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private Schema getSchema() {
    return new Schema.SchemaBuilder().setSchemaName("dimBaseballTeams")
        .addSingleValueDimension("teamID", DataType.STRING).addSingleValueDimension("teamName", DataType.STRING)
        .setPrimaryKeyColumns(Collections.singletonList("teamID")).build();
  }

  private Schema getSchemaWithExtraColumn() {
    return new Schema.SchemaBuilder().setSchemaName("dimBaseballTeams")
        .addSingleValueDimension("teamID", DataType.STRING).addSingleValueDimension("teamName", DataType.STRING)
        .addSingleValueDimension("teamCity", DataType.STRING).setPrimaryKeyColumns(Collections.singletonList("teamID"))
        .build();
  }

  private DimensionTableDataManager makeTableDataManager(HelixManager helixManager) {
    DimensionTableDataManager tableDataManager = DimensionTableDataManager.createInstanceByTableName(TABLE_NAME);
    TableDataManagerConfig config;
    {
      config = mock(TableDataManagerConfig.class);
      when(config.getTableName()).thenReturn(TABLE_NAME);
      when(config.getDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    }
    tableDataManager.init(config, "dummyInstance", helixManager.getHelixPropertyStore(),
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), helixManager, null,
        new TableDataManagerParams(0, false, -1));
    tableDataManager.start();
    return tableDataManager;
  }

  @Test
  public void testInstantiation()
      throws Exception {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaUtils.toZNRecord(getSchema()));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    DimensionTableDataManager tableDataManager = makeTableDataManager(helixManager);
    assertEquals(tableDataManager.getTableName(), TABLE_NAME);

    // fetch the same instance via static method
    DimensionTableDataManager returnedManager = DimensionTableDataManager.getInstanceByTableName(TABLE_NAME);
    assertNotNull(returnedManager, "Manager should find instance");
    assertEquals(tableDataManager, returnedManager, "Manager should return already created instance");

    // assert that segments are released after loading data
    tableDataManager.addSegment(_indexDir, _indexLoadingConfig);
    for (SegmentDataManager segmentManager : returnedManager.acquireAllSegments()) {
      assertEquals(segmentManager.getReferenceCount() - 1, // Subtract this acquisition
          1, // Default ref count
          "Reference counts should be same before and after segment loading.");
      returnedManager.releaseSegment(segmentManager);
      returnedManager.removeSegment(segmentManager.getSegmentName());
    }

    // try fetching non-existent table
    returnedManager = DimensionTableDataManager.getInstanceByTableName("doesNotExist");
    assertNull(returnedManager, "Manager should return null for non-existent table");
  }

  @Test
  public void testLookup()
      throws Exception {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaUtils.toZNRecord(getSchema()));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    DimensionTableDataManager tableDataManager = makeTableDataManager(helixManager);

    // try fetching data BEFORE loading segment
    GenericRow resp = tableDataManager.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    assertNull(resp, "Response should be null if no segment is loaded");

    tableDataManager.addSegment(_indexDir, _indexLoadingConfig);

    // Confirm table is loaded and available for lookup
    resp = tableDataManager.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    assertNotNull(resp, "Should return response after segment load");
    assertEquals(resp.getFieldToValueMap().size(), 2);
    assertEquals(resp.getValue("teamID"), "SF");
    assertEquals(resp.getValue("teamName"), "San Francisco Giants");

    // Confirm we can get FieldSpec for loaded tables columns.
    FieldSpec spec = tableDataManager.getColumnFieldSpec("teamName");
    assertNotNull(spec, "Should return spec for existing column");
    assertEquals(spec.getDataType(), DataType.STRING, "Should return correct data type for teamName column");

    // Confirm we can read primary column list
    List<String> pkColumns = tableDataManager.getPrimaryKeyColumns();
    assertEquals(pkColumns, Collections.singletonList("teamID"), "Should return PK column list");

    // Remove the segment
    List<SegmentDataManager> segmentManagers = tableDataManager.acquireAllSegments();
    assertEquals(segmentManagers.size(), 1, "Should have exactly one segment manager");
    SegmentDataManager segMgr = segmentManagers.get(0);
    String segmentName = segMgr.getSegmentName();
    tableDataManager.removeSegment(segmentName);
    // confirm table is cleaned up
    resp = tableDataManager.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    assertNull(resp, "Response should be null if no segment is loaded");
  }

  @Test
  public void testReloadTable()
      throws Exception {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaUtils.toZNRecord(getSchema()));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    DimensionTableDataManager tableDataManager = makeTableDataManager(helixManager);

    tableDataManager.addSegment(_indexDir, _indexLoadingConfig);

    // Confirm table is loaded and available for lookup
    GenericRow resp = tableDataManager.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    assertNotNull(resp, "Should return response after segment load");
    assertEquals(resp.getFieldToValueMap().size(), 2);
    assertEquals(resp.getValue("teamID"), "SF");
    assertEquals(resp.getValue("teamName"), "San Francisco Giants");

    // Confirm the new column does not exist
    FieldSpec teamCitySpec = tableDataManager.getColumnFieldSpec("teamCity");
    assertNull(teamCitySpec, "Should not return spec for non-existing column");

    // Reload the segment with a new column
    Schema schemaWithExtraColumn = getSchemaWithExtraColumn();
    when(propertyStore.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaUtils.toZNRecord(schemaWithExtraColumn));
    tableDataManager.reloadSegment(_segmentZKMetadata.getSegmentName(), _indexLoadingConfig, _segmentZKMetadata,
        _segmentMetadata, schemaWithExtraColumn, false);

    // Confirm the new column is available for lookup
    teamCitySpec = tableDataManager.getColumnFieldSpec("teamCity");
    assertNotNull(teamCitySpec, "Should return spec for existing column");
    assertEquals(teamCitySpec.getDataType(), DataType.STRING, "Should return correct data type for teamCity column");
    resp = tableDataManager.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    assertEquals(resp.getFieldToValueMap().size(), 3);
    assertEquals(resp.getValue("teamID"), "SF");
    assertEquals(resp.getValue("teamName"), "San Francisco Giants");
    assertEquals(resp.getValue("teamCity"), "null");
  }
}
