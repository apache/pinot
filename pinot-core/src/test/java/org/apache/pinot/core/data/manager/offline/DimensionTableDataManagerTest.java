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
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderTest;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DimensionTableDataManagerTest {
  private static final String TABLE_NAME = "dimBaseballTeams";
  private static final File INDEX_DIR = new File(LoaderTest.class.getName());
  private static final String AVRO_DATA_PATH = "data/dimBaseballTeams.avro";

  private File _indexDir;
  private IndexLoadingConfig _indexLoadingConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    // prepare segment data
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    // create segment
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, INDEX_DIR, TABLE_NAME);
    segmentGeneratorConfig.setSegmentVersion(SegmentVersion.v3);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();
    _indexDir = new File(INDEX_DIR, driver.getSegmentName());

    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setReadMode(ReadMode.mmap);
    _indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private ZkHelixPropertyStore mockPropertyStore() {
    String baseballTeamsSchemaStr =
        "{\"schemaName\":\"dimBaseballTeams\",\"dimensionFieldSpecs\":[{\"name\":\"teamID\",\"dataType\":\"STRING\"},{\"name\":\"teamName\",\"dataType\":\"STRING\"}],\"primaryKeyColumns\":[\"teamID\"]}";
    ZNRecord zkSchemaRec = new ZNRecord("dimBaseballTeams");
    zkSchemaRec.setSimpleField("schemaJSON", baseballTeamsSchemaStr);

    ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).
        thenReturn(zkSchemaRec);

    return propertyStore;
  }

  private DimensionTableDataManager makeTestableManager() {
    DimensionTableDataManager tableDataManager = DimensionTableDataManager.createInstanceByTableName(TABLE_NAME);
    TableDataManagerConfig config;
    {
      config = mock(TableDataManagerConfig.class);
      when(config.getTableName()).thenReturn(TABLE_NAME);
      when(config.getDataDir()).thenReturn(INDEX_DIR.getAbsolutePath());
    }
    tableDataManager.init(config, "dummyInstance", mockPropertyStore(),
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class));
    tableDataManager.start();

    return tableDataManager;
  }

  @Test
  public void instantiationTests()
      throws Exception {
    DimensionTableDataManager mgr = makeTestableManager();
    Assert.assertEquals(mgr.getTableName(), TABLE_NAME);

    // fetch the same instance via static method
    DimensionTableDataManager returnedManager = DimensionTableDataManager.getInstanceByTableName(TABLE_NAME);
    Assert.assertNotNull(returnedManager, "Manager should find instance");
    Assert.assertEquals(mgr, returnedManager, "Manager should return already created instance");

    // assert that segments are released after loading data
    mgr.addSegment(_indexDir, _indexLoadingConfig);
    for (SegmentDataManager segmentManager : returnedManager.acquireAllSegments()) {
      Assert.assertEquals(segmentManager.getReferenceCount() - 1, // Subtract this acquisition
          1, // Default ref count
          "Reference counts should be same before and after segment loading.");
      returnedManager.releaseSegment(segmentManager);
      returnedManager.removeSegment(segmentManager.getSegmentName());
    }

    // try fetching non-existent table
    returnedManager = DimensionTableDataManager.getInstanceByTableName("doesNotExist");
    Assert.assertNull(returnedManager, "Manager should return null for non-existent table");
  }

  @Test
  public void lookupTests()
      throws Exception {
    DimensionTableDataManager mgr = makeTestableManager();

    // try fetching data BEFORE loading segment
    GenericRow resp = mgr.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    Assert.assertNull(resp, "Response should be null if no segment is loaded");

    mgr.addSegment(_indexDir, _indexLoadingConfig);

    // Confirm table is loaded and available for lookup
    resp = mgr.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    Assert.assertNotNull(resp, "Should return response after segment load");
    Assert.assertEquals(resp.getValue("teamName"), "San Francisco Giants");

    // Confirm we can get FieldSpec for loaded tables columns.
    FieldSpec spec = mgr.getColumnFieldSpec("teamName");
    Assert.assertNotNull(spec, "Should return spec for existing column");
    Assert.assertEquals(spec.getDataType(), FieldSpec.DataType.STRING,
        "Should return correct data type for teamName column");

    // Confirm we can read primary column list
    List<String> pkColumns = mgr.getPrimaryKeyColumns();
    Assert.assertEquals(pkColumns, Arrays.asList("teamID"), "Should return PK column list");

    // Remove the segment
    List<SegmentDataManager> segmentManagers = mgr.acquireAllSegments();
    Assert.assertEquals(segmentManagers.size(), 1, "Should have exactly one segment manager");
    SegmentDataManager segMgr = segmentManagers.get(0);
    String segmentName = segMgr.getSegmentName();
    mgr.removeSegment(segmentName);
    // confirm table is cleaned up
    resp = mgr.lookupRowByPrimaryKey(new PrimaryKey(new String[]{"SF"}));
    Assert.assertNull(resp, "Response should be null if no segment is loaded");
  }
}
