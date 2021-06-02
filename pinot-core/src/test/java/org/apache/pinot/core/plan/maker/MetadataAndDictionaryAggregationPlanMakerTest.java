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
package org.apache.pinot.core.plan.maker;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.plan.AggregationGroupByPlanNode;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.plan.DictionaryBasedAggregationPlanNode;
import org.apache.pinot.core.plan.MetadataBasedAggregationPlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MetadataAndDictionaryAggregationPlanMakerTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_201711219_20171120";
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "MetadataAndDictionaryAggregationPlanMakerTest");
  private static final InstancePlanMakerImplV2 PLAN_MAKER = new InstancePlanMakerImplV2();

  private IndexSegment _indexSegment;
  private IndexSegment _upsertIndexSegment;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch").build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGeneratorConfig.setSkipTimeValueCheck(true);
    segmentGeneratorConfig
        .setInvertedIndexCreationColumns(Arrays.asList("column6", "column7", "column11", "column17", "column18"));

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    ServerMetrics serverMetrics = Mockito.mock(ServerMetrics.class);
    _upsertIndexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    ((ImmutableSegmentImpl) _upsertIndexSegment)
        .enableUpsert(new PartitionUpsertMetadataManager("testTable_REALTIME", 0, serverMetrics),
            new ThreadSafeMutableRoaringBitmap());
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test(dataProvider = "testPlanMakerDataProvider")
  public void testPlanMaker(String query, Class<? extends PlanNode> planNodeClass,
      Class<? extends PlanNode> upsertPlanNodeClass) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
    PlanNode planNode = PLAN_MAKER.makeSegmentPlanNode(_indexSegment, queryContext);
    assertTrue(planNodeClass.isInstance(planNode));
    PlanNode upsertPlanNode = PLAN_MAKER.makeSegmentPlanNode(_upsertIndexSegment, queryContext);
    assertTrue(upsertPlanNodeClass.isInstance(upsertPlanNode));
  }

  @DataProvider(name = "testPlanMakerDataProvider")
  public Object[][] testPlanMakerDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{"select * from testTable", /*selection query*/
        SelectionPlanNode.class, SelectionPlanNode.class});
    entries.add(new Object[]{"select column1,column5 from testTable", /*selection query*/
        SelectionPlanNode.class, SelectionPlanNode.class});
    entries.add(new Object[]{"select * from testTable where daysSinceEpoch > 100", /*selection query with filters*/
        SelectionPlanNode.class, SelectionPlanNode.class});
    entries.add(new Object[]{"select count(*) from testTable", /*count(*) from metadata*/
        MetadataBasedAggregationPlanNode.class, AggregationPlanNode.class});
    entries
        .add(new Object[]{"select max(daysSinceEpoch),min(daysSinceEpoch) from testTable", /*min max from dictionary*/
            DictionaryBasedAggregationPlanNode.class, AggregationPlanNode.class});
    entries.add(new Object[]{"select minmaxrange(daysSinceEpoch) from testTable", /*min max from dictionary*/
        DictionaryBasedAggregationPlanNode.class, AggregationPlanNode.class});
    entries.add(new Object[]{"select max(column17),min(column17) from testTable", /* minmax from dictionary*/
        DictionaryBasedAggregationPlanNode.class, AggregationPlanNode.class});
    entries.add(new Object[]{"select minmaxrange(column17) from testTable", /*no minmax metadata, go to dictionary*/
        DictionaryBasedAggregationPlanNode.class, AggregationPlanNode.class});
    entries.add(new Object[]{"select sum(column1) from testTable", /*aggregation query*/
        AggregationPlanNode.class, AggregationPlanNode.class});
    entries.add(
        new Object[]{"select sum(column1) from testTable group by daysSinceEpoch", /*aggregation with group by query*/
            AggregationGroupByPlanNode.class, AggregationGroupByPlanNode.class});
    entries.add(
        new Object[]{"select count(*),min(column17) from testTable", /*multiple aggregations query, one from metadata, one from dictionary*/
            AggregationPlanNode.class, AggregationPlanNode.class});
    entries.add(
        new Object[]{"select count(*),min(daysSinceEpoch) from testTable group by daysSinceEpoch", /*multiple aggregations with group by*/
            AggregationGroupByPlanNode.class, AggregationGroupByPlanNode.class});

    return entries.toArray(new Object[entries.size()][]);
  }

  @Test(dataProvider = "isFitForPlanDataProvider")
  public void testIsFitFor(String query, IndexSegment indexSegment, boolean expectedIsFitForMetadata,
      boolean expectedIsFitForDictionary) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
    assertEquals(InstancePlanMakerImplV2.isFitForMetadataBasedPlan(queryContext), expectedIsFitForMetadata);
    assertEquals(InstancePlanMakerImplV2.isFitForDictionaryBasedPlan(queryContext, indexSegment),
        expectedIsFitForDictionary);
  }

  @DataProvider(name = "isFitForPlanDataProvider")
  public Object[][] provideDataForIsFitChecks() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(
        new Object[]{"select count(*) from testTable", _indexSegment, true, false /* count* from metadata, even if star tree present */});
    entries.add(
        new Object[]{"select min(daysSinceEpoch) from testTable", _indexSegment, false, true /* max (time column) from dictionary */});
    entries.add(
        new Object[]{"select max(daysSinceEpoch),minmaxrange(daysSinceEpoch) from testTable", _indexSegment, false, true});
    entries.add(
        new Object[]{"select count(*),max(daysSinceEpoch) from testTable", _indexSegment, false, false /* count* and max(time) from metadata*/});
    entries.add(new Object[]{"select sum(column1) from testTable", _indexSegment, false, false});
    return entries.toArray(new Object[entries.size()][]);
  }
}
