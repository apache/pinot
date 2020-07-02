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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.plan.AggregationGroupByPlanNode;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.plan.DictionaryBasedAggregationPlanNode;
import org.apache.pinot.core.plan.MetadataBasedAggregationPlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MetadataAndDictionaryAggregationPlanMakerTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_201711219_20171120";
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "MetadataAndDictionaryAggregationPlanMakerTest");

  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final InstancePlanMakerImplV2 PLAN_MAKER = new InstancePlanMakerImplV2();
  private IndexSegment _indexSegment = null;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resource);
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
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test(dataProvider = "testPlanNodeMakerDataProvider")
  public void testInstancePlanMakerForMetadataAndDictionaryPlan(String query, Class<? extends PlanNode> planNodeClass) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);
    PlanNode plan = PLAN_MAKER.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    Assert.assertTrue(planNodeClass.isInstance(plan));
  }

  @DataProvider(name = "testPlanNodeMakerDataProvider")
  public Object[][] provideTestPlanNodeMakerData() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{"select * from testTable", /*selection query*/
        SelectionPlanNode.class});
    entries.add(new Object[]{"select column1,column5 from testTable", /*selection query*/
        SelectionPlanNode.class});
    entries.add(new Object[]{"select * from testTable where daysSinceEpoch > 100", /*selection query with filters*/
        SelectionPlanNode.class});
    entries.add(new Object[]{"select count(*) from testTable", /*count(*) from metadata*/
        MetadataBasedAggregationPlanNode.class});
    entries
        .add(new Object[]{"select max(daysSinceEpoch),min(daysSinceEpoch) from testTable", /*min max from dictionary*/
            DictionaryBasedAggregationPlanNode.class});
    entries.add(new Object[]{"select minmaxrange(daysSinceEpoch) from testTable", /*min max from dictionary*/
        DictionaryBasedAggregationPlanNode.class});
    entries.add(new Object[]{"select max(column17),min(column17) from testTable", /* minmax from dictionary*/
        DictionaryBasedAggregationPlanNode.class});
    entries.add(new Object[]{"select minmaxrange(column17) from testTable", /*no minmax metadata, go to dictionary*/
        DictionaryBasedAggregationPlanNode.class});
    entries.add(new Object[]{"select sum(column1) from testTable", /*aggregation query*/
        AggregationPlanNode.class});
    entries.add(
        new Object[]{"select sum(column1) from testTable group by daysSinceEpoch", /*aggregation with group by query*/
            AggregationGroupByPlanNode.class});
    entries.add(
        new Object[]{"select count(*),min(column17) from testTable", /*multiple aggregations query, one from metadata, one from dictionary*/
            AggregationPlanNode.class});
    entries.add(
        new Object[]{"select count(*),min(daysSinceEpoch) from testTable group by daysSinceEpoch", /*multiple aggregations with group by*/
            AggregationGroupByPlanNode.class});

    return entries.toArray(new Object[entries.size()][]);
  }

  @Test(dataProvider = "isFitForPlanDataProvider")
  public void testIsFitFor(String query, IndexSegment indexSegment, boolean expectedIsFitForMetadata,
      boolean expectedIsFitForDictionary) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);

    boolean isFitForMetadataBasedPlan = InstancePlanMakerImplV2.isFitForMetadataBasedPlan(brokerRequest, indexSegment);
    boolean isFitForDictionaryBasedPlan =
        InstancePlanMakerImplV2.isFitForDictionaryBasedPlan(brokerRequest, indexSegment);
    Assert.assertEquals(isFitForMetadataBasedPlan, expectedIsFitForMetadata);
    Assert.assertEquals(isFitForDictionaryBasedPlan, expectedIsFitForDictionary);
  }

  @DataProvider(name = "isFitForPlanDataProvider")
  public Object[][] provideDataForIsFitChecks() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{"select * from testTable", _indexSegment, false, false});
    entries.add(
        new Object[]{"select count(*) from testTable", _indexSegment, true, false /* count* from metadata, even if star tree present */});
    entries.add(
        new Object[]{"select min(daysSinceEpoch) from testTable", _indexSegment, false, true /* max (time column) from dictionary */});
    entries.add(
        new Object[]{"select max(daysSinceEpoch),minmaxrange(daysSinceEpoch) from testTable", _indexSegment, false, true});
    entries.add(
        new Object[]{"select count(*),max(daysSinceEpoch) from testTable", _indexSegment, false, false /* count* and max(time) from metadata*/});
    entries.add(new Object[]{"select sum(column1) from testTable", _indexSegment, false, false});
    entries.add(new Object[]{"select count(*) from testTable group by daysSinceEpoch", _indexSegment, false, false});
    entries.add(new Object[]{"select count(*) from testTable where daysSinceEpoch > 1", _indexSegment, false, false});
    entries.add(
        new Object[]{"select max(column5) from testTable where daysSinceEpoch > 100", _indexSegment, false, false});
    entries.add(new Object[]{"select column1 from testTable", _indexSegment, false, false});
    return entries.toArray(new Object[entries.size()][]);
  }
}
