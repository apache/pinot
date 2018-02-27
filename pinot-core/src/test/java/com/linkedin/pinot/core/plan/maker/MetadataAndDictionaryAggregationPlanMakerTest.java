/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.plan.maker;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.plan.AggregationGroupByPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.DictionaryBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.MetadataBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;


public class MetadataAndDictionaryAggregationPlanMakerTest {

  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_201711219_20171120";
  private static final String SEGMENT_NAME_STARTREE = "testTableStarTree_201711219_20171120";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PlanNodeFactoryTest");
  private static final File INDEX_DIR_STARTREE = new File(FileUtils.getTempDirectory(), "StarTreeSegmentPlanNodeFactoryTest");

  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final InstancePlanMakerImplV2 PLAN_MAKER = new InstancePlanMakerImplV2();
  private IndexSegment _indexSegment = null;
  private IndexSegment _starTreeIndexSegment = null;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    FileUtils.deleteQuietly(INDEX_DIR_STARTREE);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT)
        .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING)
        .addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime("daysSinceEpoch", 1, TimeUnit.DAYS, DataType.INT)
        .build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setInvertedIndexCreationColumns(
        Arrays.asList("column6", "column7", "column11", "column17", "column18"));

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();



    // Star Tree segment
    // Build the segment schema.
    schema = new Schema.SchemaBuilder().setSchemaName("testTableStarTree")
        .addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT)
        .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING)
        .addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime("daysSinceEpoch", 1, TimeUnit.DAYS, DataType.INT)
        .build();

    // Create the segment generator config.
    segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTableStarTree");
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME_STARTREE);
    segmentGeneratorConfig.setOutDir(INDEX_DIR_STARTREE.getAbsolutePath());
    segmentGeneratorConfig.enableStarTreeIndex(new StarTreeIndexSpec());

    // Build the index segment.
    driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment() throws Exception {
    _indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _starTreeIndexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR_STARTREE, SEGMENT_NAME_STARTREE), ReadMode.heap);
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
    _starTreeIndexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
    FileUtils.deleteQuietly(INDEX_DIR_STARTREE);
  }

  @Test(dataProvider = "testPlanNodeMakerDataProvider")
  public void testInstancePlanMakerForMetadataAndDictionaryPlan(String query, Class<? extends PlanNode> planNodeClass,
      Class<? extends PlanNode> starTreePlanNodeClass) {

    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);
    PlanNode plan = PLAN_MAKER.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    Assert.assertTrue(planNodeClass.isInstance(plan));

   plan = PLAN_MAKER.makeInnerSegmentPlan(_starTreeIndexSegment, brokerRequest);
    Assert.assertTrue(starTreePlanNodeClass.isInstance(plan));
  }


  @DataProvider(name = "testPlanNodeMakerDataProvider")
  public Object[][] provideTestPlanNodeMakerData() {

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "select * from testTable", /*selection query*/
        SelectionPlanNode.class,
        SelectionPlanNode.class
    });
    entries.add(new Object[] {
        "select column1,column5 from testTable", /*selection query*/
        SelectionPlanNode.class,
        SelectionPlanNode.class
    });
    entries.add(new Object[] {
        "select * from testTable where daysSinceEpoch > 100", /*selection query with filters*/
        SelectionPlanNode.class,
        SelectionPlanNode.class
    });
    entries.add(new Object[] {
        "select count(*) from testTable", /*count(*) from metadata*/
        MetadataBasedAggregationPlanNode.class,
        MetadataBasedAggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select max(daysSinceEpoch),min(daysSinceEpoch) from testTable", /*min max from dictionary*/
        DictionaryBasedAggregationPlanNode.class,
        AggregationPlanNode.class /* in case of star tree, we don't go to metadata/dictionary */
    });
    entries.add(new Object[] {
        "select minmaxrange(daysSinceEpoch) from testTable", /*min max from dictionary*/
        DictionaryBasedAggregationPlanNode.class,
        AggregationPlanNode.class /* in case of star tree, we don't go to metadata/dictionary */
    });
    entries.add(new Object[] {
        "select max(column17),min(column17) from testTable", /* minmax from dictionary*/
        DictionaryBasedAggregationPlanNode.class,
        AggregationPlanNode.class /* in case of star tree, we don't go to dictionary */
    });
    entries.add(new Object[] {
        "select minmaxrange(column17) from testTable", /*no minmax metadata, go to dictionary*/
        DictionaryBasedAggregationPlanNode.class,
        AggregationPlanNode.class /* in case of star tree, we don't go to dictionary */
    });
    entries.add(new Object[] {
        "select sum(column1) from testTable", /*aggregation query*/
        AggregationPlanNode.class,
        AggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select sum(column1) from testTable group by daysSinceEpoch", /*aggregation with group by query*/
        AggregationGroupByPlanNode.class,
        AggregationGroupByPlanNode.class
    });

    entries.add(new Object[] {
        "select count(*),min(column17) from testTable", /*multiple aggregations query, one from metadata, one from dictionary*/
        AggregationPlanNode.class,
        AggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select count(*),min(daysSinceEpoch) from testTable group by daysSinceEpoch", /*multiple aggregations with group by*/
        AggregationGroupByPlanNode.class,
        AggregationGroupByPlanNode.class
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  @Test(dataProvider = "isFitForPlanDataProvider")
  public void testIsFitFor(String query, IndexSegment indexSegment,
      boolean expectedIsFitForMetadata, boolean expectedIsFitForDictionary) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);

    boolean isFitForMetadataBasedPlan = PLAN_MAKER.isFitForMetadataBasedPlan(brokerRequest, indexSegment);
    boolean isFitForDictionaryBasedPlan = PLAN_MAKER.isFitForDictionaryBasedPlan(brokerRequest, indexSegment);
    Assert.assertEquals(isFitForMetadataBasedPlan, expectedIsFitForMetadata);
    Assert.assertEquals(isFitForDictionaryBasedPlan, expectedIsFitForDictionary);

  }

  @DataProvider(name = "isFitForPlanDataProvider")
  public Object[][] provideDataForIsFitChecks() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "select * from testTable", _indexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTable", _indexSegment, true, false /* count* from metadata, even if star tree present */
    });
    entries.add(new Object[] {
        "select min(daysSinceEpoch) from testTable", _indexSegment, false, true /* max (time column) from dictionary */
    });
    entries.add(new Object[] {
        "select max(daysSinceEpoch),minmaxrange(daysSinceEpoch) from testTable", _indexSegment, false, true
    });
    entries.add(new Object[] {
        "select count(*),max(daysSinceEpoch) from testTable", _indexSegment, false, false /* count* and max(time) from metadata*/
    });
    entries.add(new Object[] {
        "select sum(column1) from testTable", _indexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTable group by daysSinceEpoch", _indexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTable where daysSinceEpoch > 1", _indexSegment, false, false
    });
    entries.add(new Object[] {
        "select max(column5) from testTable where daysSinceEpoch > 100", _indexSegment, false, false
    });
    entries.add(new Object[] {
        "select column1 from testTable", _indexSegment, false, false
    });

    entries.add(new Object[] {
        "select * from testTableStarTree", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTableStarTree", _starTreeIndexSegment, true, false /* count* from metadata, even if star tree present */
    });
    entries.add(new Object[] {
        "select min(daysSinceEpoch) from testTableStarTree", _starTreeIndexSegment, false, false /* skip in case of star tree */
    });
    entries.add(new Object[] {
        "select max(daysSinceEpoch),minmaxrange(daysSinceEpoch) from testTableStarTree", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*),max(daysSinceEpoch) from testTableStarTree", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select sum(column1) from testTableStarTree", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTableStarTree group by daysSinceEpoch", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTableStarTree where daysSinceEpoch > 1", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select max(column5) from testTableStarTree where daysSinceEpoch > 100", _starTreeIndexSegment, false, false
    });
    entries.add(new Object[] {
        "select column1 from testTableStarTree", _starTreeIndexSegment, false, false
    });

    return entries.toArray(new Object[entries.size()][]);
  }

}
