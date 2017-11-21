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

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DateTimeFieldSpec.DateTimeType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.FieldSpec;
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


public class InstancePlanMakerTest {

  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_201711219_20171120";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MakeInnerSegmentPlanTest");

  private static final InstancePlanMakerImplV2 PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
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
        .addDateTime("daysSinceEpoch", FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS", DateTimeType.PRIMARY)
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
  }

  @BeforeClass
  public void loadSegment() throws Exception {
    _indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test(dataProvider = "makeInnerSegmentPlanDataProvider")
  public void testMakeInnerSegmentPlan(String query, Class<? extends PlanNode> planNodeClass) {

    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);
    PlanNode plan = PLAN_MAKER.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    Assert.assertTrue(planNodeClass.isInstance(plan));
  }

  @DataProvider(name = "makeInnerSegmentPlanDataProvider")
  public Object[][] provideMakeInnerSegmentPlanData() {

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "select * from testTable", /*selection query*/
        SelectionPlanNode.class
    });
    entries.add(new Object[] {
        "select column1,column5 from testTable", /*selection query*/
        SelectionPlanNode.class
    });
    entries.add(new Object[] {
        "select * from testTable where daysSinceEpoch > 100", /*selection query with filters*/
        SelectionPlanNode.class
    });
    entries.add(new Object[] {
        "select count(*) from testTable", /*count(*) query*/
        MetadataBasedAggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select max(daysSinceEpoch),min(daysSinceEpoch) from testTable", /*min,max query*/
        DictionaryBasedAggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select minmaxrange(daysSinceEpoch) from testTable", /*minmaxrange query*/
        DictionaryBasedAggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select sum(column1) from testTable", /*aggregation query*/
        AggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select sum(column1) from testTable group by daysSinceEpoch", /*aggregation with group by query*/
        AggregationGroupByPlanNode.class
    });
    entries.add(new Object[] {
        "select count(*),min(daysSinceEpoch) from testTable", /*multiple aggregations query*/
        AggregationPlanNode.class
    });
    entries.add(new Object[] {
        "select count(*),min(column1) from testTable group by daysSinceEpoch", /*multiple aggregations with group by*/
        AggregationGroupByPlanNode.class
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  @Test(dataProvider = "isFitForPlanDataProvider")
  public void testIsFitForMetadataBased(String query, boolean expectedIsFitForMetadata, boolean expectedIsFitForDictionary) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);
    boolean isFitForMetadataBasedPlan = PLAN_MAKER.isFitForMetadataBasedPlan(brokerRequest);
    boolean isFitForDictionaryBasedPlan = PLAN_MAKER.isFitForDictionaryBasedPlan(brokerRequest, _indexSegment);
    Assert.assertEquals(isFitForMetadataBasedPlan, expectedIsFitForMetadata);
    Assert.assertEquals(isFitForDictionaryBasedPlan, expectedIsFitForDictionary);
  }

  @DataProvider(name = "isFitForPlanDataProvider")
  public Object[][] provideDataForIsFitChecks() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "select * from testTable", false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTable", true, false
    });
    entries.add(new Object[] {
        "select min(daysSinceEpoch) from testTable", false, true
    });
    entries.add(new Object[] {
        "select max(daysSinceEpoch),minmaxrange(daysSinceEpoch) from testTable", false, true
    });
    entries.add(new Object[] {
        "select count(*),max(daysSinceEpoch) from testTable", false, false
    });
    entries.add(new Object[] {
        "select sum(column1) from testTable", false, false
    });
    entries.add(new Object[] {
        "select count(*) from testTable group by daysSinceEpoch", false, false
    });
    entries.add(new Object[] {
        "select max(column5) from testTable where daysSinceEpoch > 100", false, false
    });
    entries.add(new Object[] {
        "select column1 from testTable", false, false
    });

    return entries.toArray(new Object[entries.size()][]);
  }

}
