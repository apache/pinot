/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.query.plan;

import static org.testng.Assert.assertEquals;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOnlyOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOrderByOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV1;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;


public class PlanMakerTest {

  private final String LARGE_AVRO_DATA = "data/simpleData2000001.avro";
  private final String SMALL_AVRO_DATA = "data/simpleData200001.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "TestPlanMaker");
  private static File INDEXES_DIR = new File(FileUtils.getTempDirectory() + File.separator + "TestPlanMakerList");

  private static BrokerRequest _brokerRequest;
  private static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList = new ArrayList<IndexSegment>();

  @BeforeClass
  public void setup() {
    _brokerRequest = getAggregationNoFilterBrokerRequest();
    try {
      setupSegment();
      setupSegmentList(20);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
  }

  private void setupSegment() throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(PlanMakerTest.class.getClassLoader().getResource(LARGE_AVRO_DATA));

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "dim1",
            TimeUnit.DAYS, "test");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
    final File indexSegmentDir = new File(INDEX_DIR, driver.getSegmentName());
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.mmap);
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(SMALL_AVRO_DATA));
    _indexSegmentList.clear();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, "dim1",
              TimeUnit.DAYS, "test");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      System.out.println("built at : " + segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(segmentDir, driver.getSegmentName()), ReadMode.mmap));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationNoFilter() {
    BrokerRequest brokerRequest = getAggregationNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    MAggregationOperator operator = (MAggregationOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println(resultBlock.getAggregationResult().get(0));
    System.out.println(resultBlock.getAggregationResult().get(1));
    System.out.println(resultBlock.getAggregationResult().get(2));
    System.out.println(resultBlock.getAggregationResult().get(3));
    System.out.println(resultBlock.getAggregationResult().get(4));
    System.out.println(resultBlock.getAggregationResult().get(5));
    System.out.println(resultBlock.getAggregationResult().get(6));
    Assert.assertEquals(resultBlock.getAggregationResult().get(0), 2000001L);
    Assert.assertEquals(resultBlock.getAggregationResult().get(1), 2000001000000.0);
    Assert.assertEquals(resultBlock.getAggregationResult().get(2), 2000000.0);
    Assert.assertEquals(resultBlock.getAggregationResult().get(3), 0.0);
    Assert.assertEquals(Double.parseDouble(resultBlock.getAggregationResult().get(4).toString()), 1000000.0);
    Assert.assertEquals(((IntOpenHashSet) resultBlock.getAggregationResult().get(5)).size(), 10);
    Assert.assertEquals(((IntOpenHashSet) resultBlock.getAggregationResult().get(6)).size(), 100);
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationWithFilter() {
    BrokerRequest brokerRequest = getAggregationWithFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionNoFilter() {
    BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    //USelectionOperator operator = (USelectionOperator) rootPlanNode.run();
    MSelectionOrderByOperator operator = (MSelectionOrderByOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    PriorityQueue<Serializable[]> retPriorityQueue = (PriorityQueue<Serializable[]>) resultBlock.getSelectionResult();
    while (!retPriorityQueue.isEmpty()) {
      Serializable[] row = retPriorityQueue.poll();
      System.out.println(Arrays.toString(row));
      Assert.assertEquals(row[0], 9);
      Assert.assertEquals(row[1], 99);
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionNoFilterNoOrdering() {
    BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    //USelectionOperator operator = (USelectionOperator) rootPlanNode.run();
    MSelectionOnlyOperator operator = (MSelectionOnlyOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    ArrayList<Serializable[]> retList = (ArrayList<Serializable[]>) resultBlock.getSelectionResult();
    int i = 0;
    for (Serializable[] row : retList) {
      System.out.println(Arrays.toString(row));
      Assert.assertEquals(row[0], (i % 10));
      Assert.assertEquals(row[1], i);
      Assert.assertEquals(row[2], i);
      i++;
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionWithFilter() {
    BrokerRequest brokerRequest = getSelectionWithFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByNoFilter() {
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    MAggregationGroupByOperator operator = (MAggregationGroupByOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());
    //    System.out.println(resultBlock.getAggregationGroupByResult().get("0.0"));
    List<Map<String, Serializable>> combinedGroupByResult = resultBlock.getAggregationGroupByOperatorResult();
    //    Assert.assertEquals(20000001L, resultBlock.getAggregationResult().get(0));
    //    Assert.assertEquals(200000010000000.0, resultBlock.getAggregationResult().get(1));
    //    Assert.assertEquals(20000000.0, resultBlock.getAggregationResult().get(2));
    //    Assert.assertEquals(0.0, resultBlock.getAggregationResult().get(3));
    int i = 0;
    Map<String, Serializable> singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(200001, ((Long) resultList).longValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(200000, ((Long) resultList).longValue());
      }
    }

    i = 1;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedSumValue =
            ((Double.parseDouble(keyString) + 2000000 + Double.parseDouble(keyString)) * 200001) / 2;
        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedSumValue =
            (((Double.parseDouble(keyString) + 2000000) - 10) + Double.parseDouble(keyString)) * 100000;
        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      }
    }

    i = 2;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(2000000 + Double.parseDouble(keyString), ((Double) resultList).doubleValue());

      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals((2000000 - 10) + Double.parseDouble(keyString), ((Double) resultList).doubleValue());

      }
    }

    i = 3;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      }
    }

    i = 4;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);

        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedAvgValue =
            ((Double.parseDouble(keyString) + 2000000 + Double.parseDouble(keyString)) * 200001) / 2 / 200001;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedAvgValue =
            ((((Double.parseDouble(keyString) + 2000000) - 10) + Double.parseDouble(keyString)) * 100000) / 200000;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      }
    }

    i = 5;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      System.out.println("grouped key : " + keyString + ", value : " + resultList);
      assertEquals(1, ((IntOpenHashSet) resultList).size());
    }

    i = 6;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      System.out.println("grouped key : " + keyString + ", value : " + resultList);
      assertEquals(10, ((IntOpenHashSet) resultList).size());
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByWithFilter() {
    BrokerRequest brokerRequest = getAggregationGroupByWithFilterBrokerRequest();
    brokerRequest.getGroupBy().getColumns().clear();
    brokerRequest.getGroupBy().getColumns().add("dim0");
    brokerRequest.getGroupBy().getColumns().add("dim1");
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    MAggregationGroupByOperator operator = (MAggregationGroupByOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());
    List<Map<String, Serializable>> combinedGroupByResult = resultBlock.getAggregationGroupByOperatorResult();
    for (int i = 0; i < combinedGroupByResult.size(); ++i) {
      System.out.println("function : " + brokerRequest.getAggregationsInfo().get(i));
      for (String keyString : combinedGroupByResult.get(i).keySet()) {
        System.out.println("grouped key : " + keyString + ", value : " + combinedGroupByResult.get(i).get(keyString));
      }
    }
  }

  @Test
  public void testInterSegmentAggregationPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setSelections(null);
    brokerRequest.setSelectionsIsSet(false);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
  }

  @Test
  public void testInterSegmentAggregationPlanMakerAndRun() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();
    System.out.println(instanceResponse.getLong(0, 0));
    System.out.println(instanceResponse.getDouble(0, 1));
    System.out.println(instanceResponse.getDouble(0, 2));
    System.out.println(instanceResponse.getDouble(0, 3));
    System.out.println(instanceResponse.getObject(0, 4).toString());
    System.out.println(instanceResponse.getObject(0, 5).toString());
    System.out.println(instanceResponse.getObject(0, 6).toString());
    System.out.println("Query time: " + instanceResponse.getMetadata().get("timeUsedMs"));
    Assert.assertEquals(200001L * _indexSegmentList.size(), instanceResponse.getLong(0, 0));
    Assert.assertEquals(20000100000.0 * _indexSegmentList.size(), instanceResponse.getDouble(0, 1));
    Assert.assertEquals(200000.0, instanceResponse.getDouble(0, 2));
    Assert.assertEquals(0.0, instanceResponse.getDouble(0, 3));
    Assert.assertEquals(100000.0, Double.parseDouble(instanceResponse.getObject(0, 4).toString()));
    Assert.assertEquals(10, ((IntOpenHashSet) instanceResponse.getObject(0, 5)).size());
    Assert.assertEquals(100, ((IntOpenHashSet) instanceResponse.getObject(0, 6)).size());
    DefaultReduceService reduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(brokerResponse.getAggregationResults());
  }

  @Test
  public void testInterSegmentAggregationGroupByPlanMakerAndRun() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV1();
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    System.out.println(instanceResponse);
    List<DataTable> instanceResponseList = new ArrayList<DataTable>();
    instanceResponseList.add(instanceResponse);

    List<Map<String, Serializable>> combinedGroupByResult =
        AggregationGroupByOperatorService.transformDataTableToGroupByResult(instanceResponse);
    int i = 0;
    Map<String, Serializable> singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(400020, ((Long) resultList).longValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(400000, ((Long) resultList).longValue());
      }
    }

    i = 1;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedSumValue =
            (((Double.parseDouble(keyString) + 200000 + Double.parseDouble(keyString)) * 20001) / 2) * 20;
        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedSumValue =
            (((Double.parseDouble(keyString) + 200000) - 10) + Double.parseDouble(keyString)) * 10000 * 20;

        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      }
    }

    i = 2;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(200000 + Double.parseDouble(keyString), ((Double) resultList).doubleValue());

      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals((200000 - 10) + Double.parseDouble(keyString), ((Double) resultList).doubleValue());

      }
    }

    i = 3;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      }
    }

    i = 4;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);

        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedAvgValue =
            ((((Double.parseDouble(keyString) + 200000 + Double.parseDouble(keyString)) * 20001) / 2) * 20) / 400020;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + resultList);
        double expectedAvgValue =
            ((((Double.parseDouble(keyString) + 200000) - 10) + Double.parseDouble(keyString)) * 10000 * 20) / 400000;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      }
    }

    i = 5;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      System.out.println("grouped key : " + keyString + ", value : " + resultList);
      int expectedAvgValue = 1;
      assertEquals(expectedAvgValue, ((IntOpenHashSet) resultList).size());
    }

    i = 6;
    singleGroupByResult = combinedGroupByResult.get(i);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      System.out.println("grouped key : " + keyString + ", value : " + resultList);
      int expectedAvgValue = 10;
      assertEquals(expectedAvgValue, ((IntOpenHashSet) resultList).size());
    }

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getAggregationResults()));
    System.out.println("Time used : " + brokerResponse.getTimeUsedMs());

  }

  @Test
  public void testInterSegmentSelectionPlanMaker() throws JSONException {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setAggregationsInfo(null);
    brokerRequest.setAggregationsInfoIsSet(false);
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(20);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);

    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(brokerResponse.getSelectionResults());
    System.out.println("TimeUsedMs : " + brokerResponse.getTimeUsedMs());
    System.out.println(brokerResponse);
    JSONArray selectionResultsArray = brokerResponse.getSelectionResults().getJSONArray("results");
    for (int j = 0; j < selectionResultsArray.length(); ++j) {
      System.out.println(selectionResultsArray.getJSONArray(j));
      Assert.assertEquals(selectionResultsArray.getJSONArray(j).getInt(0), 1);
      Assert.assertEquals(selectionResultsArray.getJSONArray(j).getInt(1), 91);
      Assert.assertEquals(selectionResultsArray.getJSONArray(j).getInt(2) % 100, 91);
    }

  }

  @Test
  public void testInterSegmentSelectionNoOrderingPlanMaker() throws JSONException {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setAggregationsInfo(null);
    brokerRequest.setAggregationsInfoIsSet(false);
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest.getSelections().setSelectionSortSequence(null);
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(20);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);

    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(brokerResponse.getSelectionResults());
    System.out.println("TimeUsedMs : " + brokerResponse.getTimeUsedMs());
    System.out.println(brokerResponse);

    JSONArray selectionResultsArray = brokerResponse.getSelectionResults().getJSONArray("results");
    for (int j = 0; j < selectionResultsArray.length(); ++j) {
      Assert.assertEquals(selectionResultsArray.getJSONArray(j).getInt(0), 1);
    }
  }

  private static BrokerRequest getAggregationNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountDim0AggregationInfo());
    aggregationsInfo.add(getDistinctCountDim1AggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    return brokerRequest;
  }

  private static BrokerRequest getAggregationWithFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountDim0AggregationInfo());
    aggregationsInfo.add(getDistinctCountDim1AggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest = setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest getAggregationGroupByNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountDim0AggregationInfo());
    aggregationsInfo.add(getDistinctCountDim1AggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    return brokerRequest;
  }

  private static BrokerRequest getAggregationGroupByWithFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountDim0AggregationInfo());
    aggregationsInfo.add(getDistinctCountDim1AggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    brokerRequest = setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest getSelectionNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

  private static BrokerRequest getSelectionWithFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest = setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    String filterColumn = "dim0";
    String filterVal = "1";
    if (filterColumn.contains(",")) {
      String[] filterColumns = filterColumn.split(",");
      String[] filterValues = filterVal.split(",");
      List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        FilterQueryTree d = new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }
    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
    return brokerRequest;
  }

  private static Selection getSelectionQuery() {
    Selection selection = new Selection();
    selection.setOffset(10);
    selection.setSize(10);
    List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim0");
    selectionColumns.add("dim1");
    selectionColumns.add("met");
    selection.setSelectionColumns(selectionColumns);

    List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("dim0");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);
    selectionSort = new SelectionSort();
    selectionSort.setColumn("dim1");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);

    selection.setSelectionSortSequence(selectionSortSequence);

    return selection;
  }

  private static AggregationInfo getCountAggregationInfo() {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getAvgAggregationInfo() {
    String type = "avg";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getDistinctCountDim0AggregationInfo() {
    String type = "distinctCount";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "dim0");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getDistinctCountDim1AggregationInfo() {
    String type = "distinctCount";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "dim1");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static GroupBy getGroupBy() {
    GroupBy groupBy = new GroupBy();
    List<String> columns = new ArrayList<String>();
    columns.add("dim0");
    groupBy.setColumns(columns);
    groupBy.setTopN(15);
    return groupBy;
  }

  public static void main(String[] args) throws FileNotFoundException {
    PrintWriter jsonFilePW = new PrintWriter("/tmp/simpleData200001.json");
    for (int i = 0; i < 200001; ++i) {
      String s = "{\"dim0\":" + (i % 10) + ",";
      s += "\"dim1\":" + (i % 100) + ",";
      s += "\"met\":" + i + "}";
      jsonFilePW.println(s);
    }
    jsonFilePW.close();

  }

}
