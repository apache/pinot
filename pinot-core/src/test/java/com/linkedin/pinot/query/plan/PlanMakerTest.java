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
package com.linkedin.pinot.query.plan;

import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import java.io.File;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.query.MSelectionOnlyOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import static org.testng.Assert.assertEquals;


public class PlanMakerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlanMakerTest.class);

  private static final String LARGE_AVRO_DATA = "data/simpleData2000001.avro";
  private static final String SMALL_AVRO_DATA = "data/simpleData200001.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "TestPlanMaker");
  private static File INDEXES_DIR = new File(FileUtils.getTempDirectory() + File.separator + "TestPlanMakerList");

  private static final int COUNT_AGGREGATION_INDEX = 0;
  private static final int SUM_AGGREGATION_INDEX = 1;
  private static final int MAX_AGGREGATION_INDEX = 2;
  private static final int MIN_AGGREGATION_INDEX = 3;
  private static final int AVG_AGGREGATION_INDEX = 4;
  private static final int DISTINCT_DIM0_AGGREGATION_INDEX = 5;
  private static final int DISTINCT_DIM1_AGGREGATION_INDEX = 6;

  private BrokerRequest _brokerRequest;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegmentList = new ArrayList<>();

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
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    for (IndexSegment segment : _indexSegmentList) {
      segment.destroy();
    }
    _indexSegmentList.clear();
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

    LOGGER.debug("built at: {}", INDEX_DIR.getAbsolutePath());
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

      LOGGER.debug("built at: {}", segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(segmentDir, driver.getSegmentName()), ReadMode.mmap));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationNoFilter() {
    BrokerRequest brokerRequest = getAggregationNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    for (int i = 0; i < 7; i++) {
      LOGGER.debug(resultBlock.getAggregationResult().get(i).toString());
    }
    assertEquals(((Number) resultBlock.getAggregationResult().get(0)).longValue(), 2000001L);
    assertEquals(resultBlock.getAggregationResult().get(1), 2000001000000.0);
    assertEquals(resultBlock.getAggregationResult().get(2), 2000000.0);
    assertEquals(resultBlock.getAggregationResult().get(3), 0.0);
    assertEquals(Double.parseDouble(resultBlock.getAggregationResult().get(4).toString()), 1000000.0);
    assertEquals(((IntOpenHashSet) resultBlock.getAggregationResult().get(5)).size(), 10);
    assertEquals(((IntOpenHashSet) resultBlock.getAggregationResult().get(6)).size(), 100);
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
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    PriorityQueue<Serializable[]> retPriorityQueue = (PriorityQueue<Serializable[]>) resultBlock.getSelectionResult();
    while (!retPriorityQueue.isEmpty()) {
      Serializable[] row = retPriorityQueue.poll();
      LOGGER.debug(Arrays.toString(row));
      assertEquals(row[0], 9);
      assertEquals(row[1], 99);
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
      LOGGER.debug(Arrays.toString(row));
      assertEquals(row[0], (i % 10));
      assertEquals(row[1], i);
      assertEquals(row[2], i);
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
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    LOGGER.debug("RunningTime: {}", resultBlock.getTimeUsedMs());
    LOGGER.debug("NumDocsScanned: {}", resultBlock.getNumDocsScanned());
    LOGGER.debug("TotalDocs: {}", resultBlock.getTotalRawDocs());
    List<Map<String, Serializable>> combinedGroupByResult = resultBlock.getAggregationGroupByOperatorResult();

    Map<String, Serializable> singleGroupByResult = combinedGroupByResult.get(COUNT_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(200001, ((Number) resultList).longValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(200000, ((Number) resultList).longValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(SUM_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedSumValue =
            ((Double.parseDouble(keyString) + 2000000 + Double.parseDouble(keyString)) * 200001) / 2;
        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedSumValue =
            (((Double.parseDouble(keyString) + 2000000) - 10) + Double.parseDouble(keyString)) * 100000;
        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(MAX_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(2000000 + Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals((2000000 - 10) + Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(MIN_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(AVG_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);

        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedAvgValue =
            ((Double.parseDouble(keyString) + 2000000 + Double.parseDouble(keyString)) * 200001) / 2 / 200001;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedAvgValue =
            ((((Double.parseDouble(keyString) + 2000000) - 10) + Double.parseDouble(keyString)) * 100000) / 200000;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      }
    }

    singleGroupByResult = combinedGroupByResult.get(DISTINCT_DIM0_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
      assertEquals(1, ((IntOpenHashSet) resultList).size());
    }

    singleGroupByResult = combinedGroupByResult.get(DISTINCT_DIM1_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
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
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    LOGGER.debug("RunningTime: {}", resultBlock.getTimeUsedMs());
    LOGGER.debug("NumDocsScanned: {}", resultBlock.getNumDocsScanned());
    LOGGER.debug("TotalDocs: {}", resultBlock.getTotalRawDocs());
    List<Map<String, Serializable>> combinedGroupByResult = resultBlock.getAggregationGroupByOperatorResult();
    for (int i = 0; i < combinedGroupByResult.size(); ++i) {
      LOGGER.debug("function: {}", brokerRequest.getAggregationsInfo().get(i));
      for (String keyString : combinedGroupByResult.get(i).keySet()) {
        LOGGER.debug("grouped key: {}, value: {}", keyString, combinedGroupByResult.get(i).get(keyString));
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
    Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
  }

  private List<SegmentDataManager> makeSegMgrList(List<IndexSegment> indexSegmentList) {
    List<SegmentDataManager> segMgrList = new ArrayList<>(indexSegmentList.size());
    for (IndexSegment segment : indexSegmentList) {
      segMgrList.add(new OfflineSegmentDataManager(segment));
    }
    return segMgrList;
  }

  @Test
  public void testInterSegmentAggregationPlanMakerAndRun() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();
    LOGGER.debug(Long.toString(instanceResponse.getLong(0, 0)));
    LOGGER.debug(Double.toString(instanceResponse.getDouble(0, 1)));
    LOGGER.debug(Double.toString(instanceResponse.getDouble(0, 2)));
    LOGGER.debug(Double.toString(instanceResponse.getDouble(0, 3)));
    LOGGER.debug(instanceResponse.getObject(0, 4).toString());
    LOGGER.debug(instanceResponse.getObject(0, 5).toString());
    LOGGER.debug(instanceResponse.getObject(0, 6).toString());
    LOGGER.debug("Query time: {}", instanceResponse.getMetadata().get("timeUsedMs"));
    assertEquals(200001L * _indexSegmentList.size(), instanceResponse.getLong(0, 0));
    assertEquals(20000100000.0 * _indexSegmentList.size(), instanceResponse.getDouble(0, 1));
    assertEquals(200000.0, instanceResponse.getDouble(0, 2));
    assertEquals(0.0, instanceResponse.getDouble(0, 3));
    assertEquals(100000.0, Double.parseDouble(instanceResponse.getObject(0, 4).toString()));
    assertEquals(10, ((IntOpenHashSet) instanceResponse.getObject(0, 5)).size());
    assertEquals(100, ((IntOpenHashSet) instanceResponse.getObject(0, 6)).size());
    BrokerReduceService reduceService = new BrokerReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);
    BrokerResponseNative brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.debug(brokerResponse.getAggregationResults().toString());
  }

  @Test
  public void testInterSegmentAggregationGroupByPlanMakerAndRun() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    LOGGER.debug(instanceResponse.toString());
    List<DataTable> instanceResponseList = new ArrayList<DataTable>();
    instanceResponseList.add(instanceResponse);

    List<Map<String, Serializable>> combinedGroupByResult =
        AggregationGroupByOperatorService.transformDataTableToGroupByResult(instanceResponse);

    Map<String, Serializable> singleGroupByResult = combinedGroupByResult.get(COUNT_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(400020, ((Number) resultList).longValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(400000, ((Number) resultList).longValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(SUM_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedSumValue =
            (((Double.parseDouble(keyString) + 200000 + Double.parseDouble(keyString)) * 20001) / 2) * 20;
        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedSumValue =
            (((Double.parseDouble(keyString) + 200000) - 10) + Double.parseDouble(keyString)) * 10000 * 20;

        assertEquals(expectedSumValue, ((Double) resultList).doubleValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(MAX_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(200000 + Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals((200000 - 10) + Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(MIN_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        assertEquals(Double.parseDouble(keyString), ((Double) resultList).doubleValue());
      }
    }

    singleGroupByResult = combinedGroupByResult.get(AVG_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      if (keyString.equals("0")) {
        Serializable resultList = singleGroupByResult.get(keyString);

        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedAvgValue =
            ((((Double.parseDouble(keyString) + 200000 + Double.parseDouble(keyString)) * 20001) / 2) * 20) / 400020;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      } else {
        Serializable resultList = singleGroupByResult.get(keyString);
        LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
        double expectedAvgValue =
            ((((Double.parseDouble(keyString) + 200000) - 10) + Double.parseDouble(keyString)) * 10000 * 20) / 400000;
        assertEquals(expectedAvgValue, Double.parseDouble((resultList.toString())));
      }
    }

    singleGroupByResult = combinedGroupByResult.get(DISTINCT_DIM0_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
      int expectedAvgValue = 1;
      assertEquals(expectedAvgValue, ((IntOpenHashSet) resultList).size());
    }

    singleGroupByResult = combinedGroupByResult.get(DISTINCT_DIM1_AGGREGATION_INDEX);
    for (String keyString : singleGroupByResult.keySet()) {
      Serializable resultList = singleGroupByResult.get(keyString);
      LOGGER.debug("grouped key: {}, value: {}", keyString, resultList);
      int expectedAvgValue = 10;
      assertEquals(expectedAvgValue, ((IntOpenHashSet) resultList).size());
    }

    BrokerReduceService reduceService = new BrokerReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    BrokerResponseNative brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.debug(new JSONArray(brokerResponse.getAggregationResults()).toString());
    LOGGER.debug("Time used: {}", brokerResponse.getTimeUsedMs());
  }

  @Test
  public void testInterSegmentSelectionPlanMaker()
      throws JSONException {
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setAggregationsInfo(null);
    brokerRequest.setAggregationsInfoIsSet(false);
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(20);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    BrokerReduceService reduceService = new BrokerReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);

    BrokerResponseNative brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.debug(brokerResponse.getSelectionResults().toString());
    LOGGER.debug("Time used: {}", brokerResponse.getTimeUsedMs());
    LOGGER.debug(brokerResponse.toString());
    List<Serializable[]> rows = brokerResponse.getSelectionResults().getRows();
    for (int i = 0; i < rows.size(); i++) {
      Serializable[] actualValues = rows.get(i);
      assertEquals(Double.parseDouble(actualValues[0].toString()), 1.0);
      assertEquals(Double.parseDouble(actualValues[1].toString()), 91.0);
      assertEquals(Double.parseDouble(actualValues[2].toString()) % 100, 91.0);
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
    Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    BrokerReduceService defaultReduceService = new BrokerReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);

    BrokerResponseNative brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.debug(brokerResponse.getSelectionResults().toString());
    LOGGER.debug("TimeUsedMs: {}", brokerResponse.getTimeUsedMs());
    LOGGER.debug(brokerResponse.toString());

    List<Serializable[]> rows = brokerResponse.getSelectionResults().getRows();
    for (Serializable[] row : rows) {
      Assert.assertEquals(Double.parseDouble(row[0].toString()), 1.0);
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
}
