/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2;

import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.HashSet;
import java.util.HashMap;
import org.testng.Assert;
import java.util.ArrayList;
import java.util.Collections;
import com.google.common.io.Files;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.startree.plan.StarTreeFilterPlanNode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;


public class OffHeapStarTreeV2IntegrationTest {

  private File _onHeapIndexDir;
  private File _offHeapIndexDir;
  private File _onHeapFilePath;
  private File _offHeapFilePath;

  private IndexSegment _segment;
  private int _numMetricColumns;
  private int _numGroupByColumns;
  private BrokerRequest _brokerRequest;
  private Dictionary[] _metricDictionaries;
  private BlockSingleValIterator[] _metricValIterators;
  private BlockSingleValIterator[] _groupByValIterators;


  private int _starTreeId;
  private List<FieldSpec.DataType> _metricDataType;
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private List<StarTreeV2Config> _onHeapStarTreeV2ConfigList = new ArrayList<>();
  private List<StarTreeV2Config> _offHeapStarTreeV2ConfigList = new ArrayList<>();



  private final String[] STAR_TREE1_HARD_CODED_QUERIES =
      new String[] {
          "SELECT SUM(salary) FROM T GROUP BY Name",
          "SELECT MAX(m1) FROM T WHERE Country IN ('US', 'IN') AND Name NOT IN ('Rahul') GROUP BY Language"
  };

  @BeforeTest
  void setUp() throws Exception {

    String segmentName = "starTreeV2BuilderTest";
    String onHeapSegmentOutputDir = Files.createTempDir().toString();
    String offHeapSegmentOutputDir = Files.createTempDir().toString();

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();

    List<GenericRow> _rows = StarTreeV2SegmentHelper.createSegmentSmallData(schema);

    //List<GenericRow> _rows = StarTreeV2SegmentHelper.createSegmentLargeData(schema);

    RecordReader _recordReader = new GenericRowRecordReader(_rows, schema);
    _onHeapIndexDir = StarTreeV2SegmentHelper.createSegment(schema, segmentName, onHeapSegmentOutputDir, _recordReader);
    _offHeapIndexDir = StarTreeV2SegmentHelper.createSegment(schema, segmentName, offHeapSegmentOutputDir, _recordReader);

    _onHeapFilePath = new File(_onHeapIndexDir, "v3");
    _offHeapFilePath = new File(_offHeapIndexDir, "v3");

    List<AggregationFunctionColumnPair> metric2aggFuncPairs1 = new ArrayList<>();
    List<AggregationFunctionColumnPair> metric2aggFuncPairs2 = new ArrayList<>();

    AggregationFunctionColumnPair pair1 = new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "salary");
    AggregationFunctionColumnPair pair2 = new AggregationFunctionColumnPair(AggregationFunctionType.MAX, "salary");
    AggregationFunctionColumnPair pair3 = new AggregationFunctionColumnPair(AggregationFunctionType.MIN, "salary");
    AggregationFunctionColumnPair pair7 = new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "star");


    AggregationFunctionColumnPair pair4 =
        new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTHLL, "salary");
    AggregationFunctionColumnPair pair5 =
        new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILETDIGEST, "salary");
    AggregationFunctionColumnPair pair6 =
        new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILEEST, "salary");

    metric2aggFuncPairs1.add(pair1);
    metric2aggFuncPairs1.add(pair2);
    metric2aggFuncPairs1.add(pair3);
    metric2aggFuncPairs1.add(pair4);
    metric2aggFuncPairs1.add(pair5);
    metric2aggFuncPairs1.add(pair6);
    metric2aggFuncPairs1.add(pair7);

    metric2aggFuncPairs2.add(pair1);
    metric2aggFuncPairs2.add(pair2);
    metric2aggFuncPairs2.add(pair3);
    metric2aggFuncPairs2.add(pair4);
    metric2aggFuncPairs2.add(pair5);
    metric2aggFuncPairs2.add(pair6);
    metric2aggFuncPairs2.add(pair7);

    StarTreeV2Config _starTreeV2Config1 = new StarTreeV2Config();
    _starTreeV2Config1.setOutDir(_onHeapFilePath);
    _starTreeV2Config1.setMaxNumLeafRecords(1);
    _starTreeV2Config1.setDimensions(schema.getDimensionNames());
    _starTreeV2Config1.setMetric2aggFuncPairs(metric2aggFuncPairs1);
    _onHeapStarTreeV2ConfigList.add(_starTreeV2Config1);

    StarTreeV2Config _starTreeV2Config2 = new StarTreeV2Config();
    _starTreeV2Config2.setOutDir(_offHeapFilePath);
    _starTreeV2Config2.setMaxNumLeafRecords(1);
    _starTreeV2Config2.setDimensions(schema.getDimensionNames());
    _starTreeV2Config2.setMetric2aggFuncPairs(metric2aggFuncPairs2);
    _offHeapStarTreeV2ConfigList.add(_starTreeV2Config2);

    return;
  }

  @Test
  public void testBuilder() throws Exception {
    OnHeapStarTreeV2Builder onheapBuildTest = new OnHeapStarTreeV2Builder();
    OffHeapStarTreeV2Builder offHeapBuildTest = new OffHeapStarTreeV2Builder();
    try {
      for (int i = 0; i < _onHeapStarTreeV2ConfigList.size(); i++) {
        onheapBuildTest.init(_onHeapIndexDir, _onHeapStarTreeV2ConfigList.get(i));
        onheapBuildTest.build();
        onheapBuildTest.serialize();
      }

      for (int i = 0; i < _offHeapStarTreeV2ConfigList.size(); i++) {
        offHeapBuildTest.init(_offHeapIndexDir, _offHeapStarTreeV2ConfigList.get(i));
        offHeapBuildTest.build();
        offHeapBuildTest.serialize();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLoader() throws Exception {
    OnHeapStarTreeV2Loader onHeapLoadTest = new OnHeapStarTreeV2Loader();
    OnHeapStarTreeV2Loader offHeapLoadTest = new OnHeapStarTreeV2Loader();

    IndexLoadingConfig v3IndexLoadingConfig = new IndexLoadingConfig();
    v3IndexLoadingConfig.setReadMode(ReadMode.mmap);
    v3IndexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    ImmutableSegment onHeapImmutableSegment = ImmutableSegmentLoader.load(_onHeapIndexDir, v3IndexLoadingConfig);
    ImmutableSegment offHeapImmutableSegment = ImmutableSegmentLoader.load(_offHeapIndexDir, v3IndexLoadingConfig);


    try {
      List<StarTreeV2> onHeapStarTreeImplList = onHeapLoadTest.load(_onHeapFilePath, onHeapImmutableSegment);
      List<StarTreeV2> offHeapStarTreeImplList = offHeapLoadTest.load(_offHeapFilePath, offHeapImmutableSegment);

      int starTreeId = 0;
      for ( int i = 0; i < onHeapStarTreeImplList.size(); i++) {
        System.out.println("Working on star tree " + Integer.toString(starTreeId + 1));

        for (String dimension : _onHeapStarTreeV2ConfigList.get(starTreeId).getDimensions()) {
          System.out.println("Checking for dimension : " + dimension);
          DataSource source1 = onHeapStarTreeImplList.get(i).getDataSource(dimension);
          DataSource source2 = offHeapStarTreeImplList.get(i).getDataSource(dimension);
          StarTreeV2LoaderHelper.compareDimensionDataSources(source1, source2);
        }

        for (AggregationFunctionColumnPair pair : _onHeapStarTreeV2ConfigList.get(starTreeId).getMetric2aggFuncPairs()) {
          String metPair = pair.toColumnName();
          DataSource source1 = onHeapStarTreeImplList.get(i).getDataSource(metPair);
          DataSource source2 = offHeapStarTreeImplList.get(i).getDataSource(metPair);
          System.out.println("Printing for Met2AggPair : " + metPair);
          StarTreeV2LoaderHelper.compareMetricAggfuncDataFromDataSource(source1, source2, pair.getFunctionType().getName());
        }
        starTreeId += 1;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

//  @Test
//  public void testExecutor() throws Exception {
//    try {
//      _segment = ImmutableSegmentLoader.load(_indexDir, ReadMode.heap);
//      for ( int i = 0; i < _starTreeV2ConfigList.size(); i++) {
//        _starTreeId = i;
//        testQueries(i);
//      }
//      _segment.destroy();
//
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  protected String[] getHardCodedQueries() {
//    return STAR_TREE1_HARD_CODED_QUERIES;
//  }
//
//  protected List<String> getMetricColumns() {
//
//    List<String> pairs = new ArrayList<>();
//    _metricDataType = new ArrayList<>();
//    AggregationFunctionFactory factory = new AggregationFunctionFactory();
//
//    for (AggregationFunctionColumnPair pair : _starTreeV2ConfigList.get(_starTreeId).getMetric2aggFuncPairs()) {
//      String s = pair.toColumnName();
//      pairs.add(s);
//      AggregationFunction function = factory.getAggregationFunction(pair.getFunctionType().getName());
//      _metricDataType.add(function.getResultDataType());
//    }
//    return pairs;
//  }
//
//  protected Map<List<Integer>, List<Double>> computeAggregated(Operator filterOperator) throws Exception {
//    BlockDocIdIterator docIdIterator = filterOperator.nextBlock().getBlockDocIdSet().iterator();
//
//    Map<List<Integer>, List<Double>> results = new HashMap<>();
//    int docId;
//    while ((docId = docIdIterator.next()) != Constants.EOF) {
//
//      List<Integer> groupKeys = new ArrayList<>(_numGroupByColumns);
//      for (int i = 0; i < _numGroupByColumns; i++) {
//        _groupByValIterators[i].skipTo(docId);
//        groupKeys.add(_groupByValIterators[i].nextIntVal());
//      }
//
//      List<Double> sums = results.get(groupKeys);
//      if (sums == null) {
//        sums = new ArrayList<>(_numMetricColumns);
//        for (int i = 0; i < _numMetricColumns; i++) {
//          sums.add(0.0);
//        }
//        results.put(groupKeys, sums);
//      }
//
//      for (int i = 0; i < _numMetricColumns; i++) {
//        if (_metricDataType.get(i).equals(FieldSpec.DataType.DOUBLE)) {
//          double abc = _metricValIterators[i].nextDoubleVal();
//          System.out.println(abc);
//          sums.set(i, sums.get(i) + abc);
//        }
//      }
//    }
//    return results;
//  }
//
//  protected Map<List<Integer>, List<Double>> compute(Operator filterOperator) {
//    BlockDocIdIterator docIdIterator = filterOperator.nextBlock().getBlockDocIdSet().iterator();
//
//    Map<List<Integer>, List<Double>> results = new HashMap<>();
//    int docId;
//    while ((docId = docIdIterator.next()) != Constants.EOF) {
//
//      List<Integer> groupKeys = new ArrayList<>(_numGroupByColumns);
//      for (int i = 0; i < _numGroupByColumns; i++) {
//        _groupByValIterators[i].skipTo(docId);
//        groupKeys.add(_groupByValIterators[i].nextIntVal());
//      }
//
//      List<Double> sums = results.get(groupKeys);
//      if (sums == null) {
//        sums = new ArrayList<>(_numMetricColumns);
//        for (int i = 0; i < _numMetricColumns; i++) {
//          sums.add(0.0);
//        }
//        results.put(groupKeys, sums);
//      }
//      for (int i = 0; i < _numMetricColumns; i++) {
//        _metricValIterators[i].skipTo(docId);
//        int dictId = _metricValIterators[i].nextIntVal();
//        sums.set(i, sums.get(i) + _metricDictionaries[i].getDoubleValue(dictId));
//      }
//    }
//
//    return results;
//  }
//
//  protected void testQueries(int starTreeId) throws Exception {
//    Assert.assertNotNull(_segment);
//    List<StarTreeV2> starTrees = _segment.getStarTrees();
//
//    List<String> metricColumns = getMetricColumns();
//    _numMetricColumns = metricColumns.size();
//    _metricValIterators = new BlockSingleValIterator[_numMetricColumns];
//    for (int i = 0; i < metricColumns.size(); i++) {
//      DataSource dataSource = starTrees.get(starTreeId).getDataSource(metricColumns.get(i));
//      _metricValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
//    }
//
//    for (String query : getHardCodedQueries()) {
//      _brokerRequest = COMPILER.compileToBrokerRequest(query);
//      List<String> groupByColumns = _brokerRequest.isSetGroupBy() ? _brokerRequest.getGroupBy().getColumns() : Collections.emptyList();
//      _numGroupByColumns = groupByColumns.size();
//      _groupByValIterators = new BlockSingleValIterator[_numGroupByColumns];
//
//      for (int i = 0; i < _numGroupByColumns; i++) {
//        DataSource dataSource = starTrees.get(starTreeId).getDataSource(groupByColumns.get(i));
//        _groupByValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
//      }
//
//      Assert.assertEquals(computeUsingAggregatedDocs(), computeUsingRawDocs(), "Comparison failed for query: " + query);
//    }
//  }
//
//  /**
//   * Helper method to compute the result using raw docs.
//   */
//  protected Map<List<Integer>, List<Double>> computeUsingRawDocs() throws Exception {
//    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(_brokerRequest);
//    Operator filterOperator;
//    if (_numGroupByColumns > 0) {
//      filterOperator = new StarTreeFilterPlanNode(_segment.getStarTrees().get(0), rootFilterNode,
//          new HashSet<>(_brokerRequest.getGroupBy().getColumns())).run();
//    } else {
//      filterOperator = new StarTreeFilterPlanNode(_segment.getStarTrees().get(0), rootFilterNode, null).run();
//    }
//    return compute(filterOperator);
//  }
//
//  /**
//   * Helper method to compute the result using aggregated docs.
//   */
//  protected Map<List<Integer>, List<Double>> computeUsingAggregatedDocs() throws Exception {
//    Operator filterOperator = new FilterPlanNode(_segment, _brokerRequest).run();
//    return computeAggregated(filterOperator);
//  }
}