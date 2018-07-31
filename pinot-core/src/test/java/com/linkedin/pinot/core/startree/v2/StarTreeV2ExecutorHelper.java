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
import org.testng.Assert;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexBasedFilterOperator;


public abstract class StarTreeV2ExecutorHelper {

  private static int _numGroupByColumns;
  private static int _numMetricColumns;
  private static Dictionary[] _metricDictionaries;
  private static BlockSingleValIterator[] _metricValIterators;
  private static BlockSingleValIterator[] _groupByValIterators;

  private static IndexSegment _segment;
  private static BrokerRequest _brokerRequest;
  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  private static final String[] STAR_TREE1_HARD_CODED_QUERIES =
      new String[]{"SELECT SUM(salary) FROM T", "SELECT MAX(m1) FROM T WHERE Country IN ('US', 'IN') AND Name NOT IN ('Rahul') GROUP BY Language",};

  public static void loadSegment(File indexDir) throws Exception {
    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.heap);
  }

  public static String[] getHardCodedQueries() {
    return STAR_TREE1_HARD_CODED_QUERIES;
  }

  public static List<String> getMetricColumns(StarTreeV2Config config) {
    List<String> pairs = new ArrayList<>();
    for (AggregationFunctionColumnPair pair : config.getMetric2aggFuncPairs()) {
      String s = pair.getColumn() + "_" + pair.getFunctionType().getName();
      pairs.add(s);
    }
    pairs.add("count");

    return pairs;
  }

  public static void execute(List<StarTreeV2Config> starTreeV2ConfigList,
      List<Map<String, DataSource>> starTreeDataSourcesList, int starTreeId) throws Exception {

    Map<String, DataSource> dataSources = starTreeDataSourcesList.get(starTreeId);
    List<String> metricColumns = getMetricColumns(starTreeV2ConfigList.get(starTreeId));

    _numMetricColumns = metricColumns.size();
    _metricDictionaries = new Dictionary[_numMetricColumns];
    _metricValIterators = new BlockSingleValIterator[_numMetricColumns];
    for (int i = 0; i < _numMetricColumns; i++) {
      DataSource dataSource = dataSources.get(metricColumns.get(i));
      _metricDictionaries[i] = dataSource.getDictionary();
      _metricValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
    }

    String[] queries = getHardCodedQueries();
    for (String query : queries) {
      _brokerRequest = COMPILER.compileToBrokerRequest(query);
      List<String> groupByColumns;
      if (_brokerRequest.isSetGroupBy()) {
        groupByColumns = _brokerRequest.getGroupBy().getColumns();
      } else {
        groupByColumns = Collections.emptyList();
      }

      _numGroupByColumns = groupByColumns.size();
      _groupByValIterators = new BlockSingleValIterator[_numGroupByColumns];
      for (int i = 0; i < _numGroupByColumns; i++) {
        DataSource dataSource = dataSources.get(groupByColumns.get(i));
        _groupByValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
      }

      Assert.assertEquals(computeUsingAggregatedDocs(), computeUsingRawDocs(), "Comparison failed for query: " + query);
    }
  }

  private static Map<List<Integer>, List<Double>> computeUsingRawDocs() throws Exception {
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(_brokerRequest);
    Operator filterOperator = FilterPlanNode.constructPhysicalOperator(filterQueryTree, _segment);
    Assert.assertFalse(filterOperator instanceof StarTreeIndexBasedFilterOperator);

    return compute(filterOperator);
  }

  private static Map<List<Integer>, List<Double>> computeUsingAggregatedDocs() throws Exception {
    Operator filterOperator = new FilterPlanNode(_segment, _brokerRequest).run();
    Assert.assertTrue(filterOperator instanceof StarTreeIndexBasedFilterOperator);

    return compute(filterOperator);
  }

  private static Map<List<Integer>, List<Double>> compute(Operator filterOperator) throws Exception {
    BlockDocIdIterator docIdIterator = filterOperator.nextBlock().getBlockDocIdSet().iterator();

    Map<List<Integer>, List<Double>> results = new HashMap<>();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {

      List<Integer> groupKeys = new ArrayList<>(_numGroupByColumns);
      for (int i = 0; i < _numGroupByColumns; i++) {
        _groupByValIterators[i].skipTo(docId);
        groupKeys.add(_groupByValIterators[i].nextIntVal());
      }

      List<Double> sums = results.get(groupKeys);
      if (sums == null) {
        sums = new ArrayList<>(_numMetricColumns);
        for (int i = 0; i < _numMetricColumns; i++) {
          sums.add(0.0);
        }
        results.put(groupKeys, sums);
      }
      for (int i = 0; i < _numMetricColumns; i++) {
        _metricValIterators[i].skipTo(docId);
        int dictId = _metricValIterators[i].nextIntVal();
        sums.set(i, sums.get(i) + _metricDictionaries[i].getDoubleValue(dictId));
      }
    }
    return results;
  }
}
