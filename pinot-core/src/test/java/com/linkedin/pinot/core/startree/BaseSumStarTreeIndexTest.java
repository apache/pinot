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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.metadata.segment.SegmentMetadata;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexOperator;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.query.utils.StarTreeUtils;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Base class containing common functionality for all star-tree integration tests.
 */
public class BaseSumStarTreeIndexTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSumStarTreeIndexTest.class);

  protected final long _randomSeed = System.nanoTime();

  protected String[] _hardCodedQueries =
      new String[]{
          "select sum(m1) from T",
          "select sum(m1) from T where d1 = 'd1-v1'",
          "select sum(m1) from T where d1 <> 'd1-v1'",
          "select sum(m1) from T where d1 between 'd1-v1' and 'd1-v3'",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2')",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1')",
          "select sum(m1) from T group by d1", "select sum(m1) from T group by d1, d2",
          "select sum(m1) from T where d1 = 'd1-v2' group by d1",
          "select sum(m1) from T where d1 between 'd1-v1' and 'd1-v3' group by d2",
          "select sum(m1) from T where d1 = 'd1-v2' group by d2, d3",
          "select sum(m1) from T where d1 <> 'd1-v1' group by d2",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') group by d2",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1') group by d3",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1') group by d3, d4"};

  protected void testHardCodedQueries(IndexSegment segment, Schema schema) {
    // Test against all metric columns, instead of just the aggregation column in the query.
    List<String> metricNames = schema.getMetricNames();
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();

    for (int i = 0; i < _hardCodedQueries.length; i++) {
      Pql2Compiler compiler = new Pql2Compiler();
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(_hardCodedQueries[i]);

      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      Assert.assertTrue(StarTreeUtils.isFitForStarTreeIndex(segmentMetadata, filterQueryTree, brokerRequest));

      Map<String, double[]> expectedResult = computeSumUsingRawDocs(segment, metricNames, brokerRequest);
      Map<String, double[]> actualResult = computeSumUsingAggregatedDocs(segment, metricNames, brokerRequest);

      Assert.assertEquals(expectedResult.size(), actualResult.size(), "Mis-match in number of groups");
      for (Map.Entry<String, double[]> entry : expectedResult.entrySet()) {
        String expectedKey = entry.getKey();
        Assert.assertTrue(actualResult.containsKey(expectedKey));

        double[] expectedSums = entry.getValue();
        double[] actualSums = actualResult.get(expectedKey);

        for (int j = 0; j < expectedSums.length; j++) {
          Assert.assertEquals(actualSums[j], expectedSums[j],
              "Mis-match sum for key '" + expectedKey + "', Metric: " + metricNames.get(j) + ", Random Seed: "
                  + _randomSeed);
        }
      }
    }
  }

  /**
   * Helper method to compute the sums using raw index.
   *  @param metricNames
   * @param brokerRequest
   */
  private Map<String, double[]> computeSumUsingRawDocs(IndexSegment segment, List<String> metricNames,
      BrokerRequest brokerRequest) {
    FilterPlanNode planNode = new FilterPlanNode(segment, brokerRequest);
    Operator rawOperator = planNode.run();
    BlockDocIdIterator rawDocIdIterator = rawOperator.nextBlock().getBlockDocIdSet().iterator();

    List<String> groupByColumns = Collections.EMPTY_LIST;
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy()) {
      groupByColumns = brokerRequest.getGroupBy().getColumns();
    }
    return computeSum(segment, rawDocIdIterator, metricNames, groupByColumns);
  }

  /**
   * Helper method to compute the sum using aggregated docs.
   * @param metricNames
   * @param brokerRequest
   * @return
   */
  private Map<String, double[]> computeSumUsingAggregatedDocs(IndexSegment segment, List<String> metricNames,
      BrokerRequest brokerRequest) {
    StarTreeIndexOperator starTreeOperator = new StarTreeIndexOperator(segment, brokerRequest);
    starTreeOperator.open();
    BlockDocIdIterator starTreeDocIdIterator = starTreeOperator.nextBlock().getBlockDocIdSet().iterator();

    List<String> groupByColumns = Collections.EMPTY_LIST;
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy()) {
      groupByColumns = brokerRequest.getGroupBy().getColumns();
    }

    return computeSum(segment, starTreeDocIdIterator, metricNames, groupByColumns);
  }

  /**
   * Compute 'sum' for a given list of metrics, by scanning the given set of doc-ids.
   *
   * @param segment
   * @param docIdIterator
   * @param metricNames
   * @return
   */
  private Map<String, double[]> computeSum(IndexSegment segment, BlockDocIdIterator docIdIterator,
      List<String> metricNames, List<String> groupByColumns) {
    int docId;
    int numMetrics = metricNames.size();
    Dictionary[] metricDictionaries = new Dictionary[numMetrics];
    BlockSingleValIterator[] metricValIterators = new BlockSingleValIterator[numMetrics];

    int numGroupByColumns = groupByColumns.size();
    Dictionary[] groupByDictionaries = new Dictionary[numGroupByColumns];
    BlockSingleValIterator[] groupByValIterators = new BlockSingleValIterator[numGroupByColumns];

    for (int i = 0; i < numMetrics; i++) {
      String metricName = metricNames.get(i);
      DataSource dataSource = segment.getDataSource(metricName);
      metricDictionaries[i] = dataSource.getDictionary();
      metricValIterators[i] = (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
    }

    for (int i = 0; i < numGroupByColumns; i++) {
      String groupByColumn = groupByColumns.get(i);
      DataSource dataSource = segment.getDataSource(groupByColumn);
      groupByDictionaries[i] = dataSource.getDictionary();
      groupByValIterators[i] = (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
    }

    Map<String, double[]> result = new HashMap<String, double[]>();
    while ((docId = docIdIterator.next()) != Constants.EOF) {

      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < numGroupByColumns; i++) {
        groupByValIterators[i].skipTo(docId);
        int dictId = groupByValIterators[i].nextIntVal();
        stringBuilder.append(groupByDictionaries[i].getStringValue(dictId));
        stringBuilder.append("_");
      }

      String key = stringBuilder.toString();
      if (!result.containsKey(key)) {
        result.put(key, new double[numMetrics]);
      }

      double[] sumsSoFar = result.get(key);
      for (int i = 0; i < numMetrics; i++) {
        metricValIterators[i].skipTo(docId);
        int dictId = metricValIterators[i].nextIntVal();
        sumsSoFar[i] += metricDictionaries[i].getDoubleValue(dictId);
      }
    }

    return result;
  }
}
