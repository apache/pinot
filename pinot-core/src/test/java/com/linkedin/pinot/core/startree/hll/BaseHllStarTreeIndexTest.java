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
package com.linkedin.pinot.core.startree.hll;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Base class containing common functionality for all star-tree integration tests.
 */
public class BaseHllStarTreeIndexTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseHllStarTreeIndexTest.class);

  protected final long _randomSeed = System.nanoTime();

  /**
   * We test on d3, d4 since we deliberately make cardinality of d3, d4 much larger than other columns
   * to mimic actual use cases
   */
  private static final Set<String> columnsToDeriveHllFields = new HashSet<>(Arrays.asList("d3", "d4"));
  protected static final HllConfig HLL_CONFIG = new HllConfig(
      HllConstants.DEFAULT_LOG2M, columnsToDeriveHllFields, HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX);

  protected String[] _hardCodedQueries =
          new String[]{
                  "select fasthll(d4) from T",
                  "select fasthll(d4) from T where d1 = 'd1-v1'",
                  "select fasthll(d4) from T where d1 <> 'd1-v1'",
                  "select fasthll(d4) from T where d1 between 'd1-v1' and 'd1-v3'",
                  "select fasthll(d4) from T where d1 in ('d1-v1', 'd1-v2')",
                  "select fasthll(d4) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1')",
                  "select fasthll(d4) from T group by d1",
                  "select fasthll(d4) from T group by d1, d2",
                  "select fasthll(d4) from T where d1 = 'd1-v2' group by d1",
                  "select fasthll(d4) from T where d1 between 'd1-v1' and 'd1-v3' group by d2",
                  "select fasthll(d4) from T where d1 = 'd1-v2' group by d2, d3",
                  "select fasthll(d4) from T where d1 <> 'd1-v1' group by d2",
                  "select fasthll(d4) from T where d1 in ('d1-v1', 'd1-v2') group by d2",
                  "select fasthll(d4) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1') group by d3",
                  "select fasthll(d4) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1') group by d3, d4"
          };

  void testHardCodedQueries(IndexSegment segment, Schema schema) throws Exception {
    // only use metric corresponding to columnsToDeriveHllFields
    List<String> metricNames = new ArrayList<>();
    for (String column: columnsToDeriveHllFields) {
      metricNames.add(column + HLL_CONFIG.getHllDeriveColumnSuffix());
    }

    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();

    LOGGER.info("[Schema] Dim: {} Metric: {}", schema.getDimensionNames(), schema.getMetricNames());

    for (int i = 0; i < _hardCodedQueries.length; i++) {
      Pql2Compiler compiler = new Pql2Compiler();
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(_hardCodedQueries[i]);

      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      Assert.assertTrue(StarTreeUtils.isFitForStarTreeIndex(segmentMetadata, filterQueryTree, brokerRequest));

      // Group -> Projected values of each group
      Map<String, long[]> expectedResult = computeHllUsingRawDocs(segment, metricNames, brokerRequest);
      Map<String, long[]> actualResult = computeHllUsingAggregatedDocs(segment, metricNames, brokerRequest);

      Assert.assertEquals(expectedResult.size(), actualResult.size(), "Mis-match in number of groups");
      for (Map.Entry<String, long[]> entry : expectedResult.entrySet()) {
        String expectedKey = entry.getKey();
        Assert.assertTrue(actualResult.containsKey(expectedKey));

        long[] expectedSums = entry.getValue();
        long[] actualSums = actualResult.get(expectedKey);

        for (int j = 0; j < expectedSums.length; j++) {
          LOGGER.info("actual hll: {} ", actualSums[j]);
          LOGGER.info("expected hll: {} ", expectedSums[j]);
          Assert.assertEquals(actualSums[j], expectedSums[j],
                  "Mis-match hll for key '" + expectedKey + "', Metric: " + metricNames.get(j) + ", Random Seed: "
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
  private Map<String, long[]> computeHllUsingRawDocs(IndexSegment segment, List<String> metricNames,
                                                     BrokerRequest brokerRequest) throws Exception {
    FilterPlanNode planNode = new FilterPlanNode(segment, brokerRequest);
    Operator rawOperator = planNode.run();
    BlockDocIdIterator rawDocIdIterator = rawOperator.nextBlock().getBlockDocIdSet().iterator();

    List<String> groupByColumns = Collections.EMPTY_LIST;
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy()) {
      groupByColumns = brokerRequest.getGroupBy().getColumns();
    }
    return computeHll(segment, rawDocIdIterator, metricNames, groupByColumns);
  }

  /**
   * Helper method to compute the sum using aggregated docs.
   * @param metricNames
   * @param brokerRequest
   * @return
   */
  private Map<String, long[]> computeHllUsingAggregatedDocs(IndexSegment segment, List<String> metricNames,
                                                            BrokerRequest brokerRequest) throws Exception {
    StarTreeIndexOperator starTreeOperator = new StarTreeIndexOperator(segment, brokerRequest);
    starTreeOperator.open();
    BlockDocIdIterator starTreeDocIdIterator = starTreeOperator.nextBlock().getBlockDocIdSet().iterator();

    List<String> groupByColumns = Collections.EMPTY_LIST;
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy()) {
      groupByColumns = brokerRequest.getGroupBy().getColumns();
    }

    return computeHll(segment, starTreeDocIdIterator, metricNames, groupByColumns);
  }

  /**
   * Compute 'sum' for a given list of metrics, by scanning the given set of doc-ids.
   *
   * @param segment
   * @param docIdIterator
   * @param metricNames
   * @return
   */
  private Map<String, long[]> computeHll(IndexSegment segment, BlockDocIdIterator docIdIterator,
                                         List<String> metricNames, List<String> groupByColumns)
          throws Exception {
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

    Map<String, HyperLogLog[]> result = new HashMap<>();
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
        // init
        HyperLogLog[] initHllArray = new HyperLogLog[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
          initHllArray[i] = new HyperLogLog(HLL_CONFIG.getHllLog2m());
        }
        result.put(key, initHllArray);
      }

      HyperLogLog[] hllSoFar = result.get(key);
      for (int i = 0; i < numMetrics; i++) {
        metricValIterators[i].skipTo(docId);
        int dictId = metricValIterators[i].nextIntVal();
        HyperLogLog value = HllUtil.convertStringToHll(metricDictionaries[i].getStringValue(dictId));
        hllSoFar[i].addAll(value);
      }
    }

    // construct ret
    Map<String, long[]> ret = new HashMap<>();
    for (String key: result.keySet()) {
      long[] valueArray = new long[numMetrics];
      ret.put(key, valueArray);
      for (int i = 0; i < numMetrics; i++) {
        valueArray[i] = result.get(key)[i].cardinality();
      }
    }

    return ret;
  }
}
