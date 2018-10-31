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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexBasedFilterOperator;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.Assert;


/**
 * Base class containing common functionality for all star-tree index tests.
 */
public abstract class BaseStarTreeIndexTest {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  // Set up segment before running test.
  protected IndexSegment _segment;

  protected BrokerRequest _brokerRequest;
  protected int _numMetricColumns;
  protected Dictionary[] _metricDictionaries;
  protected BlockSingleValIterator[] _metricValIterators;
  protected int _numGroupByColumns;
  protected BlockSingleValIterator[] _groupByValIterators;

  protected abstract String[] getHardCodedQueries();

  protected abstract List<String> getMetricColumns();

  protected void testHardCodedQueries() throws Exception {
    Assert.assertNotNull(_segment);

    List<String> metricColumns = getMetricColumns();
    _numMetricColumns = metricColumns.size();
    _metricDictionaries = new Dictionary[_numMetricColumns];
    _metricValIterators = new BlockSingleValIterator[_numMetricColumns];
    for (int i = 0; i < _numMetricColumns; i++) {
      DataSource dataSource = _segment.getDataSource(metricColumns.get(i));
      _metricDictionaries[i] = dataSource.getDictionary();
      _metricValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
    }

    for (String query : getHardCodedQueries()) {
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
        DataSource dataSource = _segment.getDataSource(groupByColumns.get(i));
        _groupByValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
      }

      Assert.assertEquals(computeUsingAggregatedDocs(), computeUsingRawDocs(), "Comparison failed for query: " + query);
    }
  }

  /**
   * Helper method to compute the result using raw docs.
   */
  private Map<List<Integer>, List<Double>> computeUsingRawDocs() throws Exception {
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(_brokerRequest);
    Operator filterOperator = FilterPlanNode.constructPhysicalOperator(filterQueryTree, _segment);
    Assert.assertFalse(filterOperator instanceof StarTreeIndexBasedFilterOperator);

    return compute(filterOperator);
  }

  /**
   * Helper method to compute the result using aggregated docs.
   */
  private Map<List<Integer>, List<Double>> computeUsingAggregatedDocs() throws Exception {
    Operator filterOperator = new FilterPlanNode(_segment, _brokerRequest).run();
    Assert.assertTrue(filterOperator instanceof StarTreeIndexBasedFilterOperator);

    return compute(filterOperator);
  }

  /**
   * Compute the result by scanning the docIds filtered out from the given filter operator.
   * <p>The result is a map from a list of dictIds (group key) to an array (results for aggregations)
   */
  protected abstract Map<List<Integer>, List<Double>> compute(Operator filterOperator) throws Exception;
}
