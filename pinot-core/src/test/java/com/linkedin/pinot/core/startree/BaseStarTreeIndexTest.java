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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.startree.plan.StarTreeFilterPlanNode;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  protected Set<String> _groupByColumns;
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

      _groupByColumns = new HashSet<>();
      if (_brokerRequest.isSetGroupBy()) {
        for (String groupByExpression : _brokerRequest.getGroupBy().getExpressions()) {
          TransformExpressionTree.compileToExpressionTree(groupByExpression).getColumns(_groupByColumns);
        }
      }
      _numGroupByColumns = _groupByColumns.size();
      _groupByValIterators = new BlockSingleValIterator[_numGroupByColumns];
      int index = 0;
      for (String groupByColumn : _groupByColumns) {
        _groupByValIterators[index++] =
            (BlockSingleValIterator) _segment.getDataSource(groupByColumn).nextBlock().getBlockValueSet().iterator();
      }

      Assert.assertEquals(computeWithoutStarTree(), computeWithStarTree(), "Comparison failed for query: " + query);
    }
  }

  /**
   * Helper method to compute the result using raw docs.
   */
  private Map<List<Integer>, List<Double>> computeWithStarTree() throws Exception {
    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(_brokerRequest);
    Operator filterOperator;
    if (_numGroupByColumns > 0) {
      filterOperator =
          new StarTreeFilterPlanNode(_segment.getStarTrees().get(0), rootFilterNode,
              _groupByColumns, _brokerRequest.getDebugOptions()).run();
    } else {
      filterOperator = new StarTreeFilterPlanNode(_segment.getStarTrees().get(0), rootFilterNode,
          null, _brokerRequest.getDebugOptions()).run();
    }
    return compute(filterOperator);
  }

  /**
   * Helper method to compute the result using aggregated docs.
   */
  private Map<List<Integer>, List<Double>> computeWithoutStarTree() throws Exception {
    Operator filterOperator = new FilterPlanNode(_segment, _brokerRequest).run();
    return compute(filterOperator);
  }

  /**
   * Compute the result by scanning the docIds filtered out from the given filter operator.
   * <p>The result is a map from a list of dictIds (group key) to an array (results for aggregations)
   */
  protected abstract Map<List<Integer>, List<Double>> compute(Operator filterOperator) throws Exception;
}
