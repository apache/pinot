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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.startree.plan.StarTreeFilterPlanNode;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.Assert;


/**
 * Base class to test star-tree index by scanning and aggregating records filtered out from the
 * {@link StarTreeFilterPlanNode} against normal {@link FilterPlanNode}.
 * <p>Set up {@link IndexSegment} and {@link StarTreeV2} objects before calling {@link #testQuery(String)}.
 * <p>The query tested should be fit for star-tree.
 *
 * @param <R> Type or raw value
 * @param <A> Type of aggregated value
 */
public abstract class BaseStarTreeV2Test<R, A> {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final Long NON_STAR_TREE_COUNT_VALUE = 1L;

  // Set up index segment and star-tree before running tests
  protected IndexSegment _indexSegment;
  protected StarTreeV2 _starTreeV2;

  protected void testQuery(String query) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);

    // Aggregations
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();
    int numAggregations = aggregationInfos.size();
    List<AggregationFunctionColumnPair> aggregationFunctionColumnPairs = new ArrayList<>(numAggregations);
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      aggregationFunctionColumnPairs.add(AggregationFunctionUtils.getFunctionColumnPair(aggregationInfo));
    }

    // Group-by columns
    Set<String> groupByColumnSet = new HashSet<>();
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      for (String expression : groupBy.getExpressions()) {
        TransformExpressionTree.compileToExpressionTree(expression).getColumns(groupByColumnSet);
      }
    }
    int numGroupByColumns = groupByColumnSet.size();
    List<String> groupByColumns = new ArrayList<>(groupByColumnSet);

    // Filters
    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(brokerRequest);

    // Extract values with star-tree
    PlanNode starTreeFilterPlanNode;
    if (groupByColumns.isEmpty()) {
      starTreeFilterPlanNode = new StarTreeFilterPlanNode(_starTreeV2, rootFilterNode,
          null, brokerRequest.getDebugOptions());
    } else {
      starTreeFilterPlanNode = new StarTreeFilterPlanNode(_starTreeV2, rootFilterNode,
          groupByColumnSet, brokerRequest.getDebugOptions());
    }
    List<BlockSingleValIterator> starTreeAggregationColumnValueIterators = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      starTreeAggregationColumnValueIterators.add(
          (BlockSingleValIterator) _starTreeV2.getDataSource(aggregationFunctionColumnPair.toColumnName())
              .nextBlock()
              .getBlockValueSet()
              .iterator());
    }
    List<BlockSingleValIterator> starTreeGroupByColumnValueIterators = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      starTreeGroupByColumnValueIterators.add(
          (BlockSingleValIterator) _starTreeV2.getDataSource(groupByColumn).nextBlock().getBlockValueSet().iterator());
    }
    Map<List<Integer>, List<List<R>>> starTreeResult =
        extractValues(starTreeFilterPlanNode, starTreeAggregationColumnValueIterators, null,
            starTreeGroupByColumnValueIterators);

    // Extract values without star-tree
    PlanNode nonStarTreeFilterPlanNode = new FilterPlanNode(_indexSegment, brokerRequest);
    List<BlockSingleValIterator> nonStarTreeAggregationColumnValueIterators = new ArrayList<>(numAggregations);
    List<Dictionary> nonStarTreeAggregationColumnDictionaries = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      if (aggregationFunctionColumnPair.getFunctionType() == AggregationFunctionType.COUNT) {
        nonStarTreeAggregationColumnValueIterators.add(null);
        nonStarTreeAggregationColumnDictionaries.add(null);
      } else {
        DataSource dataSource = _indexSegment.getDataSource(aggregationFunctionColumnPair.getColumn());
        nonStarTreeAggregationColumnValueIterators.add(
            (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator());
        nonStarTreeAggregationColumnDictionaries.add(dataSource.getDictionary());
      }
    }
    List<BlockSingleValIterator> nonStarTreeGroupByColumnValueIterators = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      nonStarTreeGroupByColumnValueIterators.add((BlockSingleValIterator) _indexSegment.getDataSource(groupByColumn)
          .nextBlock()
          .getBlockValueSet()
          .iterator());
    }
    Map<List<Integer>, List<List<R>>> nonStarTreeResult =
        extractValues(nonStarTreeFilterPlanNode, nonStarTreeAggregationColumnValueIterators,
            nonStarTreeAggregationColumnDictionaries, nonStarTreeGroupByColumnValueIterators);

    // Assert results
    for (Map.Entry<List<Integer>, List<List<R>>> entry : starTreeResult.entrySet()) {
      List<Integer> starTreeGroup = entry.getKey();
      Assert.assertTrue(nonStarTreeResult.containsKey(starTreeGroup));
      List<List<R>> starTreeValues = entry.getValue();
      List<List<R>> nonStarTreeValues = nonStarTreeResult.get(starTreeGroup);
      for (int i = 0; i < numAggregations; i++) {
        assertAggregatedValue(aggregate(starTreeValues.get(i)), aggregate(nonStarTreeValues.get(i)));
      }
    }
  }

  private Map<List<Integer>, List<List<R>>> extractValues(PlanNode planNode,
      List<BlockSingleValIterator> aggregationColumnValueIterators, List<Dictionary> aggregationColumnDictionaries,
      List<BlockSingleValIterator> groupByColumnValueIterators) {
    Map<List<Integer>, List<List<R>>> result = new HashMap<>();
    int numAggregations = aggregationColumnValueIterators.size();
    int numGroupByColumns = groupByColumnValueIterators.size();
    BlockDocIdIterator docIdIterator = planNode.run().nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by queries)
      List<Integer> group = new ArrayList<>(numGroupByColumns);
      for (BlockSingleValIterator valueIterator : groupByColumnValueIterators) {
        valueIterator.skipTo(docId);
        group.add(valueIterator.nextIntVal());
      }
      List<List<R>> values = result.computeIfAbsent(group, k -> {
        List<List<R>> list = new ArrayList<>(numAggregations);
        for (int i = 0; i < numAggregations; i++) {
          list.add(new ArrayList<>());
        }
        return list;
      });
      for (int i = 0; i < numAggregations; i++) {
        BlockSingleValIterator valueIterator = aggregationColumnValueIterators.get(i);

        R value;
        if (valueIterator == null) {
          // COUNT aggregation function
          // noinspection unchecked
          value = (R) NON_STAR_TREE_COUNT_VALUE;
        } else {
          valueIterator.skipTo(docId);
          Dictionary dictionary = aggregationColumnDictionaries != null ? aggregationColumnDictionaries.get(i) : null;
          value = getNextValue(valueIterator, dictionary);
        }
        values.get(i).add(value);
      }
    }
    return result;
  }

  protected abstract R getNextValue(@Nonnull BlockSingleValIterator valueIterator, @Nullable Dictionary dictionary);

  protected abstract A aggregate(@Nonnull List<R> values);

  protected abstract void assertAggregatedValue(A starTreeResult, A nonStarTreeResult);
}