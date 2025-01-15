/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/data-structure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultGroupByExecutor implements GroupByExecutor {
  // Thread local (reusable) array for single-valued group keys
  private static final ThreadLocal<int[]> THREAD_LOCAL_SV_GROUP_KEYS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  // Thread local (reusable) array for multi-valued group keys
  private static final ThreadLocal<int[][]> THREAD_LOCAL_MV_GROUP_KEYS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][]);

  protected final AggregationFunction[] _aggregationFunctions;
  protected final boolean _nullHandlingEnabled;
  protected final GroupKeyGenerator _groupKeyGenerator;
  protected final GroupByResultHolder[] _groupByResultHolders;
  protected final boolean _hasMVGroupByExpression;
  protected final int[] _svGroupKeys;
  protected final int[][] _mvGroupKeys;

  public DefaultGroupByExecutor(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      BaseProjectOperator<?> projectOperator) {
    this(queryContext, queryContext.getAggregationFunctions(), groupByExpressions, projectOperator, null);
  }

  public DefaultGroupByExecutor(QueryContext queryContext, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, BaseProjectOperator<?> projectOperator) {
    this(queryContext, aggregationFunctions, groupByExpressions, projectOperator, null);
  }

  public DefaultGroupByExecutor(QueryContext queryContext, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, BaseProjectOperator<?> projectOperator,
      @Nullable GroupKeyGenerator groupKeyGenerator) {
    _aggregationFunctions = aggregationFunctions;
    assert _aggregationFunctions != null;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();

    boolean hasMVGroupByExpression = false;
    boolean hasNoDictionaryGroupByExpression = false;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpression);
      hasMVGroupByExpression |= !columnContext.isSingleValue();
      hasNoDictionaryGroupByExpression |= columnContext.getDictionary() == null;
    }
    _hasMVGroupByExpression = hasMVGroupByExpression;

    // Initialize group key generator
    int numGroupsLimit = queryContext.getNumGroupsLimit();
    int maxInitialResultHolderCapacity = queryContext.getMaxInitialResultHolderCapacity();
    Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates = null;
    if (queryContext.getQueryOptions() != null
        && QueryOptionsUtils.optimizeMaxInitialResultHolderCapacityEnabled(queryContext.getQueryOptions())) {
      groupByExpressionSizesFromPredicates = getGroupByExpressionSizesFromPredicates(queryContext);
    }
    if (groupKeyGenerator != null) {
      _groupKeyGenerator = groupKeyGenerator;
    } else {
      if (hasNoDictionaryGroupByExpression || _nullHandlingEnabled) {
        if (groupByExpressions.length == 1) {
          // TODO(nhejazi): support MV and dictionary based when null handling is enabled.
          _groupKeyGenerator =
              new NoDictionarySingleColumnGroupKeyGenerator(projectOperator, groupByExpressions[0], numGroupsLimit,
                  _nullHandlingEnabled, groupByExpressionSizesFromPredicates);
        } else {
          _groupKeyGenerator =
              new NoDictionaryMultiColumnGroupKeyGenerator(projectOperator, groupByExpressions, numGroupsLimit,
                  _nullHandlingEnabled, groupByExpressionSizesFromPredicates);
        }
      } else {
        _groupKeyGenerator = new DictionaryBasedGroupKeyGenerator(projectOperator, groupByExpressions, numGroupsLimit,
            maxInitialResultHolderCapacity, groupByExpressionSizesFromPredicates);
      }
    }

    // Initialize result holders
    int maxNumResults = _groupKeyGenerator.getGlobalGroupKeyUpperBound();
    int initialCapacity = Math.min(maxNumResults, maxInitialResultHolderCapacity);
    int numAggregationFunctions = _aggregationFunctions.length;
    _groupByResultHolders = new GroupByResultHolder[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _groupByResultHolders[i] = _aggregationFunctions[i].createGroupByResultHolder(initialCapacity, maxNumResults);
    }

    // Initialize map from document Id to group key
    if (_hasMVGroupByExpression) {
      _svGroupKeys = null;
      _mvGroupKeys = THREAD_LOCAL_MV_GROUP_KEYS.get();
    } else {
      _svGroupKeys = THREAD_LOCAL_SV_GROUP_KEYS.get();
      _mvGroupKeys = null;
    }
  }

  /**
   * Retrieve the sizes of GroupBy expressions from IN an EQ predicates found in the filter context, if available.
   * 1. If the filter context is null or lacks GroupBy expressions, return null.
   * 2. Ensure the top-level filter context consists solely of AND-type filters; other types for example OR we cannot
   *    guarantee deterministic sizes for GroupBy expressions.
   */
  private Map<ExpressionContext, Integer> getGroupByExpressionSizesFromPredicates(QueryContext queryContext) {
    FilterContext filterContext = queryContext.getFilter();
    if (filterContext == null || queryContext.getGroupByExpressions() == null) {
      return null;
    }

    Set<Predicate> predicateColumns = new HashSet<>();
    if (filterContext.getType() == FilterContext.Type.AND) {
      for (FilterContext child : filterContext.getChildren()) {
        FilterContext.Type type = child.getType();
        if (type != FilterContext.Type.PREDICATE && type != FilterContext.Type.AND) {
          return null;
        } else if (child.getPredicate() != null) {
          predicateColumns.add(child.getPredicate());
        }
      }
    } else if (filterContext.getPredicate() != null) {
      predicateColumns.add(filterContext.getPredicate());
    } else {
      return null;
    }

    // Collect IN and EQ predicates and store their sizes
    Map<ExpressionContext, Integer> predicateSizeMap = predicateColumns.stream()
        .filter(predicate -> predicate.getType() == Predicate.Type.IN || predicate.getType() == Predicate.Type.EQ)
        .collect(Collectors.toMap(
            Predicate::getLhs,
            predicate -> (predicate.getType() == Predicate.Type.IN)
                ? ((InPredicate) predicate).getValues().size()
                : 1,
            Integer::min
        ));

    // Populate the group-by expressions with sizes from the predicate map
    return queryContext.getGroupByExpressions().stream()
        .filter(predicateSizeMap::containsKey)
        .collect(Collectors.toMap(
            expression -> expression,
            expression -> predicateSizeMap.getOrDefault(expression, null)
        ));
  }

  @Override
  public void process(ValueBlock valueBlock) {
    // Generate group keys
    // NOTE: groupKeyGenerator will limit the number of groups. Once reaching limit, no new group will be generated
    if (_hasMVGroupByExpression) {
      _groupKeyGenerator.generateKeysForBlock(valueBlock, _mvGroupKeys);
    } else {
      _groupKeyGenerator.generateKeysForBlock(valueBlock, _svGroupKeys);
    }

    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();
    int length = valueBlock.getNumDocs();
    int numAggregationFunctions = _aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctions; i++) {
      GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
      groupByResultHolder.ensureCapacity(capacityNeeded);
      aggregate(valueBlock, length, i);
    }
  }

  protected void aggregate(ValueBlock valueBlock, int length, int functionIndex) {
    AggregationFunction aggregationFunction = _aggregationFunctions[functionIndex];
    Map<ExpressionContext, BlockValSet> blockValSetMap =
        AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, valueBlock);
    GroupByResultHolder groupByResultHolder = _groupByResultHolders[functionIndex];
    if (_hasMVGroupByExpression) {
      aggregationFunction.aggregateGroupByMV(length, _mvGroupKeys, groupByResultHolder, blockValSetMap);
    } else {
      aggregationFunction.aggregateGroupBySV(length, _svGroupKeys, groupByResultHolder, blockValSetMap);
    }
  }

  @Override
  public AggregationGroupByResult getResult() {
    return new AggregationGroupByResult(_groupKeyGenerator, _aggregationFunctions, _groupByResultHolders);
  }

  @Override
  public int getNumGroups() {
    return _groupKeyGenerator.getNumKeys();
  }

  @Override
  public Collection<IntermediateRecord> trimGroupByResult(int trimSize, TableResizer tableResizer) {
    return tableResizer.trimInSegmentResults(_groupKeyGenerator, _groupByResultHolders, trimSize);
  }

  @Override
  public GroupKeyGenerator getGroupKeyGenerator() {
    return _groupKeyGenerator;
  }

  @Override
  public GroupByResultHolder[] getGroupByResultHolders() {
    return _groupByResultHolders;
  }
}
