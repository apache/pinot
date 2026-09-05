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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
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
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// This class implements group by aggregation.
/// It is optimized for performance, and uses the best possible algorithm/data-structure for a given query based on the
/// following parameters:
/// - Whether all group-by columns are dictionary encoded.
/// - Maximum number of group keys possible.
/// - Single/Multi valued columns.
///
/// Null handling does not affect the choice: every group key generator gives a null a group of its own when it is
/// enabled.
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
    this(queryContext, queryContext.getAggregationFunctions(), groupByExpressions, projectOperator, null,
        GroupKeyGeneratorProvider.DEFAULT);
  }

  public DefaultGroupByExecutor(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      BaseProjectOperator<?> projectOperator, GroupKeyGeneratorProvider groupKeyGeneratorProvider) {
    this(queryContext, queryContext.getAggregationFunctions(), groupByExpressions, projectOperator, null,
        groupKeyGeneratorProvider);
  }

  public DefaultGroupByExecutor(QueryContext queryContext, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, BaseProjectOperator<?> projectOperator) {
    this(queryContext, aggregationFunctions, groupByExpressions, projectOperator, null,
        GroupKeyGeneratorProvider.DEFAULT);
  }

  public DefaultGroupByExecutor(QueryContext queryContext, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, BaseProjectOperator<?> projectOperator,
      @Nullable GroupKeyGenerator groupKeyGenerator) {
    this(queryContext, aggregationFunctions, groupByExpressions, projectOperator, groupKeyGenerator,
        GroupKeyGeneratorProvider.DEFAULT);
  }

  private DefaultGroupByExecutor(QueryContext queryContext, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, BaseProjectOperator<?> projectOperator,
      @Nullable GroupKeyGenerator groupKeyGenerator, GroupKeyGeneratorProvider groupKeyGeneratorProvider) {
    _aggregationFunctions = aggregationFunctions;
    assert _aggregationFunctions != null;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    Objects.requireNonNull(groupKeyGeneratorProvider);

    boolean hasMVGroupByExpression = false;
    boolean hasNoDictionaryGroupByExpression = false;
    boolean groupingSets = queryContext.isGroupingSets();
    List<GroupKeyGeneratorContext.GroupKeySpec> groupKeySpecs =
        groupKeyGenerator == null && !groupingSets
            && groupKeyGeneratorProvider != GroupKeyGeneratorProvider.DEFAULT
            ? new ArrayList<>(groupByExpressions.length) : null;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpression);
      if (groupKeySpecs != null) {
        DataSource dataSource = columnContext.getDataSource();
        DataType storedType = columnContext.getDataType().getStoredType();
        Optional<GroupKeyGeneratorContext.IntegralDomain> exactIntegralDomain = Optional.empty();
        OptionalInt cardinalityHint = OptionalInt.empty();
        if (dataSource != null) {
          DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
          exactIntegralDomain = getExactIntegralDomain(storedType, dataSourceMetadata);
          int cardinality = dataSourceMetadata.getCardinality();
          if (cardinality >= 0) {
            cardinalityHint = OptionalInt.of(cardinality);
          }
        }
        groupKeySpecs.add(new GroupKeyGeneratorContext.GroupKeySpec(groupByExpression, storedType,
            columnContext.isSingleValue(), columnContext.isDictionaryEncoded(), exactIntegralDomain, cardinalityHint));
      }
      hasMVGroupByExpression |= !columnContext.isSingleValue();
      // A column with EncodingType.RAW + explicit dictionaryIndex has a non-null dictionary but a RAW forward
      // index that throws on readDictIds; route those through the no-dict GROUP BY generator via the explicit
      // isDictionaryEncoded() flag rather than gating on dictionary nullness alone.
      hasNoDictionaryGroupByExpression |= !columnContext.isDictionaryEncoded();
    }
    // Grouping-set queries expand each row into one group per grouping set, so they always use the
    // multi-value (int[][]) executor path even though the union group-by columns are single-valued.
    _hasMVGroupByExpression = hasMVGroupByExpression || groupingSets;

    // Initialize group key generator
    int numGroupsLimit = queryContext.getNumGroupsLimit();
    int maxInitialResultHolderCapacity = queryContext.getMaxInitialResultHolderCapacity();
    Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates =
        queryContext.isOptimizeMaxInitialResultHolderCapacity()
            ? getGroupByExpressionSizesFromPredicates(queryContext, projectOperator) : null;
    GroupKeyGenerator selectedGroupKeyGenerator;
    if (groupKeyGenerator != null) {
      selectedGroupKeyGenerator = groupKeyGenerator;
    } else if (groupingSets) {
      selectedGroupKeyGenerator = new GroupingSetsGroupKeyGenerator(projectOperator, groupByExpressions,
          queryContext.getGroupingSets(), numGroupsLimit, _nullHandlingEnabled);
    } else if (groupKeyGeneratorProvider == GroupKeyGeneratorProvider.DEFAULT) {
      selectedGroupKeyGenerator = createDefaultGroupKeyGenerator(groupByExpressions, projectOperator,
          hasNoDictionaryGroupByExpression, numGroupsLimit, maxInitialResultHolderCapacity,
          groupByExpressionSizesFromPredicates);
    } else {
      GroupKeyGeneratorContext context = createGroupKeyGeneratorContext(Objects.requireNonNull(groupKeySpecs),
          groupByExpressionSizesFromPredicates, numGroupsLimit,
          maxInitialResultHolderCapacity);
      Optional<GroupKeyGenerator> providedGroupKeyGenerator =
          Objects.requireNonNull(groupKeyGeneratorProvider.tryCreate(context));
      selectedGroupKeyGenerator = providedGroupKeyGenerator.isPresent() ? providedGroupKeyGenerator.get()
          : createDefaultGroupKeyGenerator(groupByExpressions, projectOperator, hasNoDictionaryGroupByExpression,
              numGroupsLimit, maxInitialResultHolderCapacity,
              groupByExpressionSizesFromPredicates);
    }

    try {
      // Initialize result holders
      int maxNumResults = selectedGroupKeyGenerator.getGlobalGroupKeyUpperBound();
      int initialCapacity = Math.min(maxNumResults, maxInitialResultHolderCapacity);
      int numAggregationFunctions = _aggregationFunctions.length;
      GroupByResultHolder[] groupByResultHolders = new GroupByResultHolder[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        groupByResultHolders[i] =
            _aggregationFunctions[i].createGroupByResultHolder(initialCapacity, maxNumResults);
      }

      // Initialize map from document Id to group key
      int[] svGroupKeys;
      int[][] mvGroupKeys;
      if (_hasMVGroupByExpression) {
        svGroupKeys = null;
        mvGroupKeys = THREAD_LOCAL_MV_GROUP_KEYS.get();
      } else {
        svGroupKeys = THREAD_LOCAL_SV_GROUP_KEYS.get();
        mvGroupKeys = null;
      }
      _groupKeyGenerator = selectedGroupKeyGenerator;
      _groupByResultHolders = groupByResultHolders;
      _svGroupKeys = svGroupKeys;
      _mvGroupKeys = mvGroupKeys;
    } catch (RuntimeException | Error e) {
      if (groupKeyGenerator == null) {
        try {
          selectedGroupKeyGenerator.close();
        } catch (RuntimeException | Error closeError) {
          if (closeError != e) {
            e.addSuppressed(closeError);
          }
        }
      }
      throw e;
    }
  }

  private GroupKeyGenerator createDefaultGroupKeyGenerator(ExpressionContext[] groupByExpressions,
      BaseProjectOperator<?> projectOperator, boolean hasNoDictionaryGroupByExpression, int numGroupsLimit,
      int maxInitialResultHolderCapacity,
      @Nullable Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates) {
    // Null handling does not steer this choice: every generator gives a null an id of its own, so the physical
    // encoding of the group-by columns decides which built-in implementation to use.
    if (hasNoDictionaryGroupByExpression) {
      if (groupByExpressions.length == 1) {
        return new NoDictionarySingleColumnGroupKeyGenerator(projectOperator, groupByExpressions[0], numGroupsLimit,
            _nullHandlingEnabled, groupByExpressionSizesFromPredicates);
      }
      return new NoDictionaryMultiColumnGroupKeyGenerator(projectOperator, groupByExpressions, numGroupsLimit,
          _nullHandlingEnabled, groupByExpressionSizesFromPredicates);
    }
    return new DictionaryBasedGroupKeyGenerator(projectOperator, groupByExpressions, numGroupsLimit,
        maxInitialResultHolderCapacity, _nullHandlingEnabled, groupByExpressionSizesFromPredicates);
  }

  private GroupKeyGeneratorContext createGroupKeyGeneratorContext(
      List<GroupKeyGeneratorContext.GroupKeySpec> groupKeySpecs,
      @Nullable Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates, int numGroupsLimit,
      int maxInitialResultHolderCapacity) {
    return new GroupKeyGeneratorContext(groupKeySpecs,
        groupByExpressionSizesFromPredicates != null ? groupByExpressionSizesFromPredicates : Map.of(),
        numGroupsLimit, maxInitialResultHolderCapacity, _nullHandlingEnabled);
  }

  private static Optional<GroupKeyGeneratorContext.IntegralDomain> getExactIntegralDomain(DataType dataType,
      DataSourceMetadata dataSourceMetadata) {
    Object minValue = dataSourceMetadata.getMinValue();
    Object maxValue = dataSourceMetadata.getMaxValue();
    if (dataType == DataType.INT && minValue instanceof Integer && maxValue instanceof Integer) {
      int min = (Integer) minValue;
      int max = (Integer) maxValue;
      return min <= max ? Optional.of(new GroupKeyGeneratorContext.IntegralDomain(min, max)) : Optional.empty();
    }
    if (dataType == DataType.LONG && minValue instanceof Long && maxValue instanceof Long) {
      long min = (Long) minValue;
      long max = (Long) maxValue;
      return min <= max ? Optional.of(new GroupKeyGeneratorContext.IntegralDomain(min, max)) : Optional.empty();
    }
    return Optional.empty();
  }

  /// Retrieve the sizes of GroupBy expressions from IN an EQ predicates found in the filter context, if available.
  /// 1. If the filter context is null or lacks GroupBy expressions, return null.
  /// 2. Ensure the top-level filter context consists solely of AND-type filters; other types for example OR we cannot
  ///    guarantee deterministic sizes for GroupBy expressions.
  /// 3. Skip multi-value GroupBy expressions: a row matching an IN/EQ predicate on a multi-value column contributes
  ///    one group per value inside the row (not only the matching values), so the predicate size does not bound the
  ///    number of distinct groups.
  private Map<ExpressionContext, Integer> getGroupByExpressionSizesFromPredicates(QueryContext queryContext,
      BaseProjectOperator<?> projectOperator) {
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
    // NOTE: The merge function handles duplicate group-by expressions (e.g. GROUP BY c0, c0)
    return queryContext.getGroupByExpressions().stream()
        .filter(predicateSizeMap::containsKey)
        .filter(expression -> projectOperator.getResultColumnContext(expression).isSingleValue())
        .collect(Collectors.toMap(
            expression -> expression,
            expression -> predicateSizeMap.getOrDefault(expression, null),
            Integer::min
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
  public List<IntermediateRecord> trimGroupByResult(int trimSize, TableResizer tableResizer, boolean sortedOutput) {
    return tableResizer.trimInSegmentResults(_groupKeyGenerator, _groupByResultHolders, trimSize, sortedOutput);
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
