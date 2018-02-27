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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/data-structure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
public class DefaultGroupByExecutor implements GroupByExecutor {
  // Thread local (reusable) array for dict id to group key mapping.
  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_ID_TO_GROUP_KEY = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  };

  // Thread local (reusable) array for dict id to MV group key mapping.
  private static final ThreadLocal<int[][]> THREAD_LOCAL_DICT_ID_TO_MV_GROUP_KEY = new ThreadLocal<int[][]>() {
    @Override
    protected int[][] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
  };

  private static final double GROUP_BY_TRIM_FACTOR = 0.9;
  private final int _numAggrFunc;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final AggregationFunctionContext[] _aggrFunctionContexts;
  private final AggregationFunction[] _aggregationFunctions;

  private GroupKeyGenerator _groupKeyGenerator;
  private GroupByResultHolder[] _resultHolderArray;
  private final String[] _groupByColumns;

  private int[] _docIdToSVGroupKey;
  private int[][] _docIdToMVGroupKey;

  private boolean _hasMVGroupByColumns = false;
  private boolean _inited = false; // boolean to ensure init() has been called.
  private boolean _finished = false; // boolean to ensure that finish() has been called.
  private boolean _groupByInited = false; // boolean for lazy creation of group-key generator etc.
  private boolean _hasColumnsWithoutDictionary = false;

  /**
   * Constructor for the class.
   * @param aggrFunctionContexts Array of aggregation functions
   * @param groupBy Group by from broker request
   * @param maxInitialResultHolderCapacity Maximum initial capacity for the result holder
   * @param numGroupsLimit Limit on number of aggregation groups returned in the result
   */
  public DefaultGroupByExecutor(@Nonnull AggregationFunctionContext[] aggrFunctionContexts, GroupBy groupBy,
      int maxInitialResultHolderCapacity, int numGroupsLimit) {
    Preconditions.checkNotNull(aggrFunctionContexts.length > 0);
    Preconditions.checkNotNull(groupBy);

    List<String> groupByColumns = groupBy.getColumns();
    List<String> groupByExpressions = groupBy.getExpressions();

    // Expressions contain simple group by columns (ie without any transform) as well.
    if (groupByExpressions != null && !groupByExpressions.isEmpty()) {
      _groupByColumns = groupByExpressions.toArray(new String[groupByExpressions.size()]);
    } else {
      _groupByColumns = groupByColumns.toArray(new String[groupByColumns.size()]);
    }

    _numAggrFunc = aggrFunctionContexts.length;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;

    // TODO: revisit the trim factor. Usually the factor should be 5-10, and based on the 'TOP' limit.
    // When results are trimmed, drop bottom 10% of groups.
    _numGroupsLimit = (int) (GROUP_BY_TRIM_FACTOR * numGroupsLimit);

    _aggrFunctionContexts = aggrFunctionContexts;
    _aggregationFunctions = new AggregationFunction[_numAggrFunc];
    for (int i = 0; i < _numAggrFunc; i++) {
      _aggregationFunctions[i] = aggrFunctionContexts[i].getAggregationFunction();
    }
  }

  /**
   * {@inheritDoc}
   * No-op for this implementation of GroupKeyGenerator. Most initialization happens lazily
   * in process(), as a transform is required to initialize group key generator, etc.
   */
  @Override
  public void init() {
    // Returned if already initialized.
    if (_inited) {
      return;
    }

    _inited = true;
  }

  /**
   * Process the provided set of docId's to perform the requested aggregation-group-by-operation.
   *
   * @param transformBlock Transform block to process
   */
  @Override
  public void process(TransformBlock transformBlock) {
    Preconditions.checkState(_inited,
        "Method 'process' cannot be called before 'init' for class " + getClass().getName());

    initGroupBy(transformBlock);
    generateGroupKeysForBlock(transformBlock);
    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();

    for (int i = 0; i < _numAggrFunc; i++) {
      _resultHolderArray[i].ensureCapacity(capacityNeeded);
      aggregateColumn(transformBlock, _aggrFunctionContexts[i], _resultHolderArray[i]);

      // Result holder limits the max number of group keys (default 100k), if the number of groups
      // exceeds beyond that limit, groups with lower values (as per sort order) are trimmed.
      // Once result holder trims those groups, the group key generator needs to purge them.
      if (!_hasColumnsWithoutDictionary) {
        int[] trimmedKeys = _resultHolderArray[i].trimResults();
        _groupKeyGenerator.purgeKeys(trimmedKeys);
      }
    }
  }

  /**
   * Helper method to perform aggregation for a given column.
   *
   * @param transformBlock Transform block to aggregate
   * @param aggrFuncContext Aggregation function context
   * @param resultHolder Holder for results of aggregation
   */
  @SuppressWarnings("ConstantConditions")
  private void aggregateColumn(TransformBlock transformBlock, AggregationFunctionContext aggrFuncContext,
      GroupByResultHolder resultHolder) {
    AggregationFunction aggregationFunction = aggrFuncContext.getAggregationFunction();
    String[] aggregationColumns = aggrFuncContext.getAggregationColumns();
    Preconditions.checkState(aggregationColumns.length == 1);
    int length = transformBlock.getNumDocs();

    if (!aggregationFunction.getName().equals(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(aggregationColumns[0]);
      if (_hasMVGroupByColumns) {
        aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, blockValueSet);
      } else {
        aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, blockValueSet);
      }
    } else {
      if (_hasMVGroupByColumns) {
        aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder);
      } else {
        aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void finish() {
    Preconditions.checkState(_inited,
        "Method 'finish' cannot be called before 'init' for class " + getClass().getName());

    _finished = true;
  }

  /**
   * Return the final result of the aggregation-group-by operation.
   * This method should be called after all docIdSets have been 'processed'.
   *
   * @return Results of aggregation group by.
   */
  @Override
  public AggregationGroupByResult getResult() {
    Preconditions.checkState(_finished,
        "Method 'getResult' cannot be called before 'finish' for class " + getClass().getName());

    // If group by was not initialized (in case of no transform blocks), return null.
    if (!_groupByInited) {
      return null;
    }

    return new AggregationGroupByResult(_groupKeyGenerator, _aggregationFunctions, _resultHolderArray);
  }

  /**
   * Generate group keys for the given docIdSet. For single valued columns, each docId has one group key,
   * but for multi-valued columns, each docId could have more than one group key.
   *
   * For SV keys: _docIdToSVGroupKey mapping is updated.
   * For MV keys: _docIdToMVGroupKey mapping is updated.
   *
   * @param transformBlock Transform block for which to generate group keys
   */
  private void generateGroupKeysForBlock(TransformBlock transformBlock) {
    if (_hasMVGroupByColumns) {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _docIdToMVGroupKey);
    } else {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _docIdToSVGroupKey);
    }
  }

  /**
   * Helper method to initialize result holder array.
   *
   * @param trimSize Trim size for group by keys
   * @param maxNumResults Maximum number of groups possible
   */
  private void initResultHolderArray(int trimSize, int maxNumResults) {
    _resultHolderArray = new GroupByResultHolder[_numAggrFunc];
    int initialCapacity = Math.min(maxNumResults, _maxInitialResultHolderCapacity);
    for (int i = 0; i < _numAggrFunc; i++) {
      _resultHolderArray[i] = _aggrFunctionContexts[i].getAggregationFunction()
          .createGroupByResultHolder(initialCapacity, maxNumResults, trimSize);
    }
  }

  /**
   * Allocate storage for docId to group keys mapping.
   */
  private void initDocIdToGroupKeyMap() {
    if (_hasMVGroupByColumns) {
      // TODO: Revisit block fetching of multi-valued columns
      _docIdToMVGroupKey = THREAD_LOCAL_DICT_ID_TO_MV_GROUP_KEY.get();
    } else {
      _docIdToSVGroupKey = THREAD_LOCAL_DICT_ID_TO_GROUP_KEY.get();
    }
  }

  /**
   * Initializes the following:
   * <p> - Group key generator. </p>
   * <p> - Result holders </p>
   * <p> - Re-usable storage (eg docId to group key mapping) </p>
   *
   * This is separate from init(), as this can only happen within process as transform block is
   * required to create group key generator.
   *
   * @param transformBlock Transform block to group by.
   */
  private void initGroupBy(TransformBlock transformBlock) {
    if (_groupByInited) {
      return;
    }

    FieldSpec.DataType dataType = null;
    for (String groupByColumn : _groupByColumns) {
      BlockMetadata metadata = transformBlock.getBlockMetadata(groupByColumn);

      if (!metadata.isSingleValue()) {
        _hasMVGroupByColumns = true;
      }

      if (!metadata.hasDictionary()) {
        _hasColumnsWithoutDictionary = true;
      }

      // Used only for single group-by case, so ok to overwrite.
      dataType = metadata.getDataType();
    }

    if (_hasColumnsWithoutDictionary) {
      if (_groupByColumns.length == 1) {
        _groupKeyGenerator = new NoDictionarySingleColumnGroupKeyGenerator(_groupByColumns[0], dataType);
      } else {
        _groupKeyGenerator = new NoDictionaryMultiColumnGroupKeyGenerator(transformBlock, _groupByColumns);
      }
    } else {
      _groupKeyGenerator =
          new DictionaryBasedGroupKeyGenerator(transformBlock, _groupByColumns, _maxInitialResultHolderCapacity);
    }

    int maxNumResults = _groupKeyGenerator.getGlobalGroupKeyUpperBound();
    initResultHolderArray(_numGroupsLimit, maxNumResults);
    initDocIdToGroupKeyMap();
    _groupByInited = true;
  }
}
