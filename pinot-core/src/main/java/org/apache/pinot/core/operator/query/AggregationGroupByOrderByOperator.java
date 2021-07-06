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
package org.apache.pinot.core.operator.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BitmapDocIdSetOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByExecutor;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static org.apache.pinot.core.util.GroupByUtils.getTableCapacity;


/**
 * The <code>AggregationGroupByOrderByOperator</code> class provides the operator for aggregation group-by query on a
 * single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationGroupByOrderByOperator";

  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final OrderByExpressionContext[] _orderByExpressionContexts;
  private final int _limit;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final int _minSegmentTrimSize;
  private final TransformOperator _transformOperator;
  private final long _numTotalDocs;
  private final boolean _useStarTree;
  private final DataSchema _dataSchema;
  private final QueryContext _queryContext;
  private final boolean _enableGroupByOpt;
  private final IndexSegment _indexSegment;
  private int _numDocsScanned = 0;

  public AggregationGroupByOrderByOperator(IndexSegment indexSegment, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, OrderByExpressionContext[] orderByExpressionContexts,
      int maxInitialResultHolderCapacity, int numGroupsLimit, int minSegmentTrimSize,
      TransformOperator transformOperator, long numTotalDocs, QueryContext queryContext, boolean enableGroupByOpt,
      boolean useStarTree) {
    _aggregationFunctions = aggregationFunctions;
    _groupByExpressions = groupByExpressions;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _numTotalDocs = numTotalDocs;
    _useStarTree = useStarTree;
    _queryContext = queryContext;
    _minSegmentTrimSize = minSegmentTrimSize;
    _transformOperator = transformOperator;
    _enableGroupByOpt = enableGroupByOpt;
    _orderByExpressionContexts = orderByExpressionContexts;
    _limit = queryContext.getLimit();
    _indexSegment = indexSegment;
    // NOTE: The indexedTable expects that the the data schema will have group by columns before aggregation columns
    int numGroupByExpressions = groupByExpressions.length;
    int numAggregationFunctions = aggregationFunctions.length;
    int numColumns = numGroupByExpressions + numAggregationFunctions;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    // Extract column names and data types for group-by columns
    for (int i = 0; i < numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
      columnNames[i] = groupByExpression.toString();
      columnDataTypes[i] = DataSchema.ColumnDataType
          .fromDataTypeSV(_transformOperator.getResultMetadata(groupByExpression).getDataType());
    }

    // Extract column names and data types for aggregation functions
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = aggregationFunctions[i];
      int index = numGroupByExpressions + i;
      columnNames[index] = aggregationFunction.getResultColumnName();
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    TransformOperator transformOperator = _enableGroupByOpt ? constructTransformOperator() : _transformOperator;
    // Perform aggregation group-by on all the blocks
    GroupByExecutor groupByExecutor;
    if (_useStarTree) {
      groupByExecutor =
          new StarTreeGroupByExecutor(_aggregationFunctions, _groupByExpressions, _maxInitialResultHolderCapacity,
              _numGroupsLimit, transformOperator);
    } else {
      groupByExecutor =
          new DefaultGroupByExecutor(_aggregationFunctions, _groupByExpressions, _maxInitialResultHolderCapacity,
              _numGroupsLimit, transformOperator);
    }
    TransformBlock transformBlock;
    while ((transformBlock = transformOperator.nextBlock()) != null) {
      _numDocsScanned += transformBlock.getNumDocs();
      groupByExecutor.process(transformBlock);
    }

    int minSegmentTrimSize = calculateMinSegmentTrimSize();
    // There is no OrderBy or minSegmentTrimSize is set to be negative or 0
    if (_orderByExpressionContexts == null || _minSegmentTrimSize <= 0) {
      // Build intermediate result block based on aggregation group-by result from the executor
      return new IntermediateResultsBlock(_aggregationFunctions, groupByExecutor.getResult(), _dataSchema);
    }
    int trimSize = getTableCapacity(_limit, _minSegmentTrimSize);
    // Num of groups hasn't reached the threshold
    if (groupByExecutor.getNumGroups() <= trimSize) {
      return new IntermediateResultsBlock(_aggregationFunctions, groupByExecutor.getResult(), _dataSchema);
    }
    // Trim
    TableResizer tableResizer = new TableResizer(_dataSchema, _queryContext);
    Collection<IntermediateRecord> intermediateRecords = groupByExecutor.trimGroupByResult(trimSize, tableResizer);
    return new IntermediateResultsBlock(_aggregationFunctions, intermediateRecords, _dataSchema);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    //TODO: Determine the correct stats
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  /**
   * In query option, if a positive min trim size is given, we use it to override the server settings. Otherwise
   * check if a simple boolean option is given and use default trim size.
   */
  private int calculateMinSegmentTrimSize() {
    Map<String, String> options = _queryContext.getQueryOptions();
    if (options == null) {
      return _minSegmentTrimSize;
    }
    boolean queryOptionEnableTrim = QueryOptions.isEnableSegmentTrim(options);
    int queryOptionTrimSize = QueryOptions.getMinSegmentTrimSize(options);
    if (queryOptionTrimSize > 0) {
      return queryOptionTrimSize;
    } else if (queryOptionEnableTrim && _minSegmentTrimSize <= 0) {
      return GroupByUtils.DEFAULT_MIN_NUM_GROUPS;
    }
    return _minSegmentTrimSize;
  }

  private TransformOperator constructTransformOperator() {
    List<TransformResultMetadata> orderByExpressionMetadataList = new ArrayList<>();
    for (OrderByExpressionContext orderByExpressionContext : _orderByExpressionContexts) {
      ExpressionContext expression = orderByExpressionContext.getExpression();
      TransformResultMetadata orderByExpressionMetadata = _transformOperator.getResultMetadata(expression);
      // Only handle single value column now
      if (!orderByExpressionMetadata.isSingleValue()) {
        return _transformOperator;
      }
      orderByExpressionMetadataList.add(orderByExpressionMetadata);
    }
    return constructNewTransformOperator(orderByExpressionMetadataList.toArray(new TransformResultMetadata[0]));
  }

  /**
   * Two pass approach for orderBy on groupBy columns. Fetch the orderBy columns to rank the top results
   * whose docIds will be used to construct a new transform operator for aggregations.
   */
  private TransformOperator constructNewTransformOperator(TransformResultMetadata[] orderByExpressionMetadata) {
    int numOrderByExpressions = _orderByExpressionContexts.length;
    HashMap<Key, MutableRoaringBitmap> groupByKeyMap = new HashMap<>();
    TransformBlock transformBlock;

    Dictionary[] dictionaries = new Dictionary[numOrderByExpressions];
    boolean[] hasDict = new boolean[numOrderByExpressions];
    int numNoDict = 0;
    long cardinalityProduct = 1L;
    boolean longOverflow = false;
    // Get dictionaries and calculate cardinalities
    for (int i = 0; i < numOrderByExpressions; i++) {
      ExpressionContext expression = _orderByExpressionContexts[i].getExpression();
      hasDict[i] = orderByExpressionMetadata[i].hasDictionary();
      if (hasDict[i]) {
        dictionaries[i] = _transformOperator.getDictionary(expression);
        int cardinality = dictionaries[i].length();
        if (!longOverflow) {
          if (cardinalityProduct > Long.MAX_VALUE / cardinality) {
            longOverflow = true;
          } else {
            cardinalityProduct *= cardinality;
          }
        }
      }
      numNoDict += hasDict[i] ? 0 : 1;
    }
    //TODO: Determine reasonable threshold
    if (!longOverflow && cardinalityProduct < _limit || cardinalityProduct < 500000) {
      return _transformOperator;
    }
    BlockValSet[] blockValSets = new BlockValSet[numNoDict];
    PriorityQueue<Object[]> PQ = new PriorityQueue<>(_limit,
        getComparator(orderByExpressionMetadata, numOrderByExpressions, dictionaries, hasDict));
    int[][] dictionaryIds = new int[numOrderByExpressions - numNoDict][];
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      int numDocsFetched = transformBlock.getNumDocs();
      int[] docIds = transformBlock.getBlockValueSet("$docId").getIntValuesSV();
      int dictionaryIdsIndex = 0;
      // For dictionary-based columns, we fetch the dictionary ids. Otherwise fetch the actual value
      for (int i = 0; i < numOrderByExpressions; i++) {
        ExpressionContext expression = _orderByExpressionContexts[i].getExpression();
        BlockValSet blockValSet = transformBlock.getBlockValueSet(expression);
        if (hasDict[i]) {
          dictionaryIds[dictionaryIdsIndex] = blockValSet.getDictionaryIdsSV();
          dictionaryIdsIndex++;
        } else {
          blockValSets[i - dictionaryIdsIndex] = blockValSet;
        }
      }
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      // TODO: Add special optimization for all dict condition
      for (int i = 0; i < numDocsFetched; i++) {
        int docId = docIds[i];
        // Generate key based on the dictionary Id/fetched values
        Object[] keys = new Object[numOrderByExpressions];
        Object[] row = new Object[numNoDict];
        if (numNoDict != 0) {
          blockValueFetcher.getRow(i, row, 0);
        }
        dictionaryIdsIndex = 0;
        for (int j = 0; j < numOrderByExpressions; j++) {
          if (hasDict[j]) {
            keys[j] = dictionaryIds[dictionaryIdsIndex][i];
            dictionaryIdsIndex++;
          } else {
            keys[j] = row[j - dictionaryIdsIndex];
          }
        }
        AddToObjectPriorityQueue(keys, docId, PQ, groupByKeyMap);
      }
    }
    // Collect docIds
    Collection<MutableRoaringBitmap> docIdList = groupByKeyMap.values();
    int numDocs = 0;
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    for (MutableRoaringBitmap filteredDocIds : docIdList) {
      for (Integer docId : filteredDocIds) {
        docIds.add(docId);
        numDocs++;
      }
    }

    // Make a new transform operator
    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(_aggregationFunctions, _groupByExpressions);
    Set<String> columns = new HashSet<>();
    for (ExpressionContext expression : expressionsToTransform) {
      expression.getColumns(columns);
    }
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    for (String column : columns) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
    }
    ProjectionOperator projectionOperator =
        new ProjectionOperator(dataSourceMap, new BitmapDocIdSetOperator(docIds, numDocs));

    return new TransformOperator(projectionOperator, expressionsToTransform);
  }

  private Comparator<Object[]> getComparator(TransformResultMetadata[] orderByExpressionMetadata,
      int numOrderByExpressions, Dictionary[] dictionaries, boolean[] hasDict) {
    // Compare all single-value columns
    FieldSpec.DataType[] storedTypes = new FieldSpec.DataType[numOrderByExpressions];
    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[numOrderByExpressions];
    for (int i = 0; i < numOrderByExpressions; i++) {
      storedTypes[i] = orderByExpressionMetadata[i].getDataType().getStoredType();
      multipliers[i] = _orderByExpressionContexts[i].isAsc() ? -1 : 1;
    }

    return (o1, o2) -> {
      for (int i = 0; i < numOrderByExpressions; i++) {
        // TODO: Handle orderBy funcions
        // TODO: Evaluate the performance of casting to Comparable and avoid the switch
        Object v1 = o1[i];
        Object v2 = o2[i];
        int result;
        if (hasDict[i]) {
          result = dictionaries[i].compare((int) v1, (int) v2);
          if (result != 0) {
            return result * multipliers[i];
          } else {
            continue;
          }
        }
        switch (storedTypes[i]) {
          case INT:
            result = ((Integer) v1).compareTo((Integer) v2);
            break;
          case LONG:
            result = ((Long) v1).compareTo((Long) v2);
            break;
          case FLOAT:
            result = ((Float) v1).compareTo((Float) v2);
            break;
          case DOUBLE:
            result = ((Double) v1).compareTo((Double) v2);
            break;
          case STRING:
            result = ((String) v1).compareTo((String) v2);
            break;
          case BYTES:
            result = ((ByteArray) v1).compareTo((ByteArray) v2);
            break;
          // NOTE: Multi-value columns are not comparable, so we should not reach here
          default:
            throw new IllegalStateException();
        }
        if (result != 0) {
          return result * multipliers[i];
        }
      }
      return 0;
    };
  }

  /**
   * This function first checks if the group key present in the hash map, and add docId to its
   * bit map if there is existing entry. Otherwise create a new entry for the hash map and priority
   * queue
   */
  private void AddToObjectPriorityQueue(Object[] row, int docId, PriorityQueue<Object[]> rows,
      HashMap<Key, MutableRoaringBitmap> groupByKeyMap) {
    if (rows.size() < _limit) {
      Key groupByKeys = new Key(row);
      if (AddToKeyMap(groupByKeys, docId, groupByKeyMap)) {
        rows.add(row);
      }
    } else {
      int compareResult = rows.comparator().compare(rows.peek(), row);
      if (compareResult < 0) {
        Key groupByKeys = new Key(row);
        if (AddToKeyMap(groupByKeys, docId, groupByKeyMap)) {
          Object[] removedRow = rows.poll();
          Key removedGroupByKey = new Key(removedRow);
          groupByKeyMap.remove(removedGroupByKey);
          rows.offer(row);
        }
      } else if (compareResult == 0) {
        Key groupByKeys = new Key(row);
        MutableRoaringBitmap docIdMap = groupByKeyMap.get(groupByKeys);
        if (docIdMap != null) {
          docIdMap.add(docId);
          groupByKeyMap.put(groupByKeys, docIdMap);
        }
      }
    }
  }

  private boolean AddToKeyMap(Key groupByKeys, int docID, HashMap<Key, MutableRoaringBitmap> groupByKeyMap) {
    MutableRoaringBitmap docIdMap = groupByKeyMap.get(groupByKeys);
    if (docIdMap == null) {
      docIdMap = new MutableRoaringBitmap();
      docIdMap.add(docID);
      groupByKeyMap.put(groupByKeys, docIdMap);
      return true;
    } else {
      docIdMap.add(docID);
      groupByKeyMap.put(groupByKeys, docIdMap);
      return false;
    }
  }
}
