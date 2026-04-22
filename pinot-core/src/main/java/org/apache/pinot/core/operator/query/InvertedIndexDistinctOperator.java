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

import com.google.common.base.CaseFormat;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorFactory;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
import org.apache.pinot.core.query.distinct.table.DictIdDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.Pairs;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Inverted-index-based operator for single-column distinct queries on a single segment.
 *
 * <p>Supports three execution paths, chosen at runtime:
 * <ul>
 *   <li><b>Sorted index path</b>: For sorted columns, merge-iterates filter bitmap against contiguous doc ranges.
 *       Cost ~ O(cardinality + filteredDocs). Always chosen when the column has a sorted forward index.</li>
 *   <li><b>Bitmap inverted index path</b>: Iterates dictionary entries and uses inverted index bitmap intersections
 *       to check filter membership. Avoids the projection pipeline entirely. Chosen by cost heuristic when dictionary
 *       cardinality is much smaller than the filtered doc count.</li>
 *   <li><b>Scan path (fallback)</b>: Uses ProjectOperator + DistinctExecutor to scan filtered docs.
 *       Used when the cost heuristic determines scanning is cheaper.</li>
 * </ul>
 *
 * <p>Enabled via the {@code useIndexBasedDistinctOperator} query option. The cost ratio can be tuned
 * via the {@code invertedIndexDistinctCostRatio} query option.
 */
public class InvertedIndexDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_INVERTED_INDEX";
  private static final String EXPLAIN_NAME_SORTED_INDEX = "DISTINCT_SORTED_INDEX";
  private static final String EXPLAIN_NAME_SCAN_FALLBACK = "DISTINCT";

  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;
  private final DataSource _dataSource;
  private final Dictionary _dictionary;
  private final InvertedIndexReader<?> _invertedIndexReader;

  // Scan path: created lazily when scan fallback is chosen
  private BaseProjectOperator<?> _projectOperator;

  // Execution tracking
  private boolean _usedInvertedIndexPath = false;
  private int _numDocsScanned = 0;
  private int _numEntriesExamined = 0;
  private long _numEntriesScannedInFilter = 0;

  /**
   * Creates an InvertedIndexDistinctOperator. The caller (DistinctPlanNode) must verify that the column
   * has both a dictionary and an inverted index before constructing this operator.
   */
  public InvertedIndexDistinctOperator(IndexSegment indexSegment, SegmentContext segmentContext,
      QueryContext queryContext, BaseFilterOperator filterOperator, DataSource dataSource) {
    _indexSegment = indexSegment;
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _filterOperator = filterOperator;
    _dataSource = dataSource;
    _dictionary = dataSource.getDictionary();
    _invertedIndexReader = dataSource.getInvertedIndex();
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    // Sorted index: always use the sorted path — O(cardinality + filteredDocs) merge iteration
    if (_invertedIndexReader instanceof SortedIndexReader) {
      ImmutableRoaringBitmap filteredDocIds = buildFilteredDocIds();
      _usedInvertedIndexPath = true;
      return executeSortedIndexPath((SortedIndexReader<?>) _invertedIndexReader, filteredDocIds);
    }

    // Prefer cheap count-only inputs for the heuristic so scan fallback can keep the original filter pipeline.
    FilterPreparation filterPreparation = prepareBitmapPathInput();
    Integer filteredDocCount = filterPreparation.getFilteredDocCount();

    if (filteredDocCount != null) {
      if (filteredDocCount == 0) {
        return createEmptyResultsBlock();
      }
      // Bitmap inverted index: use cost heuristic to decide
      if (shouldUseBitmapInvertedIndex(filteredDocCount)) {
        ImmutableRoaringBitmap filteredDocIds = filterPreparation.getFilteredDocIds();
        if (filteredDocIds == null) {
          filteredDocIds = buildFilteredDocIds();
        }
        _usedInvertedIndexPath = true;
        return executeInvertedIndexPath(filteredDocIds);
      }
    }
    return executeScanPath(filterPreparation.getFilteredDocIds());
  }

  // ==================== Cost Heuristic ====================

  /**
   * Default cost ratios for the inverted-index-based distinct heuristic, keyed by dictionary cardinality threshold.
   * The inverted index path is chosen when {@code dictionaryCardinality * costRatio <= filteredDocCount}.
   *
   * <p>The cost ratio accounts for the per-entry bitmap intersection cost relative to the per-doc scan cost.
   * For low-cardinality dictionaries, each bitmap is dense and {@code intersects()} is fast, but there are few
   * entries so any unnecessary intersection is relatively expensive vs. scanning a small filtered doc set.
   * For high-cardinality dictionaries, bitmaps are sparser and {@code intersects()} is slower per entry,
   * but the scan path also becomes cheaper (fewer docs per value), so a lower ratio suffices.
   *
   * <p>Benchmarking (BenchmarkInvertedIndexDistinct, 1M docs) shows the crossover points:
   * <ul>
   *   <li>dictCard &le; 1K:  costRatio=30 — inverted index wins when filteredDocs &ge; ~30x dictCard</li>
   *   <li>dictCard &le; 10K: costRatio=10 — inverted index wins when filteredDocs &ge; ~10x dictCard</li>
   *   <li>dictCard &gt; 10K: costRatio=6  — inverted index wins when filteredDocs &ge; ~6x dictCard</li>
   * </ul>
   *
   * <p>Can be overridden at query time via the query option {@code invertedIndexDistinctCostRatio}.
   */
  static final NavigableMap<Integer, Double> DEFAULT_COST_RATIO_BY_CARDINALITY;

  static {
    TreeMap<Integer, Double> map = new TreeMap<>();
    map.put(0, 30.0);       // dictCard <= 1000: costRatio = 30
    map.put(1_001, 10.0);   // dictCard 1001..10000: costRatio = 10
    map.put(10_001, 6.0);   // dictCard > 10000: costRatio = 6
    DEFAULT_COST_RATIO_BY_CARDINALITY = Collections.unmodifiableNavigableMap(map);
  }

  static double getDefaultCostRatio(int dictionaryCardinality) {
    return DEFAULT_COST_RATIO_BY_CARDINALITY.floorEntry(dictionaryCardinality).getValue();
  }

  private boolean shouldUseBitmapInvertedIndex(int filteredDocCount) {
    int dictionaryCardinality = _dictionary.length();
    if (filteredDocCount == 0) {
      return false;
    }
    Double costRatioOverride = QueryOptionsUtils.getInvertedIndexDistinctCostRatio(_queryContext.getQueryOptions());
    double costRatio = costRatioOverride != null ? costRatioOverride : getDefaultCostRatio(dictionaryCardinality);
    return (double) dictionaryCardinality * costRatio <= filteredDocCount;
  }

  static final class FilterPreparation {
    @Nullable
    private final ImmutableRoaringBitmap _filteredDocIds;
    @Nullable
    private final Integer _filteredDocCount;

    private FilterPreparation(@Nullable ImmutableRoaringBitmap filteredDocIds, @Nullable Integer filteredDocCount) {
      _filteredDocIds = filteredDocIds;
      _filteredDocCount = filteredDocCount;
    }

    @Nullable
    ImmutableRoaringBitmap getFilteredDocIds() {
      return _filteredDocIds;
    }

    @Nullable
    Integer getFilteredDocCount() {
      return _filteredDocCount;
    }
  }

  FilterPreparation prepareBitmapPathInput() {
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    if (_filterOperator.isResultMatchingAll()) {
      return new FilterPreparation(null, totalDocs);
    }
    if (_filterOperator.isResultEmpty()) {
      return new FilterPreparation(new MutableRoaringBitmap(), 0);
    }
    // Prefer the cheaper exact count when available so scan fallback does not pay eager bitmap materialization.
    if (_filterOperator.canOptimizeCount()) {
      return new FilterPreparation(null, _filterOperator.getNumMatchingDocs());
    }
    if (_filterOperator.canProduceBitmaps()) {
      ImmutableRoaringBitmap filteredDocIds = _filterOperator.getBitmaps().reduce();
      return new FilterPreparation(filteredDocIds, filteredDocIds.getCardinality());
    }
    return new FilterPreparation(null, null);
  }

  // ==================== Scan Path (Fallback) ====================

  /**
   * Scan fallback: uses ProjectOperator + DistinctExecutor. When an exact filter bitmap is already cheaply available,
   * wraps it in a {@link BitmapBasedFilterOperator} to avoid re-evaluating the filter through the projection pipeline.
   * Otherwise preserves the original filter operator so scan fallback does not pay eager bitmap materialization.
   */
  private DistinctResultsBlock executeScanPath(@Nullable ImmutableRoaringBitmap filteredDocIds) {
    BaseFilterOperator filterOp;
    if (filteredDocIds != null) {
      filterOp = new BitmapBasedFilterOperator(filteredDocIds, false,
          _indexSegment.getSegmentMetadata().getTotalDocs());
    } else {
      filterOp = _filterOperator;
    }
    _projectOperator = new ProjectPlanNode(_segmentContext, _queryContext,
        _queryContext.getSelectExpressions(), DocIdSetPlanNode.MAX_DOC_PER_CALL, filterOp).run();
    DistinctExecutor executor = DistinctExecutorFactory.getDistinctExecutor(_projectOperator, _queryContext);
    ValueBlock valueBlock;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      _numDocsScanned += valueBlock.getNumDocs();
      if (executor.process(valueBlock)) {
        break;
      }
    }
    return new DistinctResultsBlock(executor.getResult(), _queryContext);
  }

  // ==================== Sorted Index Path ====================

  /**
   * Optimized path for sorted columns. Each dictId maps to a contiguous doc range [start, end].
   * We merge-iterate the filter bitmap with the sorted ranges in O(cardinality + filteredDocs).
   */
  private DistinctResultsBlock executeSortedIndexPath(SortedIndexReader<?> sortedReader,
      @Nullable ImmutableRoaringBitmap filteredDocIds) {
    OrderByExpressionContext orderByExpression =
        _queryContext.getOrderByExpressions() != null ? _queryContext.getOrderByExpressions().get(0) : null;
    boolean useDictIdTable = canUseDictIdDistinctTable(orderByExpression);
    DistinctTable distinctTable =
        useDictIdTable ? createDictIdDistinctTable(orderByExpression) : createTypedDistinctTable(orderByExpression);
    int dictLength = _dictionary.length();
    // Process null handling: exclude null docs from filter and determine if nulls are present
    NullFilterResult nullResult = processNullDocs(filteredDocIds);
    ImmutableRoaringBitmap nonNullFilteredDocIds = nullResult._nonNullFilteredDocIds;
    if (nullResult._hasNull) {
      distinctTable.addNull();
    }

    // When dictIds are in value order, ORDER BY + LIMIT can terminate early by iterating in the ORDER BY direction.
    boolean orderedEarlyTermination = useDictIdTable && orderByExpression != null && distinctTable.hasLimit();
    boolean iterateReverse = orderedEarlyTermination && !orderByExpression.isAsc();

    if (nonNullFilteredDocIds == null) {
      // No filter, no null exclusion — every dictionary value is present
      int entriesExamined = 0;
      int start = iterateReverse ? dictLength - 1 : 0;
      int end = iterateReverse ? -1 : dictLength;
      int step = iterateReverse ? -1 : 1;
      for (int dictId = start; dictId != end; dictId += step) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(entriesExamined, EXPLAIN_NAME_SORTED_INDEX);
        entriesExamined++;
        if (dictId == nullResult._nullPlaceholderDictId) {
          continue;
        }
        boolean done = addDistinctValue(distinctTable, dictId, orderByExpression, orderedEarlyTermination);
        if (done) {
          break;
        }
      }
      _numEntriesExamined = entriesExamined;
    } else if (!nonNullFilteredDocIds.isEmpty()) {
      if (iterateReverse) {
        // DESC + LIMIT: iterate dictIds backward, use rangeCardinality for presence check.
        // Each dictId maps to a contiguous doc range, so rangeCardinality is O(1) per check.
        int entriesExamined = 0;
        for (int dictId = dictLength - 1; dictId >= 0; dictId--) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(entriesExamined, EXPLAIN_NAME_SORTED_INDEX);
          entriesExamined++;
          Pairs.IntPair range = sortedReader.getDocIds(dictId);
          int startDocId = range.getLeft();
          int endDocId = range.getRight(); // inclusive
          if (nonNullFilteredDocIds.rangeCardinality(startDocId, endDocId + 1L) > 0) {
            if (addDistinctValue(distinctTable, dictId, orderByExpression, true)) {
              break;
            }
          }
        }
        _numEntriesExamined = entriesExamined;
      } else {
        // ASC or no ORDER BY: merge-iterate forward (O(cardinality + filteredDocs))
        PeekableIntIterator filterIter = nonNullFilteredDocIds.getIntIterator();
        int dictId;
        for (dictId = 0; dictId < dictLength && filterIter.hasNext(); dictId++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, EXPLAIN_NAME_SORTED_INDEX);
          Pairs.IntPair range = sortedReader.getDocIds(dictId);
          int startDocId = range.getLeft();
          int endDocId = range.getRight(); // inclusive

          // Skip filter docs before this range
          filterIter.advanceIfNeeded(startDocId);

          // Check if any non-null filter doc falls within this range
          if (filterIter.hasNext() && filterIter.peekNext() <= endDocId) {
            boolean done = addDistinctValue(distinctTable, dictId, orderByExpression, orderedEarlyTermination);
            if (done) {
              _numEntriesExamined = dictId + 1;
              return new DistinctResultsBlock(convertDistinctTable(distinctTable, nullResult._hasNull), _queryContext);
            }
            // Advance past the current range for next dictId
            filterIter.advanceIfNeeded(endDocId + 1);
          }
        }
        _numEntriesExamined = dictId;
      }
    }

    return new DistinctResultsBlock(convertDistinctTable(distinctTable, nullResult._hasNull), _queryContext);
  }

  // ==================== Bitmap Inverted Index Path ====================

  private DistinctResultsBlock executeInvertedIndexPath(@Nullable ImmutableRoaringBitmap filteredDocIds) {
    // Process null handling: exclude null docs from filter and determine if nulls are present
    NullFilterResult nullResult = processNullDocs(filteredDocIds);
    ImmutableRoaringBitmap nonNullFilteredDocIds = nullResult._nonNullFilteredDocIds;
    OrderByExpressionContext orderByExpression =
        _queryContext.getOrderByExpressions() != null ? _queryContext.getOrderByExpressions().get(0) : null;
    boolean useDictIdTable = canUseDictIdDistinctTable(orderByExpression);
    DistinctTable distinctTable =
        useDictIdTable ? createDictIdDistinctTable(orderByExpression) : createTypedDistinctTable(orderByExpression);
    int dictLength = _dictionary.length();
    if (nullResult._hasNull) {
      distinctTable.addNull();
    }

    // When dictIds are in value order, ORDER BY + LIMIT can terminate early by iterating in the ORDER BY direction.
    boolean orderedEarlyTermination = useDictIdTable && orderByExpression != null && distinctTable.hasLimit();
    boolean iterateReverse = orderedEarlyTermination && !orderByExpression.isAsc();

    int entriesExamined = 0;
    int start = iterateReverse ? dictLength - 1 : 0;
    int end = iterateReverse ? -1 : dictLength;
      int step = iterateReverse ? -1 : 1;

    for (int dictId = start; dictId != end; dictId += step) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(entriesExamined, EXPLAIN_NAME);
      entriesExamined++;
      if (dictId == nullResult._nullPlaceholderDictId) {
        continue;
      }

      // SortedIndexReader is handled separately in getNextBlock(), so this path only sees bitmap inverted indexes
      // whose getDocIds() returns ImmutableRoaringBitmap.
      ImmutableRoaringBitmap docIds = (ImmutableRoaringBitmap) _invertedIndexReader.getDocIds(dictId);

      // Use intersects() for early termination instead of computing full intersection.
      // intersects() short-circuits on the first common element, which is orders of magnitude
      // faster than RoaringBitmap.and() especially for low-cardinality (dense) bitmaps.
      // Use the non-null filter bitmap to skip the null placeholder value.
      boolean includeValue;
      if (nonNullFilteredDocIds == null) {
        includeValue = true;
      } else {
        includeValue = ImmutableRoaringBitmap.intersects(docIds, nonNullFilteredDocIds);
      }

      if (includeValue) {
        boolean done = addDistinctValue(distinctTable, dictId, orderByExpression, orderedEarlyTermination);
        if (done) {
          break;
        }
      }
    }
    _numEntriesExamined = entriesExamined;

    return new DistinctResultsBlock(convertDistinctTable(distinctTable, nullResult._hasNull), _queryContext);
  }

  @Nullable
  private ImmutableRoaringBitmap buildFilteredDocIds() {
    BaseFilterOperator.FilteredDocIds filteredDocIds = _filterOperator.getFilteredDocIds();
    _numEntriesScannedInFilter = filteredDocIds.getNumEntriesScannedInFilter();
    return filteredDocIds.getDocIds();
  }

  private boolean canUseDictIdDistinctTable(@Nullable OrderByExpressionContext orderByExpression) {
    return orderByExpression == null || _dictionary.isSorted();
  }

  private DistinctResultsBlock createEmptyResultsBlock() {
    OrderByExpressionContext orderByExpression =
        _queryContext.getOrderByExpressions() != null ? _queryContext.getOrderByExpressions().get(0) : null;
    DistinctTable distinctTable =
        canUseDictIdDistinctTable(orderByExpression) ? createDictIdDistinctTable(orderByExpression)
            : createTypedDistinctTable(orderByExpression);
    return new DistinctResultsBlock(convertDistinctTable(distinctTable, false), _queryContext);
  }

  private DataSchema createDataSchema() {
    ExpressionContext expr = _queryContext.getSelectExpressions().get(0);
    String column = expr.getIdentifier();
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    return new DataSchema(new String[]{column},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(dataSourceMetadata.getDataType())});
  }

  private DictIdDistinctTable createDictIdDistinctTable(@Nullable OrderByExpressionContext orderByExpression) {
    return new DictIdDistinctTable(createDataSchema(), _queryContext.getLimit(), _queryContext.isNullHandlingEnabled(),
        orderByExpression);
  }

  private DistinctTable createTypedDistinctTable(@Nullable OrderByExpressionContext orderByExpression) {
    DataSchema dataSchema = createDataSchema();
    int limit = _queryContext.getLimit();
    boolean nullHandlingEnabled = _queryContext.isNullHandlingEnabled();
    switch (_dictionary.getValueType()) {
      case INT:
        return new IntDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      case LONG:
        return new LongDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      case FLOAT:
        return new FloatDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      case DOUBLE:
        return new DoubleDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      case BIG_DECIMAL:
        return new BigDecimalDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      case STRING:
        return new StringDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      case BYTES:
        return new BytesDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
      default:
        throw new IllegalStateException("Unsupported data type: " + _dictionary.getValueType());
    }
  }

  private DistinctTable convertDistinctTable(DistinctTable distinctTable, boolean hasNull) {
    if (distinctTable instanceof DictIdDistinctTable) {
      return ((DictIdDistinctTable) distinctTable).toTypedDistinctTable(_dictionary, hasNull);
    }
    return distinctTable;
  }

  private boolean addDistinctValue(DistinctTable distinctTable, int dictId,
      @Nullable OrderByExpressionContext orderByExpression, boolean orderedEarlyTermination) {
    if (distinctTable instanceof DictIdDistinctTable) {
      DictIdDistinctTable dictIdDistinctTable = (DictIdDistinctTable) distinctTable;
      if (orderedEarlyTermination) {
        return dictIdDistinctTable.addForOrderedEarlyTermination(dictId);
      }
      if (dictIdDistinctTable.hasLimit()) {
        if (orderByExpression != null) {
          dictIdDistinctTable.addWithOrderBy(dictId);
          return false;
        }
        return dictIdDistinctTable.addWithoutOrderBy(dictId);
      }
      dictIdDistinctTable.addUnbounded(dictId);
      return false;
    }

    switch (_dictionary.getValueType()) {
      case INT: {
        IntDistinctTable table = (IntDistinctTable) distinctTable;
        int value = _dictionary.getIntValue(dictId);
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      case LONG: {
        LongDistinctTable table = (LongDistinctTable) distinctTable;
        long value = _dictionary.getLongValue(dictId);
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      case FLOAT: {
        FloatDistinctTable table = (FloatDistinctTable) distinctTable;
        float value = _dictionary.getFloatValue(dictId);
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      case DOUBLE: {
        DoubleDistinctTable table = (DoubleDistinctTable) distinctTable;
        double value = _dictionary.getDoubleValue(dictId);
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      case BIG_DECIMAL: {
        BigDecimalDistinctTable table = (BigDecimalDistinctTable) distinctTable;
        java.math.BigDecimal value = _dictionary.getBigDecimalValue(dictId);
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      case STRING: {
        StringDistinctTable table = (StringDistinctTable) distinctTable;
        String value = _dictionary.getStringValue(dictId);
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      case BYTES: {
        BytesDistinctTable table = (BytesDistinctTable) distinctTable;
        ByteArray value = new ByteArray(_dictionary.getBytesValue(dictId));
        if (table.hasLimit()) {
          if (orderByExpression != null) {
            table.addWithOrderBy(value);
            return false;
          }
          return table.addWithoutOrderBy(value);
        }
        table.addUnbounded(value);
        return false;
      }
      default:
        throw new IllegalStateException("Unsupported data type: " + _dictionary.getValueType());
    }
  }

  // ==================== Null Handling ====================

  /**
   * Processes null handling for the filter bitmap. Returns the filter bitmap with null docs excluded
   * and whether any filtered docs have null values.
   *
   * <p>Nulls are not in the dictionary, so they must be checked separately via the null value vector.
   * The null placeholder value (e.g., Integer.MIN_VALUE) is excluded from dictionary iteration by
   * removing null docs from the filter bitmap.
   */
  private NullFilterResult processNullDocs(@Nullable ImmutableRoaringBitmap filteredDocIds) {
    if (!_queryContext.isNullHandlingEnabled()) {
      return new NullFilterResult(filteredDocIds, false, Dictionary.NULL_VALUE_INDEX);
    }
    NullValueVectorReader nullReader = _dataSource.getNullValueVector();
    if (nullReader == null) {
      return new NullFilterResult(filteredDocIds, false, Dictionary.NULL_VALUE_INDEX);
    }
    ImmutableRoaringBitmap nullBitmap = nullReader.getNullBitmap();
    if (nullBitmap == null || nullBitmap.isEmpty()) {
      return new NullFilterResult(filteredDocIds, false, Dictionary.NULL_VALUE_INDEX);
    }
    // Determine if any filtered doc has null
    boolean hasNull = filteredDocIds == null || ImmutableRoaringBitmap.intersects(nullBitmap, filteredDocIds);
    // Exclude null docs from filter bitmap
    ImmutableRoaringBitmap nonNullFilteredDocIds;
    int nullPlaceholderDictId = Dictionary.NULL_VALUE_INDEX;
    if (filteredDocIds == null) {
      // Preserve match-all to avoid materializing a dense complement bitmap. Instead skip the null placeholder dictId
      // while iterating dictionary values.
      nonNullFilteredDocIds = null;
      nullPlaceholderDictId = getNullPlaceholderDictId();
    } else {
      nonNullFilteredDocIds = ImmutableRoaringBitmap.andNot(filteredDocIds, nullBitmap);
    }
    return new NullFilterResult(nonNullFilteredDocIds, hasNull, nullPlaceholderDictId);
  }

  private int getNullPlaceholderDictId() {
    Object defaultNullValue = _dataSource.getDataSourceMetadata().getFieldSpec().getDefaultNullValue();
    switch (_dictionary.getValueType()) {
      case INT:
        return _dictionary.indexOf((int) defaultNullValue);
      case LONG:
        return _dictionary.indexOf((long) defaultNullValue);
      case FLOAT:
        return _dictionary.indexOf((float) defaultNullValue);
      case DOUBLE:
        return _dictionary.indexOf((double) defaultNullValue);
      case BIG_DECIMAL:
        return _dictionary.indexOf((BigDecimal) defaultNullValue);
      case STRING:
        return _dictionary.indexOf((String) defaultNullValue);
      case BYTES:
        return _dictionary.indexOf(new ByteArray((byte[]) defaultNullValue));
      default:
        return Dictionary.NULL_VALUE_INDEX;
    }
  }

  private static class NullFilterResult {
    final ImmutableRoaringBitmap _nonNullFilteredDocIds;
    final boolean _hasNull;
    final int _nullPlaceholderDictId;

    NullFilterResult(@Nullable ImmutableRoaringBitmap nonNullFilteredDocIds, boolean hasNull,
        int nullPlaceholderDictId) {
      _nonNullFilteredDocIds = nonNullFilteredDocIds;
      _hasNull = hasNull;
      _nullPlaceholderDictId = nullPlaceholderDictId;
    }
  }

  // ==================== Operator Interface ====================

  @Override
  public List<? extends Operator> getChildOperators() {
    // If scan fallback was used, the project operator is the logical child
    if (_projectOperator != null && !_usedInvertedIndexPath) {
      return Collections.singletonList(_projectOperator);
    }
    // For inverted/sorted index paths (or before execution in EXPLAIN plans),
    // the filter operator is the logical child.
    return Collections.singletonList(_filterOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    if (_usedInvertedIndexPath || _projectOperator == null) {
      // For inverted/sorted index paths: numDocsScanned=0 (no forward index lookups),
      // numEntriesScannedInFilter tracks work done while materializing the exact filter bitmap,
      // numEntriesScannedPostFilter=numEntriesExamined (dictionary entries examined via bitmap
      // intersection or sorted range checks).
      return new ExecutionStatistics(0, _numEntriesScannedInFilter, _numEntriesExamined, numTotalDocs);
    }
    // _numEntriesScannedInFilter captures filter work from exact bitmap materialization (non-zero only when
    // the filter could not produce bitmaps directly). The project operator's stats capture any additional
    // filter work (zero when using a pre-built BitmapBasedFilterOperator).
    long numEntriesScannedInFilter = _numEntriesScannedInFilter
        + _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    // Single-column distinct, so numEntriesScannedPostFilter equals numDocsScanned
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, _numDocsScanned, numTotalDocs);
  }

  private String resolveExplainName() {
    // Sorted index can be determined pre-execution from the reader type
    if (_invertedIndexReader instanceof SortedIndexReader) {
      return EXPLAIN_NAME_SORTED_INDEX;
    }
    // After execution, if scan fallback was used, report it accurately
    if (_projectOperator != null && !_usedInvertedIndexPath) {
      return EXPLAIN_NAME_SCAN_FALLBACK;
    }
    // Default: inverted index path (correct both pre-execution and post-execution)
    return EXPLAIN_NAME;
  }

  @Override
  public String toExplainString() {
    String explainName = resolveExplainName();
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    int numExpressions = expressions.size();
    StringBuilder stringBuilder = new StringBuilder(explainName).append("(keyColumns:");
    stringBuilder.append(expressions.get(0).toString());
    for (int i = 1; i < numExpressions; i++) {
      stringBuilder.append(", ").append(expressions.get(i).toString());
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, resolveExplainName());
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    List<ExpressionContext> selectExpressions = _queryContext.getSelectExpressions();
    if (selectExpressions.isEmpty()) {
      return;
    }
    List<String> expressions = selectExpressions.stream()
        .map(ExpressionContext::toString)
        .collect(Collectors.toList());
    attributeBuilder.putStringList("keyColumns", expressions);
  }
}
