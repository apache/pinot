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
import java.util.Collections;
import java.util.List;
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
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorFactory;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
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
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Operator for distinct queries on a single segment.
 * <p>
 * Supports two execution paths:
 * <ul>
 *   <li><b>Scan path</b>: Uses ProjectOperator + DistinctExecutor to scan filtered docs (default).</li>
 *   <li><b>Inverted index path</b>: Iterates dictionary entries and uses inverted index bitmap intersections
 *       to check filter membership. Avoids the projection pipeline entirely.</li>
 * </ul>
 * When inverted index context is provided (via the augmented constructor), the operator evaluates at runtime
 * which path is cheaper based on dictionary cardinality vs filtered doc count.
 */
public class DistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT";
  private static final String EXPLAIN_NAME_INVERTED_INDEX = "DISTINCT_INVERTED_INDEX";

  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;

  // Inverted index path fields (null when not eligible)
  @Nullable
  private final DataSource _dataSource;
  @Nullable
  private final Dictionary _dictionary;
  @Nullable
  private final InvertedIndexReader<?> _invertedIndexReader;

  // Scan path: created lazily
  private BaseProjectOperator<?> _projectOperator;

  // Execution tracking
  private boolean _usedInvertedIndexPath = false;
  private int _numDocsScanned = 0;
  private int _numValuesProcessed = 0;

  public DistinctOperator(IndexSegment indexSegment, SegmentContext segmentContext,
      QueryContext queryContext, BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _filterOperator = filterOperator;

    // Check inverted index eligibility: single column with dictionary + inverted index, no nulls
    List<ExpressionContext> expressions = queryContext.getSelectExpressions();
    DataSource dataSource = null;
    Dictionary dictionary = null;
    InvertedIndexReader<?> invertedIndexReader = null;
    if (expressions.size() == 1) {
      String column = expressions.get(0).getIdentifier();
      if (column != null) {
        DataSource ds = indexSegment.getDataSource(column, queryContext.getSchema());
        if (ds.getDictionary() != null && ds.getInvertedIndex() != null) {
          boolean eligible = true;
          if (queryContext.isNullHandlingEnabled()) {
            var nullValueReader = ds.getNullValueVector();
            if (nullValueReader != null && !nullValueReader.getNullBitmap().isEmpty()) {
              eligible = false;
            }
          }
          if (eligible) {
            dataSource = ds;
            dictionary = ds.getDictionary();
            invertedIndexReader = ds.getInvertedIndex();
          }
        }
      }
    }
    _dataSource = dataSource;
    _dictionary = dictionary;
    _invertedIndexReader = invertedIndexReader;

    // Eagerly create project operator when inverted index is not eligible.
    // This ensures forward-index-disabled validation errors are thrown during plan creation (not deferred to
    // execution), and that the operator tree is visible for explain plans.
    // When inverted index IS eligible, project operator is created lazily only if the scan path is chosen.
    if (dictionary == null || invertedIndexReader == null) {
      _projectOperator = new ProjectPlanNode(_segmentContext, _queryContext,
          _queryContext.getSelectExpressions(), DocIdSetPlanNode.MAX_DOC_PER_CALL, _filterOperator).run();
    }
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    if (_dictionary != null && _invertedIndexReader != null && shouldUseInvertedIndex()) {
      _usedInvertedIndexPath = true;
      return executeInvertedIndexPath();
    }
    return executeScanPath();
  }

  // ==================== Cost Heuristic ====================

  /**
   * Default cost ratio for the inverted-index-based distinct heuristic. The inverted index path is chosen when
   * dictionaryCardinality * costRatio <= filteredDocCount.
   *
   * <p>The cost ratio accounts for the fact that each inverted index bitmap intersection is more expensive
   * than scanning a single document through projection. Benchmarking (BenchmarkInvertedIndexDistinct) shows
   * the crossover ratio varies by dictionary size: ~30x for small dicts (100), ~5-10x for large dicts (10K+).
   * A ratio of 5 balances avoiding pathological cases while capturing the large wins.
   *
   * <p>Can be overridden at query time via the query option {@code invertedIndexDistinctCostRatio}.
   */
  static final int DEFAULT_INVERTED_INDEX_COST_RATIO = 5;

  private boolean shouldUseInvertedIndex() {
    int dictionaryCardinality = _dictionary.length();
    int filteredDocCount = estimateFilteredDocCount();
    if (filteredDocCount == 0) {
      return false;
    }
    if (dictionaryCardinality == 0) {
      return true;
    }
    Integer costRatioOverride = QueryOptionsUtils.getInvertedIndexDistinctCostRatio(_queryContext.getQueryOptions());
    int costRatio = costRatioOverride != null ? costRatioOverride : DEFAULT_INVERTED_INDEX_COST_RATIO;
    return (long) dictionaryCardinality * costRatio <= filteredDocCount;
  }

  /**
   * Cheaply estimates the number of docs matching the filter without consuming the filter operator.
   */
  private int estimateFilteredDocCount() {
    if (_filterOperator.isResultEmpty()) {
      return 0;
    }
    if (_filterOperator.isResultMatchingAll()) {
      return _indexSegment.getSegmentMetadata().getTotalDocs();
    }
    if (_filterOperator.canOptimizeCount()) {
      return _filterOperator.getNumMatchingDocs();
    }
    if (_filterOperator.canProduceBitmaps()) {
      return _filterOperator.getBitmaps().reduce().getCardinality();
    }
    // Conservative fallback: assume all docs match (biases toward inverted index since user opted in)
    return _indexSegment.getSegmentMetadata().getTotalDocs();
  }

  // ==================== Scan Path ====================

  private DistinctResultsBlock executeScanPath() {
    BaseProjectOperator<?> projectOperator = getOrCreateProjectOperator();
    DistinctExecutor executor = DistinctExecutorFactory.getDistinctExecutor(projectOperator, _queryContext);
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      _numDocsScanned += valueBlock.getNumDocs();
      if (executor.process(valueBlock)) {
        break;
      }
    }
    return new DistinctResultsBlock(executor.getResult(), _queryContext);
  }

  private BaseProjectOperator<?> getOrCreateProjectOperator() {
    if (_projectOperator == null) {
      // Lazy creation: reuse the already-computed filterOperator to avoid recomputing the filter
      _projectOperator = new ProjectPlanNode(_segmentContext, _queryContext,
          _queryContext.getSelectExpressions(), DocIdSetPlanNode.MAX_DOC_PER_CALL, _filterOperator).run();
    }
    return _projectOperator;
  }

  // ==================== Inverted Index Path ====================

  private DistinctResultsBlock executeInvertedIndexPath() {
    ExpressionContext expr = _queryContext.getSelectExpressions().get(0);
    String column = expr.getIdentifier();

    RoaringBitmap filteredDocIds = buildFilteredDocIds();
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    DataSchema dataSchema = new DataSchema(new String[]{column},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(dataSourceMetadata.getDataType())});
    OrderByExpressionContext orderByExpression =
        _queryContext.getOrderByExpressions() != null ? _queryContext.getOrderByExpressions().get(0) : null;

    DistinctTable distinctTable = createDistinctTable(dataSchema, orderByExpression);
    int dictLength = _dictionary.length();
    int limit = _queryContext.getLimit();

    for (int dictId = 0; dictId < dictLength; dictId++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numValuesProcessed, EXPLAIN_NAME_INVERTED_INDEX);

      Object docIdsObj = _invertedIndexReader.getDocIds(dictId);
      if (!(docIdsObj instanceof ImmutableRoaringBitmap)) {
        continue;
      }
      ImmutableRoaringBitmap docIds = (ImmutableRoaringBitmap) docIdsObj;
      if (docIds.isEmpty()) {
        continue;
      }

      boolean includeValue;
      if (filteredDocIds == null) {
        includeValue = true;
      } else {
        RoaringBitmap docIdsRoaring = docIds.toMutableRoaringBitmap().toRoaringBitmap();
        RoaringBitmap intersection = RoaringBitmap.and(docIdsRoaring, filteredDocIds);
        includeValue = !intersection.isEmpty();
      }

      if (includeValue) {
        boolean done = addValueToDistinctTable(distinctTable, dictId, orderByExpression);
        _numValuesProcessed++;
        if (done) {
          break;
        }
      }

      if (orderByExpression == null && distinctTable.hasLimit() && distinctTable.size() >= limit) {
        break;
      }
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  @Nullable
  private RoaringBitmap buildFilteredDocIds() {
    if (_filterOperator.isResultMatchingAll()) {
      return null;
    }

    if (_filterOperator.canProduceBitmaps()) {
      return _filterOperator.getBitmaps().reduce().toRoaringBitmap();
    }

    if (_filterOperator.isResultEmpty()) {
      return new RoaringBitmap();
    }

    RoaringBitmap bitmap = new RoaringBitmap();
    DocIdSetPlanNode docIdSetPlanNode =
        new DocIdSetPlanNode(_segmentContext, _queryContext, DocIdSetPlanNode.MAX_DOC_PER_CALL, _filterOperator);
    var docIdSetOperator = docIdSetPlanNode.run();
    DocIdSetBlock block;
    while ((block = docIdSetOperator.nextBlock()) != null) {
      int[] docIds = block.getDocIds();
      int length = block.getLength();
      bitmap.addN(docIds, 0, length);
    }
    return bitmap;
  }

  private DistinctTable createDistinctTable(DataSchema dataSchema,
      @Nullable OrderByExpressionContext orderByExpression) {
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

  private boolean addValueToDistinctTable(DistinctTable distinctTable, int dictId,
      @Nullable OrderByExpressionContext orderByExpression) {
    switch (_dictionary.getValueType()) {
      case INT:
        return addToTable((IntDistinctTable) distinctTable, _dictionary.getIntValue(dictId), orderByExpression);
      case LONG:
        return addToTable((LongDistinctTable) distinctTable, _dictionary.getLongValue(dictId), orderByExpression);
      case FLOAT:
        return addToTable((FloatDistinctTable) distinctTable, _dictionary.getFloatValue(dictId), orderByExpression);
      case DOUBLE:
        return addToTable((DoubleDistinctTable) distinctTable, _dictionary.getDoubleValue(dictId), orderByExpression);
      case BIG_DECIMAL:
        return addToTable((BigDecimalDistinctTable) distinctTable, _dictionary.getBigDecimalValue(dictId),
            orderByExpression);
      case STRING:
        return addToTable((StringDistinctTable) distinctTable, _dictionary.getStringValue(dictId), orderByExpression);
      case BYTES:
        return addToTable((BytesDistinctTable) distinctTable, _dictionary.getByteArrayValue(dictId), orderByExpression);
      default:
        throw new IllegalStateException("Unsupported data type: " + _dictionary.getValueType());
    }
  }

  private static boolean addToTable(IntDistinctTable table, int value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(LongDistinctTable table, long value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(FloatDistinctTable table, float value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(DoubleDistinctTable table, double value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(BigDecimalDistinctTable table, java.math.BigDecimal value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(StringDistinctTable table, String value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(BytesDistinctTable table, ByteArray value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  // ==================== Operator Interface ====================

  @Override
  public List<? extends Operator> getChildOperators() {
    if (_usedInvertedIndexPath || _projectOperator == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    if (_usedInvertedIndexPath || _projectOperator == null) {
      return new ExecutionStatistics(_numDocsScanned > 0 ? _numDocsScanned : _numValuesProcessed, 0, 0, numTotalDocs);
    }
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }

  @Override
  public String toExplainString() {
    String explainName = _usedInvertedIndexPath ? EXPLAIN_NAME_INVERTED_INDEX : EXPLAIN_NAME;
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
    String explainName = _usedInvertedIndexPath ? EXPLAIN_NAME_INVERTED_INDEX : EXPLAIN_NAME;
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, explainName);
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
