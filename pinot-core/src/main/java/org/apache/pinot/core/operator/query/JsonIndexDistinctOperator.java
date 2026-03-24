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
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
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
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.RoaringBitmap;


/**
 * Distinct operator that uses the JSON index value→docId map directly instead of scanning documents.
 * Avoids the projection/transform pipeline for SELECT DISTINCT jsonExtractIndex(...).
 */
public class JsonIndexDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_JSON_INDEX";
  private static final String FUNCTION_NAME = "jsonExtractIndex";

  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;

  private int _numEntriesExamined = 0;
  private long _numEntriesScannedInFilter = 0;

  public JsonIndexDistinctOperator(IndexSegment indexSegment, SegmentContext segmentContext,
      QueryContext queryContext, BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _filterOperator = filterOperator;
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    if (expressions.size() != 1) {
      throw new IllegalStateException("JsonIndexDistinctOperator supports single expression only");
    }

    ExpressionContext expr = expressions.get(0);
    ParsedJsonExtractIndex parsed = parseJsonExtractIndex(expr);
    if (parsed == null) {
      throw new IllegalStateException("Expected jsonExtractIndex expression");
    }

    // Evaluate the filter first so we can skip the (potentially expensive) index map when no docs match
    RoaringBitmap filteredDocIds = buildFilteredDocIds();
    if (filteredDocIds != null && filteredDocIds.isEmpty()) {
      ColumnDataType earlyColumnDataType = ColumnDataType.fromDataTypeSV(parsed._dataType);
      DataSchema earlyDataSchema = new DataSchema(
          new String[]{expr.toString()},
          new ColumnDataType[]{earlyColumnDataType});
      OrderByExpressionContext earlyOrderBy = _queryContext.getOrderByExpressions() != null
          ? _queryContext.getOrderByExpressions().get(0) : null;
      return new DistinctResultsBlock(
          createDistinctTable(earlyDataSchema, parsed._dataType, earlyOrderBy), _queryContext);
    }

    DataSource dataSource = _indexSegment.getDataSource(parsed._columnName, _queryContext.getSchema());
    JsonIndexReader jsonIndexReader = getJsonIndexReader(dataSource);
    if (jsonIndexReader == null) {
      throw new IllegalStateException("Column " + parsed._columnName + " has no JSON index");
    }

    // Same logic as JsonExtractIndexTransformFunction.getValueToMatchingDocsMap()
    Map<String, RoaringBitmap> valueToMatchingDocs =
        jsonIndexReader.getMatchingFlattenedDocsMap(parsed._jsonPathString, parsed._filterExpression);
    // Always single-value (MV _ARRAY is rejected in parseJsonExtractIndex)
    jsonIndexReader.convertFlattenedDocIdsToDocIds(valueToMatchingDocs);

    ColumnDataType columnDataType = ColumnDataType.fromDataTypeSV(parsed._dataType);
    DataSchema dataSchema = new DataSchema(
        new String[]{expr.toString()},
        new ColumnDataType[]{columnDataType});
    OrderByExpressionContext orderByExpression = _queryContext.getOrderByExpressions() != null
        ? _queryContext.getOrderByExpressions().get(0) : null;
    DistinctTable distinctTable = createDistinctTable(dataSchema, parsed._dataType, orderByExpression);

    int limit = _queryContext.getLimit();
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    // Track uncovered docs: for the no-filter case, build a union and compare against totalDocs.
    // For the filtered case, use a "remaining" bitmap that shrinks in-place (no per-value allocation).
    RoaringBitmap allMatchedDocs = filteredDocIds == null ? new RoaringBitmap() : null;
    RoaringBitmap remainingDocs = filteredDocIds != null ? filteredDocIds.clone() : null;
    boolean allDocsCovered = filteredDocIds == null ? (totalDocs == 0) : filteredDocIds.isEmpty();
    boolean earlyBreak = false;

    for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingDocs.entrySet()) {
      _numEntriesExamined++;
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numEntriesExamined, EXPLAIN_NAME);
      String value = entry.getKey();
      RoaringBitmap docIds = entry.getValue();

      boolean includeValue;
      if (filteredDocIds == null) {
        includeValue = true;
        // Build union for uncovered-docs detection; short-circuit once all segment docs are covered
        if (!allDocsCovered) {
          allMatchedDocs.or(docIds);
          if (allMatchedDocs.getLongCardinality() >= totalDocs) {
            allDocsCovered = true;
          }
        }
      } else {
        includeValue = RoaringBitmap.intersects(docIds, filteredDocIds);
        // Remove matched docs from remaining set in-place (no allocation per value)
        if (!allDocsCovered && includeValue) {
          remainingDocs.andNot(docIds);
          if (remainingDocs.isEmpty()) {
            allDocsCovered = true;
          }
        }
      }

      if (includeValue) {
        boolean done = addValueToDistinctTable(distinctTable, value, parsed._dataType, orderByExpression);
        if (done) {
          earlyBreak = true;
          break;
        }
      }

      if (orderByExpression == null && distinctTable.hasLimit() && distinctTable.size() >= limit) {
        earlyBreak = true;
        break;
      }
    }

    // Handle docs not covered by any value in the index.
    // Baseline JsonExtractIndexTransformFunction throws when a doc is missing the path and no
    // defaultValue is provided. Match that behavior here unless nullHandling is enabled.
    // allDocsCovered tracks coverage precisely (against totalDocs or filteredDocIds cardinality).
    if (!earlyBreak && !allDocsCovered) {
      if (parsed._defaultValue != null) {
        addValueToDistinctTable(distinctTable, parsed._defaultValue, parsed._dataType, orderByExpression);
      } else if (_queryContext.isNullHandlingEnabled()) {
        distinctTable.addNull();
      } else {
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for some docIds in segment [%s]",
                parsed._jsonPathString, _indexSegment.getSegmentName()));
      }
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  private DistinctTable createDistinctTable(DataSchema dataSchema, FieldSpec.DataType dataType,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    boolean nullHandlingEnabled = _queryContext.isNullHandlingEnabled();
    switch (dataType) {
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
      default:
        throw new IllegalStateException("Unsupported data type for JSON index distinct: " + dataType);
    }
  }

  private static boolean addValueToDistinctTable(DistinctTable distinctTable, String stringValue,
      FieldSpec.DataType dataType, @Nullable OrderByExpressionContext orderByExpression) {
    switch (dataType) {
      case INT:
        return addToTable((IntDistinctTable) distinctTable, Integer.parseInt(stringValue), orderByExpression);
      case LONG:
        return addToTable((LongDistinctTable) distinctTable, Long.parseLong(stringValue), orderByExpression);
      case FLOAT:
        return addToTable((FloatDistinctTable) distinctTable, Float.parseFloat(stringValue), orderByExpression);
      case DOUBLE:
        return addToTable((DoubleDistinctTable) distinctTable, Double.parseDouble(stringValue), orderByExpression);
      case BIG_DECIMAL:
        return addToTable((BigDecimalDistinctTable) distinctTable, new BigDecimal(stringValue), orderByExpression);
      case STRING:
        return addToTable((StringDistinctTable) distinctTable, stringValue, orderByExpression);
      default:
        throw new IllegalStateException("Unsupported data type for JSON index distinct: " + dataType);
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

  private static boolean addToTable(BigDecimalDistinctTable table, BigDecimal value,
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

  @Nullable
  private static JsonIndexReader getJsonIndexReader(DataSource dataSource) {
    JsonIndexReader reader = dataSource.getJsonIndex();
    if (reader == null) {
      Optional<IndexType<?, ?, ?>> compositeIndex =
          IndexService.getInstance().getOptional("composite_json_index");
      if (compositeIndex.isPresent()) {
        reader = (JsonIndexReader) dataSource.getIndex(compositeIndex.get());
      }
    }
    return reader;
  }

  @Nullable
  private RoaringBitmap buildFilteredDocIds() {
    if (_filterOperator.isResultMatchingAll()) {
      return null;
    }

    if (_filterOperator.canProduceBitmaps()) {
      // Bitmap-capable filters (inverted index, JSON index, etc.) produce bitmaps without scanning
      // docs, so _numEntriesScannedInFilter correctly stays 0 for this path.
      return _filterOperator.getBitmaps().reduce().toRoaringBitmap();
    }

    if (_filterOperator.isResultEmpty()) {
      return new RoaringBitmap();
    }

    RoaringBitmap bitmap = new RoaringBitmap();
    DocIdSetPlanNode docIdSetPlanNode = new DocIdSetPlanNode(
        _segmentContext, _queryContext, DocIdSetPlanNode.MAX_DOC_PER_CALL, _filterOperator);
    var docIdSetOperator = docIdSetPlanNode.run();
    DocIdSetBlock block;
    while ((block = docIdSetOperator.nextBlock()) != null) {
      int[] docIds = block.getDocIds();
      int length = block.getLength();
      bitmap.addN(docIds, 0, length);
    }
    _numEntriesScannedInFilter = docIdSetOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    return bitmap;
  }

  @Nullable
  private static ParsedJsonExtractIndex parseJsonExtractIndex(ExpressionContext expr) {
    if (expr.getType() != ExpressionContext.Type.FUNCTION) {
      return null;
    }
    if (!FUNCTION_NAME.equalsIgnoreCase(expr.getFunction().getFunctionName())) {
      return null;
    }
    List<ExpressionContext> args = expr.getFunction().getArguments();
    if (args.size() < 3 || args.size() > 5) {
      return null;
    }
    if (args.get(0).getType() != ExpressionContext.Type.IDENTIFIER) {
      return null;
    }
    if (args.get(1).getType() != ExpressionContext.Type.LITERAL
        || args.get(2).getType() != ExpressionContext.Type.LITERAL) {
      return null;
    }

    String columnName = args.get(0).getIdentifier();
    String jsonPathString = args.get(1).getLiteral().getStringValue();
    String resultsType = args.get(2).getLiteral().getStringValue().toUpperCase();
    // Only single-value types are supported; MV (_ARRAY) would have incorrect flattened-to-real
    // docId intersection since convertFlattenedDocIdsToDocIds is skipped for MV.
    if (resultsType.endsWith("_ARRAY")) {
      return null;
    }
    if (jsonPathString.contains("[*]")) {
      return null;
    }

    FieldSpec.DataType dataType;
    try {
      dataType = FieldSpec.DataType.valueOf(resultsType);
    } catch (IllegalArgumentException e) {
      return null;
    }
    // Only types with a corresponding DistinctTable implementation are supported
    switch (dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
      case STRING:
        break;
      default:
        return null;
    }

    try {
      JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
    } catch (Exception e) {
      return null;
    }

    String defaultValue = null;
    if (args.size() >= 4) {
      if (args.get(3).getType() != ExpressionContext.Type.LITERAL) {
        return null;
      }
      defaultValue = args.get(3).getLiteral().getStringValue();
    }

    String filterExpression = null;
    if (args.size() == 5) {
      if (args.get(4).getType() != ExpressionContext.Type.LITERAL) {
        return null;
      }
      filterExpression = args.get(4).getLiteral().getStringValue();
    }

    return new ParsedJsonExtractIndex(columnName, jsonPathString, dataType, defaultValue, filterExpression);
  }

  private static final class ParsedJsonExtractIndex {
    final String _columnName;
    final String _jsonPathString;
    final FieldSpec.DataType _dataType;
    @Nullable
    final String _defaultValue;
    @Nullable
    final String _filterExpression;

    ParsedJsonExtractIndex(String columnName, String jsonPathString,
        FieldSpec.DataType dataType, @Nullable String defaultValue, @Nullable String filterExpression) {
      _columnName = columnName;
      _jsonPathString = jsonPathString;
      _dataType = dataType;
      _defaultValue = defaultValue;
      _filterExpression = filterExpression;
    }
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_filterOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    // Index-only operator: no docs scanned, no entries scanned post-filter.
    // Filter-phase stats are tracked when buildFilteredDocIds falls back to DocIdSetPlanNode.
    return new ExecutionStatistics(0, _numEntriesScannedInFilter, 0, numTotalDocs);
  }

  @Override
  public String toExplainString() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    return EXPLAIN_NAME + "(keyColumns:" + (expressions.isEmpty() ? "" : expressions.get(0).toString()) + ")";
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    List<ExpressionContext> selectExpressions = _queryContext.getSelectExpressions();
    if (!selectExpressions.isEmpty()) {
      attributeBuilder.putStringList("keyColumns",
          List.of(selectExpressions.get(0).toString()));
    }
  }

  /**
   * Returns true if the expression is jsonExtractIndex on a column with JSON index and the path is indexed.
   * For OSS JSON index all paths are indexed. For composite JSON index, only paths in invertedIndexConfigs
   * are indexed per key.
   */
  public static boolean canUseJsonIndexDistinct(IndexSegment indexSegment, ExpressionContext expr) {
    ParsedJsonExtractIndex parsed = parseJsonExtractIndex(expr);
    if (parsed == null) {
      return false;
    }
    DataSource dataSource = indexSegment.getDataSourceNullable(parsed._columnName);
    if (dataSource == null) {
      return false;
    }
    JsonIndexReader reader = getJsonIndexReader(dataSource);
    if (reader == null) {
      return false;
    }
    if (!reader.isPathIndexed(parsed._jsonPathString)) {
      return false;
    }
    // The 5th arg (_filterExpression) is a JSON filter expression, not a plain JSON path,
    // so isPathIndexed() is not appropriate for it. The reader's getMatchingFlattenedDocsMap()
    // handles filter expressions internally.
    return true;
  }
}
