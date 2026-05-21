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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.JsonExtractIndexUtils.ParsedJsonExtractIndex;
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
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Distinct operator for the scalar {@code jsonExtractIndex(column, path, type[, defaultValue])} form.
 *
 * <p>Execution flow:
 * 1. Push a same-path {@code JSON_MATCH} predicate into the JSON-index lookup when it cannot match missing paths.
 * 2. Convert matching flattened doc ids back to segment doc ids.
 * 3. Apply any remaining row-level filter and materialize DISTINCT results, including missing-path handling.
 */
public class JsonIndexDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_JSON_INDEX";

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
    ParsedJsonExtractIndex parsed = JsonExtractIndexUtils.parseJsonExtractIndex(expr);
    if (parsed == null) {
      throw new IllegalStateException("Expected 3/4-arg scalar jsonExtractIndex expression");
    }

    DataSource dataSource = _indexSegment.getDataSource(parsed._columnName, _queryContext.getSchema());
    JsonIndexReader jsonIndexReader = JsonExtractIndexUtils.getJsonIndexReader(dataSource);
    if (jsonIndexReader == null) {
      throw new IllegalStateException("Column " + parsed._columnName + " has no JSON index");
    }

    String pushedDownFilterJson = JsonExtractIndexUtils.extractSamePathJsonMatchFilter(parsed,
        _queryContext.getFilter());
    boolean filterFullyPushedDown = pushedDownFilterJson != null
        && JsonExtractIndexUtils.isOnlySamePathJsonMatchFilter(parsed, _queryContext.getFilter())
        && !JsonExtractIndexUtils.jsonMatchFilterCanMatchMissingPath(pushedDownFilterJson);

    // Fast path: when the filter is fully pushed down into the JSON index, we only need the distinct value strings.
    // This avoids reading posting lists, building per-value bitmaps, and converting flattened doc IDs.
    if (filterFullyPushedDown) {
      Set<String> distinctValues = jsonIndexReader.getMatchingDistinctValues(
          parsed._jsonPathString, pushedDownFilterJson);
      return buildDistinctResultsFromValues(expr, parsed, distinctValues);
    }

    // Evaluate the filter first so we can skip the (potentially expensive) index map when no docs match.
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

    // All other WHERE filters remain row-level and are applied after converting flattened doc IDs to real doc IDs.
    Map<String, RoaringBitmap> valueToMatchingDocs =
        jsonIndexReader.getMatchingFlattenedDocsMap(parsed._jsonPathString, pushedDownFilterJson);

    // Always single-value (MV _ARRAY is rejected in parseJsonExtractIndex)
    jsonIndexReader.convertFlattenedDocIdsToDocIds(valueToMatchingDocs);
    return buildDistinctResultsBlock(expr, parsed, valueToMatchingDocs, filteredDocIds,
        filteredDocIds == null);
  }

  private DistinctResultsBlock buildDistinctResultsFromValues(ExpressionContext expr, ParsedJsonExtractIndex parsed,
      Set<String> distinctValues) {
    ColumnDataType columnDataType = ColumnDataType.fromDataTypeSV(parsed._dataType);
    DataSchema dataSchema = new DataSchema(
        new String[]{expr.toString()},
        new ColumnDataType[]{columnDataType});
    OrderByExpressionContext orderByExpression = _queryContext.getOrderByExpressions() != null
        ? _queryContext.getOrderByExpressions().get(0) : null;
    DistinctTable distinctTable = createDistinctTable(dataSchema, parsed._dataType, orderByExpression);
    int limit = _queryContext.getLimit();

    for (String value : distinctValues) {
      _numEntriesExamined++;
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numEntriesExamined, EXPLAIN_NAME);

      boolean done = addValueToDistinctTable(distinctTable, value, parsed._dataType, orderByExpression);
      if (done) {
        break;
      }
      if (orderByExpression == null && distinctTable.hasLimit() && distinctTable.size() >= limit) {
        break;
      }
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  private DistinctResultsBlock buildDistinctResultsBlock(ExpressionContext expr, ParsedJsonExtractIndex parsed,
      Map<String, RoaringBitmap> valueToMatchingDocs, @Nullable RoaringBitmap filteredDocIds,
      boolean allDocsSelected) {
    ColumnDataType columnDataType = ColumnDataType.fromDataTypeSV(parsed._dataType);
    DataSchema dataSchema = new DataSchema(
        new String[]{expr.toString()},
        new ColumnDataType[]{columnDataType});
    OrderByExpressionContext orderByExpression = _queryContext.getOrderByExpressions() != null
        ? _queryContext.getOrderByExpressions().get(0) : null;
    DistinctTable distinctTable = createDistinctTable(dataSchema, parsed._dataType, orderByExpression);

    int limit = _queryContext.getLimit();
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    RoaringBitmap coveredDocs = allDocsSelected ? new RoaringBitmap() : null;
    RoaringBitmap remainingDocs = filteredDocIds != null ? filteredDocIds.clone() : null;
    boolean allDocsCovered = filteredDocIds == null ? !allDocsSelected || totalDocs == 0 : filteredDocIds.isEmpty();
    boolean earlyBreak = false;

    for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingDocs.entrySet()) {
      _numEntriesExamined++;
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numEntriesExamined, EXPLAIN_NAME);

      String value = entry.getKey();
      RoaringBitmap docIds = entry.getValue();

      boolean includeValue;
      if (filteredDocIds == null) {
        includeValue = true;
        if (!allDocsCovered && allDocsSelected) {
          coveredDocs.or(docIds);
          if (coveredDocs.getLongCardinality() >= totalDocs) {
            allDocsCovered = true;
          }
        }
      } else {
        includeValue = RoaringBitmap.intersects(docIds, filteredDocIds);
        // Remove matched docs from remaining set in-place (no allocation per value).
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

    if (!earlyBreak && !allDocsCovered) {
      handleMissingDocs(distinctTable, parsed, orderByExpression);
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  private void handleMissingDocs(DistinctTable distinctTable, ParsedJsonExtractIndex parsed,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (parsed._defaultValueLiteral != null) {
      addValueToDistinctTable(distinctTable, parsed._defaultValueLiteral, parsed._dataType, orderByExpression);
    } else if (_queryContext.isNullHandlingEnabled()) {
      distinctTable.addNull();
    } else {
      throw new RuntimeException(
          String.format("Illegal Json Path: [%s], for some docIds in segment [%s]",
              parsed._jsonPathString, _indexSegment.getSegmentName()));
    }
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
  private RoaringBitmap buildFilteredDocIds() {
    BaseFilterOperator.FilteredDocIds filteredDocIds = _filterOperator.getFilteredDocIds();
    _numEntriesScannedInFilter = filteredDocIds.getNumEntriesScannedInFilter();
    ImmutableRoaringBitmap docIds = filteredDocIds.getDocIds();
    return docIds != null ? docIds.toRoaringBitmap() : null;
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
   * Returns true if the expression is the 3/4-arg scalar {@code jsonExtractIndex} form on a column with a JSON index
   * that covers the path. For OSS JSON index all paths are indexed; for the composite JSON index, only paths in
   * {@code invertedIndexConfigs} are indexed per key.
   */
  public static boolean canUseJsonIndexDistinct(IndexSegment indexSegment, ExpressionContext expr) {
    return JsonExtractIndexUtils.canUseJsonIndex(indexSegment, expr);
  }
}
