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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.CaseFormat;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Distinct operator for `jsonExtractIndex(column, path, type[, defaultValue[, filterJsonExpression]])`.
///
/// Supports both SV (e.g. `STRING`) and MV (e.g. `STRING_ARRAY`) result types — DISTINCT collapses MV array elements
/// to scalar rows, matching the scan-based `SELECT DISTINCT mvCol` convention. The 4-arg default is a single value
/// for SV; for MV it's a JSON array whose elements are each added to the distinct set when no doc matches the path.
///
/// Execution flow:
/// 1. Pass the optional 5-arg `filterJsonExpression` directly to the JSON-index lookup (matches
///    `JsonExtractIndexTransformFunction`'s convention).
/// 2. Convert matching flattened doc ids back to segment doc ids.
/// 3. Apply any remaining row-level WHERE filter and materialize DISTINCT results, including missing-path handling.
public class JsonIndexDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_JSON_INDEX";
  private static final String FUNCTION_NAME = "jsonExtractIndex";

  /// Returns true if the expression is a `jsonExtractIndex` function call. All other validation (argument count/types,
  /// column existence, JSON index presence, path support) happens inside the operator's constructor and matches what
  /// the scan-based fallback (`JsonExtractIndexTransformFunction`) would surface during its own `init`.
  public static boolean canUseJsonIndexDistinct(ExpressionContext expr) {
    return expr.getType() == ExpressionContext.Type.FUNCTION && FUNCTION_NAME.equalsIgnoreCase(
        expr.getFunction().getFunctionName());
  }

  private final IndexSegment _indexSegment;
  private final int _totalDocs;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;
  private final ExpressionContext _expression;
  private final boolean _skipMissingPath;
  private final JsonIndexReader _jsonIndexReader;
  private final String _jsonPathString;
  private final DataType _dataType;
  @Nullable
  private final String[] _defaultValueLiterals;
  @Nullable
  private final String _filterJsonExpression;
  private final DataSchema _dataSchema;
  @Nullable
  private final OrderByExpressionContext _orderByExpression;

  private int _numDocsScanned = 0;
  private long _numEntriesScannedInFilter = 0;
  private int _numEntriesExaminedPostFilter = 0;

  public JsonIndexDistinctOperator(IndexSegment indexSegment, QueryContext queryContext,
      BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _totalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
    _queryContext = queryContext;
    _filterOperator = filterOperator;
    List<ExpressionContext> expressions = queryContext.getSelectExpressions();
    if (expressions.size() != 1) {
      throw new IllegalStateException("JsonIndexDistinctOperator supports single expression only");
    }
    _expression = expressions.get(0);
    _skipMissingPath = QueryOptionsUtils.isJsonIndexDistinctSkipMissingPath(queryContext.getQueryOptions());

    // Mirrors the arguments handling logic in `JsonExtractIndexTransformFunction`

    List<ExpressionContext> arguments = _expression.getFunction().getArguments();
    int numArguments = arguments.size();
    // Check that there are exactly 3 or 4 or 5 arguments
    if (numArguments < 3 || numArguments > 5) {
      throw new IllegalArgumentException(
          "Expected 3/4/5 arguments for jsonExtractIndex(jsonFieldName, 'jsonPath', 'resultsType',"
              + " ['defaultValue'], ['jsonFilterExpression'])");
    }

    ExpressionContext firstArgument = arguments.get(0);
    if (firstArgument.getType() == ExpressionContext.Type.IDENTIFIER) {
      DataSource dataSource = indexSegment.getDataSource(firstArgument.getIdentifier());
      _jsonIndexReader = getJsonIndexReader(dataSource);
      if (_jsonIndexReader == null) {
        throw new IllegalStateException("jsonExtractIndex can only be applied on a column with JSON index");
      }
    } else {
      throw new IllegalArgumentException("jsonExtractIndex can only be applied to a raw column");
    }

    ExpressionContext secondArgument = arguments.get(1);
    if (secondArgument.getType() != ExpressionContext.Type.LITERAL) {
      throw new IllegalArgumentException("JSON path argument must be a literal");
    }
    _jsonPathString = secondArgument.getLiteral().getStringValue();
    try {
      JsonPathCache.INSTANCE.getOrCompute(_jsonPathString);
    } catch (Exception e) {
      throw new IllegalArgumentException("JSON path argument is not a valid JSON path");
    }

    ExpressionContext thirdArgument = arguments.get(2);
    if (thirdArgument.getType() != ExpressionContext.Type.LITERAL) {
      throw new IllegalArgumentException("Result type argument must be a literal");
    }
    String resultsType = thirdArgument.getLiteral().getStringValue().toUpperCase();
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    if (isSingleValue && _jsonPathString.contains("[*]")) {
      throw new IllegalArgumentException(
          "[*] syntax in json path is unsupported for singleValue field json_extract_index");
    }
    String dataTypeName = isSingleValue ? resultsType : resultsType.substring(0, resultsType.length() - 6);
    try {
      _dataType = DataType.valueOf(dataTypeName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown jsonExtractIndex result type: " + resultsType);
    }
    switch (_dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
      case STRING:
        break;
      default:
        throw new IllegalArgumentException("Unsupported jsonExtractIndex result type for distinct: " + _dataType);
    }

    // With _skipMissingPath, the 4-arg default is never used at runtime (handleMissingDocs is bypassed), so don't
    // parse or validate it — accept any literal shape and ignore it.
    if (numArguments >= 4 && !_skipMissingPath) {
      ExpressionContext fourthArgument = arguments.get(3);
      if (fourthArgument.getType() != ExpressionContext.Type.LITERAL) {
        throw new IllegalArgumentException("Default value must be a literal");
      }
      String defaultLiteral = fourthArgument.getLiteral().getStringValue();
      if (isSingleValue) {
        try {
          _dataType.convert(defaultLiteral);
        } catch (Exception e) {
          throw new IllegalArgumentException("Default value '" + defaultLiteral + "' is not a valid " + _dataType);
        }
        _defaultValueLiterals = new String[]{defaultLiteral};
      } else {
        try {
          JsonNode mvArray = JsonUtils.stringToJsonNode(defaultLiteral);
          if (!mvArray.isArray()) {
            throw new IllegalArgumentException("Default value must be a valid JSON array");
          }
          String[] literals = new String[mvArray.size()];
          for (int i = 0; i < mvArray.size(); i++) {
            literals[i] = mvArray.get(i).asText();
            try {
              _dataType.convert(literals[i]);
            } catch (Exception e) {
              throw new IllegalArgumentException("Default value '" + literals[i] + "' is not a valid " + _dataType);
            }
          }
          _defaultValueLiterals = literals;
        } catch (IOException e) {
          throw new IllegalArgumentException("Default value must be a valid JSON array");
        }
      }
    } else {
      _defaultValueLiterals = null;
    }

    if (numArguments == 5) {
      ExpressionContext fifthArgument = arguments.get(4);
      if (fifthArgument.getType() != ExpressionContext.Type.LITERAL) {
        throw new IllegalArgumentException("JSON path filter argument must be a literal");
      }
      _filterJsonExpression = fifthArgument.getLiteral().getStringValue();
    } else {
      _filterJsonExpression = null;
    }

    _dataSchema = new DataSchema(new String[]{_expression.toString()},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(_dataType)});
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    _orderByExpression = orderByExpressions != null ? orderByExpressions.get(0) : null;
  }

  @Nullable
  private static JsonIndexReader getJsonIndexReader(DataSource dataSource) {
    JsonIndexReader reader = dataSource.getJsonIndex();
    // TODO: rework
    if (reader == null) {
      Optional<IndexType<?, ?, ?>> compositeIndex = IndexService.getInstance().getOptional("composite_json_index");
      if (compositeIndex.isPresent()) {
        reader = (JsonIndexReader) dataSource.getIndex(compositeIndex.get());
      }
    }
    return reader;
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    // Evaluate the filter first so we can skip the (potentially expensive) index map when no docs match.
    BaseFilterOperator.FilteredDocIds filteredDocIds = _filterOperator.getFilteredDocIds();
    ImmutableRoaringBitmap docIds = filteredDocIds.getDocIds();
    _numDocsScanned = docIds != null ? docIds.getCardinality() : _totalDocs;
    _numEntriesScannedInFilter = filteredDocIds.getNumEntriesScannedInFilter();
    if (_numDocsScanned == 0) {
      return new DistinctResultsBlock(createDistinctTable(), _queryContext);
    }

    // The 5-arg form's filter literal is pushed into the JSON index; WHERE-clause filters remain row-level and are
    // applied after converting flattened doc IDs to real doc IDs.
    Map<String, RoaringBitmap> valueToMatchingDocs =
        _jsonIndexReader.getMatchingFlattenedDocsMap(_jsonPathString, _filterJsonExpression);
    _jsonIndexReader.convertFlattenedDocIdsToDocIds(valueToMatchingDocs);
    return buildDistinctResultsBlock(valueToMatchingDocs, docIds != null ? docIds.toRoaringBitmap() : null);
  }

  private DistinctResultsBlock buildDistinctResultsBlock(Map<String, RoaringBitmap> valueToMatchingDocs,
      @Nullable RoaringBitmap filteredDocIds) {
    DistinctTable distinctTable = createDistinctTable();

    // With _skipMissingPath, handleMissingDocs is bypassed — no need to track which docs are still uncovered, so
    // skip the bitmap allocation and per-iteration `andNot` work entirely.
    boolean allDocsCovered = _skipMissingPath;
    RoaringBitmap remainingDocs = _skipMissingPath ? null
        : (filteredDocIds != null ? filteredDocIds.clone() : RoaringBitmap.bitmapOfRange(0L, _totalDocs));
    boolean earlyBreak = false;

    for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingDocs.entrySet()) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numEntriesExaminedPostFilter++, EXPLAIN_NAME);

      String value = entry.getKey();
      RoaringBitmap docIds = entry.getValue();

      // Unfiltered always includes; filtered must intersect the original filter set (not the shrinking
      // `remainingDocs`, since a value can still belong to the result after all filtered docs are covered).
      boolean includeValue = filteredDocIds == null || RoaringBitmap.intersects(docIds, filteredDocIds);

      if (!allDocsCovered && includeValue) {
        remainingDocs.andNot(docIds);
        if (remainingDocs.isEmpty()) {
          allDocsCovered = true;
        }
      }

      // addValueToDistinctTable returns true exactly when the table has reached its LIMIT (no-ORDER-BY case);
      // for ORDER-BY or unbounded LIMIT it always returns false. So no separate size check is needed.
      if (includeValue && addValueToDistinctTable(distinctTable, value)) {
        earlyBreak = true;
        break;
      }
    }

    if (!earlyBreak && !allDocsCovered) {
      handleMissingDocs(distinctTable);
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  private DistinctTable createDistinctTable() {
    int limit = _queryContext.getLimit();
    boolean nullHandlingEnabled = _queryContext.isNullHandlingEnabled();
    switch (_dataType) {
      case INT:
        return new IntDistinctTable(_dataSchema, limit, nullHandlingEnabled, _orderByExpression);
      case LONG:
        return new LongDistinctTable(_dataSchema, limit, nullHandlingEnabled, _orderByExpression);
      case FLOAT:
        return new FloatDistinctTable(_dataSchema, limit, nullHandlingEnabled, _orderByExpression);
      case DOUBLE:
        return new DoubleDistinctTable(_dataSchema, limit, nullHandlingEnabled, _orderByExpression);
      case BIG_DECIMAL:
        return new BigDecimalDistinctTable(_dataSchema, limit, nullHandlingEnabled, _orderByExpression);
      case STRING:
        return new StringDistinctTable(_dataSchema, limit, nullHandlingEnabled, _orderByExpression);
      default:
        throw new IllegalStateException("Unsupported data type for JSON index distinct: " + _dataType);
    }
  }

  private boolean addValueToDistinctTable(DistinctTable distinctTable, String stringValue) {
    switch (_dataType) {
      case INT:
        return addToTable((IntDistinctTable) distinctTable, Integer.parseInt(stringValue));
      case LONG:
        return addToTable((LongDistinctTable) distinctTable, Long.parseLong(stringValue));
      case FLOAT:
        return addToTable((FloatDistinctTable) distinctTable, Float.parseFloat(stringValue));
      case DOUBLE:
        return addToTable((DoubleDistinctTable) distinctTable, Double.parseDouble(stringValue));
      case BIG_DECIMAL:
        return addToTable((BigDecimalDistinctTable) distinctTable, new BigDecimal(stringValue));
      case STRING:
        return addToTable((StringDistinctTable) distinctTable, stringValue);
      default:
        throw new IllegalStateException("Unsupported data type for JSON index distinct: " + _dataType);
    }
  }

  private boolean addToTable(IntDistinctTable table, int value) {
    if (!table.hasLimit()) {
      table.addUnbounded(value);
      return false;
    }
    if (_orderByExpression != null) {
      table.addWithOrderBy(value);
      return false;
    }
    return table.addWithoutOrderBy(value);
  }

  private boolean addToTable(LongDistinctTable table, long value) {
    if (!table.hasLimit()) {
      table.addUnbounded(value);
      return false;
    }
    if (_orderByExpression != null) {
      table.addWithOrderBy(value);
      return false;
    }
    return table.addWithoutOrderBy(value);
  }

  private boolean addToTable(FloatDistinctTable table, float value) {
    if (!table.hasLimit()) {
      table.addUnbounded(value);
      return false;
    }
    if (_orderByExpression != null) {
      table.addWithOrderBy(value);
      return false;
    }
    return table.addWithoutOrderBy(value);
  }

  private boolean addToTable(DoubleDistinctTable table, double value) {
    if (!table.hasLimit()) {
      table.addUnbounded(value);
      return false;
    }
    if (_orderByExpression != null) {
      table.addWithOrderBy(value);
      return false;
    }
    return table.addWithoutOrderBy(value);
  }

  private boolean addToTable(BigDecimalDistinctTable table, BigDecimal value) {
    if (!table.hasLimit()) {
      table.addUnbounded(value);
      return false;
    }
    if (_orderByExpression != null) {
      table.addWithOrderBy(value);
      return false;
    }
    return table.addWithoutOrderBy(value);
  }

  private boolean addToTable(StringDistinctTable table, String value) {
    if (!table.hasLimit()) {
      table.addUnbounded(value);
      return false;
    }
    if (_orderByExpression != null) {
      table.addWithOrderBy(value);
      return false;
    }
    return table.addWithoutOrderBy(value);
  }

  private void handleMissingDocs(DistinctTable distinctTable) {
    if (_defaultValueLiterals != null) {
      for (String literal : _defaultValueLiterals) {
        if (addValueToDistinctTable(distinctTable, literal)) {
          return;
        }
      }
    } else if (_queryContext.isNullHandlingEnabled()) {
      distinctTable.addNull();
    } else {
      throw new RuntimeException(
          String.format("Illegal Json Path: [%s], for some docIds in segment [%s]", _jsonPathString,
              _indexSegment.getSegmentName()));
    }
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of(_filterOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // - numDocsScanned tracks the matching docs
    // - numEntriesScannedInFilter tracks work done while materializing the exact filter bitmap
    // - numEntriesScannedPostFilter tracks values examined
    return new ExecutionStatistics(_numDocsScanned, _numEntriesScannedInFilter, _numEntriesExaminedPostFilter,
        _totalDocs);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME + "(keyColumns:" + _expression + ")";
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putStringList("keyColumns", List.of(_expression.toString()));
  }
}
