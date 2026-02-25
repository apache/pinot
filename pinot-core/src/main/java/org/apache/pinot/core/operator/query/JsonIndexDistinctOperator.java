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
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.RoaringBitmap;


/**
 * POC: Distinct operator that uses JSON index value→docId map directly instead of scanning docs.
 * Avoids projection/transform pipeline for SELECT DISTINCT jsonExtractIndex(...).
 */
public class JsonIndexDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_JSON_INDEX";
  private static final String FUNCTION_NAME = "jsonExtractIndex";

  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;

  private int _numValuesProcessed = 0;

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

    JsonIndexReader jsonIndexReader = getJsonIndexReader(parsed._columnName);
    if (jsonIndexReader == null) {
      throw new IllegalStateException("Column " + parsed._columnName + " has no JSON index");
    }

    // Same logic as JsonExtractIndexTransformFunction.getValueToMatchingDocsMap()
    Map<String, RoaringBitmap> valueToMatchingDocs =
        jsonIndexReader.getMatchingFlattenedDocsMap(parsed._jsonPathString, parsed._filterJsonPath);
    if (parsed._isSingleValue) {
      jsonIndexReader.convertFlattenedDocIdsToDocIds(valueToMatchingDocs);
    }

    RoaringBitmap filteredDocIds = buildFilteredDocIds();

    DataSchema dataSchema = new DataSchema(
        new String[]{expr.toString()},
        new ColumnDataType[]{ColumnDataType.STRING});
    OrderByExpressionContext orderByExpression = _queryContext.getOrderByExpressions() != null
        ? _queryContext.getOrderByExpressions().get(0) : null;
    StringDistinctTable distinctTable = new StringDistinctTable(
        dataSchema, _queryContext.getLimit(), _queryContext.isNullHandlingEnabled(), orderByExpression);

    int limit = _queryContext.getLimit();
    for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingDocs.entrySet()) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numValuesProcessed, EXPLAIN_NAME);
      String value = entry.getKey();
      RoaringBitmap docIds = entry.getValue();

      boolean includeValue;
      if (filteredDocIds == null) {
        includeValue = true;
      } else {
        RoaringBitmap intersection = RoaringBitmap.and(docIds, filteredDocIds);
        includeValue = !intersection.isEmpty();
      }

      if (includeValue) {
        if (distinctTable.hasLimit()) {
          if (orderByExpression != null) {
            distinctTable.addWithOrderBy(value);
          } else {
            if (distinctTable.addWithoutOrderBy(value)) {
              break;
            }
          }
        } else {
          distinctTable.addUnbounded(value);
        }
        _numValuesProcessed++;
      }

      if (distinctTable.hasLimit() && distinctTable.size() >= limit) {
        break;
      }
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  @Nullable
  private JsonIndexReader getJsonIndexReader(String columnName) {
    DataSource dataSource = _indexSegment.getDataSource(columnName, _queryContext.getSchema());
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
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    if (isSingleValue && jsonPathString.contains("[*]")) {
      return null;
    }

    try {
      JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
    } catch (Exception e) {
      return null;
    }

    String filterJsonPath = null;
    if (args.size() == 5 && args.get(4).getType() == ExpressionContext.Type.LITERAL) {
      filterJsonPath = args.get(4).getLiteral().getStringValue();
    }

    return new ParsedJsonExtractIndex(columnName, jsonPathString, isSingleValue, filterJsonPath);
  }

  private static final class ParsedJsonExtractIndex {
    final String _columnName;
    final String _jsonPathString;
    final boolean _isSingleValue;
    @Nullable
    final String _filterJsonPath;

    ParsedJsonExtractIndex(String columnName, String jsonPathString, boolean isSingleValue,
        @Nullable String filterJsonPath) {
      _columnName = columnName;
      _jsonPathString = jsonPathString;
      _isSingleValue = isSingleValue;
      _filterJsonPath = filterJsonPath;
    }
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numValuesProcessed, 0, 0, numTotalDocs);
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
   * Returns true if the expression is jsonExtractIndex on a column with JSON index.
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
    if (dataSource.getJsonIndex() != null) {
      return true;
    }

    //for composite Json index check for key
    Optional<IndexType<?, ?, ?>> compositeIndex =
        IndexService.getInstance().getOptional("composite_json_index");
    return compositeIndex.isPresent() && dataSource.getIndex(compositeIndex.get()) != null;
  }
}
