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
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.JsonExtractIndexUtils.ParsedJsonExtractIndex;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Group-by operator for {@code SELECT jsonExtractIndex(col, path, type[, default]), COUNT(*) ... GROUP BY 1}
 * when {@code col} has a JSON index covering {@code path}.
 *
 * <p>Execution flow:
 * 1. Push a same-path {@code JSON_MATCH} predicate into the JSON-index lookup when it cannot match missing paths.
 * 2. For each distinct value at the path, count the source documents via posting-list cardinality, intersecting
 *    with the residual WHERE bitmap when one is needed.
 * 3. Track documents that have no value at the path; emit a missing-path group consistent with the
 *    {@link JsonIndexDistinctOperator} contract (default literal / null / throw).
 *
 * <p>The output is a {@link GroupByResultsBlock} carrying one {@link IntermediateRecord} per emitted group, which the
 * downstream combine operator merges across segments via {@code CountAggregationFunction.merge} (sums).
 *
 * <p><b>Semantic note on array-traversing paths.</b> The JSON index flattens JSON arrays into one row per element,
 * so a document with {@code {"items": [{"name":"a"},{"name":"b"}]}} contributes to two distinct values at the
 * path {@code $.items.name}. This operator therefore reports per-element counts ("multikey" semantics) — the same
 * doc is counted once per distinct value present in its array. For non-array-traversing paths this matches the
 * scalar {@code jsonExtractScalar} evaluation exactly; for array-traversing paths it diverges. This matches the
 * behavior already established by {@link JsonIndexDistinctOperator}.
 */
public class JsonIndexGroupByOperator extends BaseOperator<GroupByResultsBlock> {
  private static final String EXPLAIN_NAME = "GROUP_BY_JSON_INDEX";
  // Empty order-by-values array reused across IntermediateRecord instances. The combine path reads only key/record,
  // not values, so a shared singleton is safe.
  private static final Comparable<?>[] EMPTY_ORDER_BY_VALUES = new Comparable<?>[0];

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;
  private final DataSchema _dataSchema;
  private final ParsedJsonExtractIndex _parsed;

  private int _numEntriesExamined = 0;
  private long _numEntriesScannedInFilter = 0;

  /**
   * SegmentContext is accepted for parity with {@link JsonIndexDistinctOperator} and the regular
   * {@code GroupByPlanNode} call site. It is intentionally not stored: the data source lookup goes through
   * {@code IndexSegment#getDataSource}, and any upsert / valid-doc-id filtering flows through the
   * {@code BaseFilterOperator} that {@code FilterPlanNode} produced for the caller.
   */
  public JsonIndexGroupByOperator(IndexSegment indexSegment, SegmentContext segmentContext,
      QueryContext queryContext, BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _filterOperator = filterOperator;

    ExpressionContext groupByExpression = queryContext.getGroupByExpressions().get(0);
    _parsed = JsonExtractIndexUtils.parseJsonExtractIndex(groupByExpression);
    Preconditions.checkState(_parsed != null,
        "Expected a parseable jsonExtractIndex group-by expression");

    AggregationFunction<?, ?> countFn = queryContext.getAggregationFunctions()[0];
    _dataSchema = new DataSchema(
        new String[]{groupByExpression.toString(), countFn.getResultColumnName()},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(_parsed._dataType),
            countFn.getIntermediateResultColumnType()});
  }

  /**
   * Returns {@code true} when the query is a single-group-by, single-{@code COUNT(*)} aggregation over a JSON-indexed
   * path, with no {@code HAVING} clause and no filtered aggregations. All other shapes fall back to
   * {@link GroupByOperator}.
   */
  public static boolean canUse(IndexSegment indexSegment, QueryContext queryContext) {
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions == null || groupByExpressions.size() != 1) {
      return false;
    }
    if (!JsonExtractIndexUtils.canUseJsonIndex(indexSegment, groupByExpressions.get(0))) {
      return false;
    }

    AggregationFunction<?, ?>[] aggFns = queryContext.getAggregationFunctions();
    if (aggFns == null || aggFns.length != 1) {
      return false;
    }
    if (aggFns[0].getType() != AggregationFunctionType.COUNT) {
      return false;
    }
    // CountAggregationFunction.getInputExpressions() returns the empty list for plain COUNT(*) (when null handling
    // is disabled). COUNT(col) or null-handling COUNT(*) returns a non-empty list — both rejected for v1.
    if (!aggFns[0].getInputExpressions().isEmpty()) {
      return false;
    }
    if (queryContext.getHavingFilter() != null) {
      return false;
    }
    if (queryContext.hasFilteredAggregations()) {
      return false;
    }
    return true;
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    DataSource dataSource = _indexSegment.getDataSource(_parsed._columnName, _queryContext.getSchema());
    JsonIndexReader jsonIndexReader = JsonExtractIndexUtils.getJsonIndexReader(dataSource);
    Preconditions.checkState(jsonIndexReader != null,
        "Column %s has no JSON index", _parsed._columnName);

    // Decide which slice of the WHERE clause can be evaluated entirely inside the JSON-index lookup.
    String pushedDownFilterJson = JsonExtractIndexUtils.extractSamePathJsonMatchFilter(_parsed,
        _queryContext.getFilter());
    // Don't push a filter that can match missing-path docs into the JSON-index lookup. The JsonIndexReader SPI does
    // not contractually guarantee how implementations represent missing-path matches in the returned map; relying on
    // them to return an empty map for IS_NULL would tie correctness to an implementation detail. Fall back to the
    // row-level filter, which evaluates IS_NULL correctly via the null-value vector / scan path.
    if (pushedDownFilterJson != null
        && JsonExtractIndexUtils.jsonMatchFilterCanMatchMissingPath(pushedDownFilterJson)) {
      pushedDownFilterJson = null;
    }
    boolean filterFullyPushedDown = pushedDownFilterJson != null
        && JsonExtractIndexUtils.isOnlySamePathJsonMatchFilter(_parsed, _queryContext.getFilter());

    // When the filter is fully pushed down we trust the index-side filter and skip materializing the WHERE bitmap;
    // otherwise we evaluate the full filter and intersect per-value posting lists with it.
    RoaringBitmap fullFilterDocs = filterFullyPushedDown ? null : buildFilteredDocIds();
    if (fullFilterDocs != null && fullFilterDocs.isEmpty()) {
      return new GroupByResultsBlock(_dataSchema, Collections.emptyList(), _queryContext);
    }

    Map<String, RoaringBitmap> valueToDocs =
        jsonIndexReader.getMatchingFlattenedDocsMap(_parsed._jsonPathString, pushedDownFilterJson);
    jsonIndexReader.convertFlattenedDocIdsToDocIds(valueToDocs);

    // When the filter is fully pushed down, every doc that lacks the path was already excluded by it, so we don't
    // need to track covered docs or emit a missing-path group.
    RoaringBitmap coveredDocs = filterFullyPushedDown ? null : new RoaringBitmap();

    List<IntermediateRecord> results = new ArrayList<>(valueToDocs.size());
    for (Map.Entry<String, RoaringBitmap> entry : valueToDocs.entrySet()) {
      _numEntriesExamined++;
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numEntriesExamined, EXPLAIN_NAME);

      String value = entry.getKey();
      RoaringBitmap docIds = entry.getValue();

      long count;
      if (fullFilterDocs == null) {
        count = docIds.getLongCardinality();
        if (coveredDocs != null) {
          coveredDocs.or(docIds);
        }
      } else {
        count = RoaringBitmap.andCardinality(docIds, fullFilterDocs);
        if (count == 0) {
          continue;
        }
        coveredDocs.or(docIds);
      }
      if (count == 0) {
        continue;
      }

      Object groupKey = convertValue(value, _parsed._dataType);
      results.add(buildRecord(groupKey, count));
    }

    // Account for documents that had no value at the path. When the filter was fully pushed down it can't match
    // missing-path docs by construction (we required !jsonMatchFilterCanMatchMissingPath), so there are no missing
    // docs to attribute.
    if (!filterFullyPushedDown) {
      long expectedTotal;
      long coveredCard;
      if (fullFilterDocs == null) {
        expectedTotal = _indexSegment.getSegmentMetadata().getTotalDocs();
        coveredCard = coveredDocs.getLongCardinality();
      } else {
        expectedTotal = fullFilterDocs.getLongCardinality();
        // Restrict the "covered" set to docs that survived the WHERE.
        coveredDocs.and(fullFilterDocs);
        coveredCard = coveredDocs.getLongCardinality();
      }
      long missing = expectedTotal - coveredCard;
      if (missing > 0) {
        results.add(buildMissingPathRecord(missing));
      }
    }

    return new GroupByResultsBlock(_dataSchema, results, _queryContext);
  }

  private IntermediateRecord buildMissingPathRecord(long missing) {
    Object missingKey;
    if (_parsed._defaultValueLiteral != null) {
      missingKey = convertValue(_parsed._defaultValueLiteral, _parsed._dataType);
    } else if (_queryContext.isNullHandlingEnabled()) {
      missingKey = null;
    } else {
      throw new RuntimeException(
          String.format("Illegal Json Path: [%s], for some docIds in segment [%s]",
              _parsed._jsonPathString, _indexSegment.getSegmentName()));
    }
    return buildRecord(missingKey, missing);
  }

  private static IntermediateRecord buildRecord(@Nullable Object groupKey, long count) {
    Object[] keyValues = {groupKey};
    Object[] rowValues = {groupKey, count};
    return new IntermediateRecord(new Key(keyValues), new Record(rowValues), EMPTY_ORDER_BY_VALUES);
  }

  private static Object convertValue(String stringValue, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return Integer.parseInt(stringValue);
      case LONG:
        return Long.parseLong(stringValue);
      case FLOAT:
        return Float.parseFloat(stringValue);
      case DOUBLE:
        return Double.parseDouble(stringValue);
      case BIG_DECIMAL:
        return new BigDecimal(stringValue);
      case STRING:
        return stringValue;
      default:
        throw new IllegalStateException("Unsupported data type for JSON index group-by: " + dataType);
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
    return new ExecutionStatistics(0, _numEntriesScannedInFilter, 0, numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder sb = new StringBuilder(EXPLAIN_NAME).append("(groupKey:");
    sb.append(_queryContext.getGroupByExpressions().get(0).toString());
    sb.append(", aggregations:").append(_queryContext.getAggregationFunctions()[0].toExplainString());
    return sb.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putStringList("groupKeys",
        List.of(_queryContext.getGroupByExpressions().get(0).toString()));
    attributeBuilder.putStringList("aggregations",
        List.of(_queryContext.getAggregationFunctions()[0].toExplainString()));
  }
}
