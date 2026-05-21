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

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Shared parsing and filter-pushdown helpers for index-aware operators that consume a scalar
 * {@code jsonExtractIndex(column, path, type[, defaultValue])} expression and a JSON index on the column.
 */
public final class JsonExtractIndexUtils {
  private static final String FUNCTION_NAME_EXTRACT_INDEX = "jsonExtractIndex";

  private JsonExtractIndexUtils() {
  }

  /**
   * Parsed view of a {@code jsonExtractIndex} call.
   */
  public static final class ParsedJsonExtractIndex {
    public final String _columnName;
    public final String _jsonPathString;
    public final FieldSpec.DataType _dataType;
    @Nullable
    public final String _defaultValueLiteral;

    public ParsedJsonExtractIndex(String columnName, String jsonPathString, FieldSpec.DataType dataType,
        @Nullable String defaultValueLiteral) {
      _columnName = columnName;
      _jsonPathString = jsonPathString;
      _dataType = dataType;
      _defaultValueLiteral = defaultValueLiteral;
    }
  }

  /**
   * Parses the given expression as a 3/4-arg scalar {@code jsonExtractIndex} call.
   * Returns {@code null} when the expression has a different shape (wrong arity, non-literal args, MV result type,
   * unsupported scalar type, {@code [*]} wildcard, or malformed JSON path).
   *
   * <p>This is a pure shape check; it does not consult segment metadata.
   * Pair with {@link #canUseJsonIndex} to verify that the column has a usable JSON index on the path.
   */
  @Nullable
  public static ParsedJsonExtractIndex parseJsonExtractIndex(ExpressionContext expr) {
    if (expr.getType() != ExpressionContext.Type.FUNCTION) {
      return null;
    }
    String functionName = expr.getFunction().getFunctionName();
    if (!FUNCTION_NAME_EXTRACT_INDEX.equalsIgnoreCase(functionName)) {
      return null;
    }
    List<ExpressionContext> args = expr.getFunction().getArguments();
    if (args.size() != 3 && args.size() != 4) {
      return null;
    }
    if (args.get(0).getType() != ExpressionContext.Type.IDENTIFIER) {
      return null;
    }
    if (args.get(1).getType() != ExpressionContext.Type.LITERAL
        || args.get(2).getType() != ExpressionContext.Type.LITERAL
        || (args.size() == 4 && args.get(3).getType() != ExpressionContext.Type.LITERAL)) {
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

    String defaultValueLiteral = null;
    if (args.size() == 4) {
      defaultValueLiteral = args.get(3).getLiteral().getStringValue();
      try {
        dataType.convert(defaultValueLiteral);
      } catch (Exception e) {
        return null;
      }
    }

    return new ParsedJsonExtractIndex(columnName, jsonPathString, dataType, defaultValueLiteral);
  }

  /**
   * Returns the JSON index reader for the column if present, falling back to a composite JSON index when available.
   */
  @Nullable
  public static JsonIndexReader getJsonIndexReader(DataSource dataSource) {
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

  /**
   * Returns {@code true} when the expression is a parseable {@code jsonExtractIndex} call and the referenced column
   * has a usable JSON index covering the path.
   */
  public static boolean canUseJsonIndex(IndexSegment indexSegment, ExpressionContext expr) {
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
    return reader.isPathIndexed(parsed._jsonPathString);
  }

  /**
   * Walks the filter and returns the inner JSON-match string for a single same-column-same-path {@code JSON_MATCH}
   * predicate that can be pushed into the JSON-index lookup. Returns {@code null} when no such predicate exists,
   * or when more than one candidate is found and disambiguation would be unsafe.
   */
  @Nullable
  public static String extractSamePathJsonMatchFilter(ParsedJsonExtractIndex parsed, @Nullable FilterContext filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case PREDICATE:
        return extractSamePathJsonMatchFilter(parsed, filter.getPredicate());
      case AND:
        String matchingFilter = null;
        for (FilterContext child : filter.getChildren()) {
          String childFilter = extractSamePathJsonMatchFilter(parsed, child);
          if (childFilter == null) {
            continue;
          }
          if (matchingFilter != null) {
            return null;
          }
          matchingFilter = childFilter;
        }
        return matchingFilter;
      default:
        return null;
    }
  }

  /**
   * Returns {@code true} when the filter is exactly a single same-path JSON_MATCH predicate (no other clauses).
   */
  public static boolean isOnlySamePathJsonMatchFilter(ParsedJsonExtractIndex parsed, @Nullable FilterContext filter) {
    if (filter == null || filter.getType() != FilterContext.Type.PREDICATE) {
      return false;
    }
    return extractSamePathJsonMatchFilter(parsed, filter.getPredicate()) != null;
  }

  /**
   * Returns {@code true} when the filter string can match documents that lack the JSON path entirely (i.e. an
   * {@code IS_NULL} on the path). Callers use this to decide whether they must still account for missing-path
   * documents after running an index-side filter.
   */
  public static boolean jsonMatchFilterCanMatchMissingPath(String filterJsonString) {
    try {
      FilterContext filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterJsonString));
      return filter.getType() == FilterContext.Type.PREDICATE
          && filter.getPredicate().getType() == Predicate.Type.IS_NULL;
    } catch (Exception e) {
      return false;
    }
  }

  @Nullable
  private static String extractSamePathJsonMatchFilter(ParsedJsonExtractIndex parsed, Predicate predicate) {
    if (!(predicate instanceof JsonMatchPredicate)) {
      return null;
    }
    ExpressionContext lhs = predicate.getLhs();
    if (lhs.getType() != ExpressionContext.Type.IDENTIFIER
        || !parsed._columnName.equals(lhs.getIdentifier())) {
      return null;
    }
    String filterJsonString = ((JsonMatchPredicate) predicate).getValue();
    int start = filterJsonString.indexOf('"');
    if (start < 0) {
      return null;
    }
    int end = filterJsonString.indexOf('"', start + 1);
    if (end < 0) {
      return null;
    }
    String filterPath = filterJsonString.substring(start + 1, end);
    return parsed._jsonPathString.equals(filterPath) ? filterJsonString : null;
  }
}
