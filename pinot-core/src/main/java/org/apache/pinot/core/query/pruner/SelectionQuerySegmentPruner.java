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
package org.apache.pinot.core.query.pruner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Segment pruner for selection queries.
 * <ul>
 *   <li>For selection query with LIMIT 0, keep 1 segment to create the data schema</li>
 *   <li>For selection only query without filter, keep enough documents to fulfill the LIMIT requirement</li>
 *   <li>
 *     For selection order-by query, if the first order-by expression is an identifier (column), prune segments based on
 *     the column min/max value and keep enough documents to fulfill the LIMIT and OFFSET requirement. This works both
 *     without a filter and with a filter: with a filter, each segment contributes towards the LIMIT only the number of
 *     rows that <em>provably</em> match the filter based on min/max metadata (its total docs if it fully matches, 0
 *     otherwise). Using this lower bound on matching rows keeps the boundary safe, so segments are never pruned when
 *     they might still hold a top-n matching row. The optimization is skipped when null handling is active for the
 *     order-by/predicate columns because nulls are stored as a default value that pollutes the min/max metadata
 *     (see #18685).
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SelectionQuerySegmentPruner implements SegmentPruner {

  @Override
  public void init(PinotConfiguration config) {
  }

  @Override
  public boolean isApplicableTo(QueryContext query) {
    if (!QueryContextUtils.isSelectionQuery(query)) {
      return false;
    }
    // Without a filter (or for LIMIT 0, where we just keep one segment for the schema), pruning is always applicable.
    if (query.getFilter() == null || query.getLimit() == 0) {
      return true;
    }
    // With a filter, only the order-by-on-identifier path can prune safely (the selection-only path relies on exact
    // doc counts, which a filter invalidates). Additionally, null handling must not be active for the order-by column,
    // because nulls are stored as a default value that pollutes the column min/max metadata used for sorting and the
    // boundary.
    List<OrderByExpressionContext> orderByExpressions = query.getOrderByExpressions();
    if (orderByExpressions == null) {
      return false;
    }
    ExpressionContext firstOrderByExpression = orderByExpressions.get(0).getExpression();
    return firstOrderByExpression.getType() == ExpressionContext.Type.IDENTIFIER
        && !isNullHandlingActive(query, firstOrderByExpression.getIdentifier());
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty()) {
      return segments;
    }

    // For LIMIT 0 case, keep one segment to create the schema
    int limit = query.getLimit();
    if (limit == 0) {
      return List.of(segments.get(0));
    }

    // Skip pruning segments for upsert table because valid doc index is equivalent to a filter
    if (segments.get(0).getValidDocIds() != null) {
      return segments;
    }

    if (query.getOrderByExpressions() == null) {
      // Count-based selection-only pruning is only safe without a filter (total docs is an exact match count). With a
      // filter present this path is not selected (see isApplicableTo); guard defensively in case it is reached.
      return query.getFilter() == null ? pruneSelectionOnly(segments, query) : segments;
    } else {
      return pruneSelectionOrderBy(segments, query);
    }
  }

  /**
   * Helper method to prune segments for selection only queries without filter.
   * <p>We just need to keep enough documents to fulfill the LIMIT requirement.
   */
  private List<IndexSegment> pruneSelectionOnly(List<IndexSegment> segments, QueryContext query) {
    List<IndexSegment> selectedSegments = new ArrayList<>(segments.size());
    int remainingDocs = query.getLimit();
    for (IndexSegment segment : segments) {
      if (remainingDocs > 0) {
        selectedSegments.add(segment);
        remainingDocs -= segment.getSegmentMetadata().getTotalDocs();
      } else {
        break;
      }
    }
    return selectedSegments;
  }

  /**
   * Helper method to prune segments for selection order-by queries.
   * <p>When the first order-by expression is an identifier (column), we can prune segments based on the column min/max
   * value:
   * <ul>
   *   <li>1. Sort all the segments by the column min/max value</li>
   *   <li>2. Pick the top segments until we get enough documents to fulfill the LIMIT and OFFSET requirement</li>
   *   <li>3. Keep the segments that has value overlap with the selected ones; remove the others</li>
   * </ul>
   * <p>Each segment contributes towards the LIMIT only its {@link #guaranteedMatchingDocs} (a lower bound on its
   * matching rows), so the optimization remains correct when a filter is present.
   */
  private List<IndexSegment> pruneSelectionOrderBy(List<IndexSegment> segments, QueryContext query) {
    List<OrderByExpressionContext> orderByExpressions = query.getOrderByExpressions();
    assert orderByExpressions != null;
    int numOrderByExpressions = orderByExpressions.size();
    assert numOrderByExpressions > 0;
    OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
    if (firstOrderByExpression.getExpression().getType() != ExpressionContext.Type.IDENTIFIER) {
      return segments;
    }
    String firstOrderByColumn = firstOrderByExpression.getExpression().getIdentifier();

    // Extract the column min/max value from each segment
    int numSegments = segments.size();
    List<IndexSegment> selectedSegments = new ArrayList<>(numSegments);
    List<MinMaxValue> minMaxValues = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      IndexSegment segment = segments.get(i);
      DataSourceMetadata dataSourceMetadata =
          segment.getDataSource(firstOrderByColumn, query.getSchema()).getDataSourceMetadata();
      Comparable minValue = dataSourceMetadata.getMinValue();
      Comparable maxValue = dataSourceMetadata.getMaxValue();
      // Always keep the segment if it does not have column min/max value in the metadata
      if (minValue == null || maxValue == null) {
        selectedSegments.add(segment);
      } else {
        minMaxValues.add(new MinMaxValue(i, minValue, maxValue));
      }
    }
    if (minMaxValues.isEmpty()) {
      return segments;
    }

    int remainingDocs = query.getLimit() + query.getOffset();
    if (firstOrderByExpression.isAsc()) {
      // For ascending order, sort on column max value in ascending order
      try {
        minMaxValues.sort(Comparator.comparing(o -> o._maxValue));
      } catch (Exception e) {
        // Skip the pruning when segments have different data types for the first order-by column
        return segments;
      }

      // Maintain the max value for all the selected segments
      Comparable maxValue = null;
      for (MinMaxValue minMaxValue : minMaxValues) {
        IndexSegment segment = segments.get(minMaxValue._index);
        if (remainingDocs > 0) {
          selectedSegments.add(segment);
          remainingDocs -= guaranteedMatchingDocs(segment, query);
          maxValue = minMaxValue._maxValue;
        } else {
          // After getting enough documents, prune all the segments with min value larger than the current max value, or
          // min value equal to the current max value and there is only one order-by expression
          assert maxValue != null;
          int result = minMaxValue._minValue.compareTo(maxValue);
          if (result < 0 || (result == 0 && numOrderByExpressions != 1)) {
            selectedSegments.add(segment);
          }
        }
      }
    } else {
      // For descending order, sort on column min value in descending order
      try {
        minMaxValues.sort((o1, o2) -> o2._minValue.compareTo(o1._minValue));
      } catch (Exception e) {
        // Skip the pruning when segments have different data types for the first order-by column
        return segments;
      }

      // Maintain the min value for all the selected segments
      Comparable minValue = null;
      for (MinMaxValue minMaxValue : minMaxValues) {
        IndexSegment segment = segments.get(minMaxValue._index);
        if (remainingDocs > 0) {
          selectedSegments.add(segment);
          remainingDocs -= guaranteedMatchingDocs(segment, query);
          minValue = minMaxValue._minValue;
        } else {
          // After getting enough documents, prune all the segments with max value smaller than the current min value,
          // or max value equal to the current min value and there is only one order-by expression
          assert minValue != null;
          int result = minMaxValue._maxValue.compareTo(minValue);
          if (result > 0 || (result == 0 && numOrderByExpressions != 1)) {
            selectedSegments.add(segment);
          }
        }
      }
    }

    return selectedSegments;
  }

  /**
   * Returns a lower bound on the number of rows in the segment that match the query filter, used to decide how many
   * leading segments must be kept to guarantee the LIMIT + OFFSET requirement.
   * <ul>
   *   <li>Without a filter, every row matches, so this is the exact total doc count.</li>
   *   <li>With a filter, this is the total doc count if the segment <em>provably</em> matches the filter for all of its
   *   rows (based on min/max metadata), and 0 otherwise. Using 0 for segments that only partially (or not provably
   *   fully) match is a safe under-count: such segments are still kept (they overlap the boundary), but they never let
   *   the boundary advance past rows they might contain.</li>
   * </ul>
   */
  private long guaranteedMatchingDocs(IndexSegment segment, QueryContext query) {
    int totalDocs = segment.getSegmentMetadata().getTotalDocs();
    FilterContext filter = query.getFilter();
    if (filter == null) {
      return totalDocs;
    }
    return fullyMatches(segment, filter, query) ? totalDocs : 0;
  }

  /**
   * Returns {@code true} only if <em>all</em> rows of the segment provably satisfy the filter, based on min/max
   * metadata. A {@code false} result never means "does not match"; it means "cannot prove that all rows match", which
   * is always safe to treat as a 0 lower bound. NOT and unsupported predicates conservatively return {@code false}.
   */
  private boolean fullyMatches(IndexSegment segment, FilterContext filter, QueryContext query) {
    switch (filter.getType()) {
      case AND:
        assert filter.getChildren() != null;
        for (FilterContext child : filter.getChildren()) {
          if (!fullyMatches(segment, child, query)) {
            return false;
          }
        }
        return true;
      case OR:
        assert filter.getChildren() != null;
        for (FilterContext child : filter.getChildren()) {
          if (fullyMatches(segment, child, query)) {
            return true;
          }
        }
        return false;
      case CONSTANT:
        return filter.isConstantTrue();
      case PREDICATE:
        return predicateFullyMatches(segment, filter.getPredicate(), query);
      case NOT:
      default:
        return false;
    }
  }

  /**
   * Returns {@code true} only if all rows of the segment provably satisfy the predicate, based on the predicate
   * column's min/max metadata. Only identifier predicates on non-nullable columns of types {@code RANGE} (e.g.
   * {@code >, >=, <, <=}), {@code EQ} ({@code =}) and {@code NOT_EQ} ({@code <>}) are supported; everything else
   * conservatively returns {@code false}.
   */
  private boolean predicateFullyMatches(IndexSegment segment, Predicate predicate, QueryContext query) {
    ExpressionContext lhs = predicate.getLhs();
    if (lhs.getType() != ExpressionContext.Type.IDENTIFIER) {
      return false;
    }
    String column = lhs.getIdentifier();
    // Nulls are stored as a default value that pollutes the column min/max metadata (and are excluded from comparisons
    // under null handling), so full-match cannot be reasoned about when null handling is active for the column.
    if (isNullHandlingActive(query, column)) {
      return false;
    }
    DataSource dataSource = segment.getDataSource(column, query.getSchema());
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    Comparable minValue = dataSourceMetadata.getMinValue();
    Comparable maxValue = dataSourceMetadata.getMaxValue();
    if (minValue == null || maxValue == null) {
      return false;
    }
    // NaN (FLOAT/DOUBLE) breaks the ordering assumptions: Float/Double.compareTo treats NaN as the largest value, which
    // does not match filter semantics (comparisons against NaN are never true). A NaN min/max could make a segment look
    // fully-matching when its NaN rows actually do not match, so refuse to reason about full match in that case.
    if (isNaN(minValue) || isNaN(maxValue)) {
      return false;
    }
    DataType dataType = dataSourceMetadata.getDataType();
    try {
      switch (predicate.getType()) {
        case RANGE:
          return rangeFullyMatches((RangePredicate) predicate, minValue, maxValue, dataType);
        case EQ: {
          Comparable value = convertValue(((EqPredicate) predicate).getValue(), dataType);
          // All rows equal the value iff the whole segment collapses to that single value.
          return minValue.compareTo(value) == 0 && maxValue.compareTo(value) == 0;
        }
        case NOT_EQ: {
          Comparable value = convertValue(((NotEqPredicate) predicate).getValue(), dataType);
          // All rows differ from the value iff the value lies outside [min, max].
          return value.compareTo(minValue) < 0 || value.compareTo(maxValue) > 0;
        }
        default:
          return false;
      }
    } catch (Exception e) {
      // Different data types / unparseable literal: cannot prove full match.
      return false;
    }
  }

  /**
   * Returns {@code true} if the segment's whole {@code [minValue, maxValue]} range provably satisfies the range
   * predicate (i.e. it is fully contained within the predicate's bounds).
   */
  private boolean rangeFullyMatches(RangePredicate predicate, Comparable minValue, Comparable maxValue,
      DataType dataType) {
    String lowerBound = predicate.getLowerBound();
    if (!lowerBound.equals(RangePredicate.UNBOUNDED)) {
      Comparable lowerBoundValue = convertValue(lowerBound, dataType);
      if (predicate.isLowerInclusive()) {
        if (minValue.compareTo(lowerBoundValue) < 0) {
          return false;
        }
      } else {
        if (minValue.compareTo(lowerBoundValue) <= 0) {
          return false;
        }
      }
    }
    String upperBound = predicate.getUpperBound();
    if (!upperBound.equals(RangePredicate.UNBOUNDED)) {
      Comparable upperBoundValue = convertValue(upperBound, dataType);
      if (predicate.isUpperInclusive()) {
        if (maxValue.compareTo(upperBoundValue) > 0) {
          return false;
        }
      } else {
        if (maxValue.compareTo(upperBoundValue) >= 0) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns whether null handling is active for the column, in which case this optimization must be skipped. Pinot
   * stores nulls as a default value that pollutes the column min/max metadata and the total doc count; only when null
   * handling is enabled are those null rows excluded from comparisons, which is when the pollution becomes unsafe to
   * reason about (with null handling disabled, nulls are simply the default value and the min/max stay accurate). This
   * mirrors the null caution in {@code SelectionPlanNode#isSorted}.
   * <p>A column carries null semantics only when null handling is enabled for the query <b>and</b> the column is
   * nullable. Nullability follows the same resolution used at segment build time (e.g.
   * {@code BaseSegmentCreator#isNullable}): under column-based null handling the per-column
   * {@link FieldSpec#isNullable} flag, otherwise (table/query-level null handling) all columns are nullable. The check
   * is conservative: an unknown schema or column is treated as null-handling-active.
   */
  private static boolean isNullHandlingActive(QueryContext query, String column) {
    if (!query.isNullHandlingEnabled()) {
      // Null semantics are off: nulls are just the default value, so min/max and doc counts stay accurate.
      return false;
    }
    Schema schema = query.getSchema();
    if (schema == null) {
      return true;
    }
    if (schema.isEnableColumnBasedNullHandling()) {
      // Column-based null handling: only nullable columns carry nulls.
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      return fieldSpec == null || fieldSpec.isNullable();
    }
    // Table/query-level (legacy) null handling applies null semantics to all columns.
    return true;
  }

  private static boolean isNaN(Comparable value) {
    return (value instanceof Double && ((Double) value).isNaN())
        || (value instanceof Float && ((Float) value).isNaN());
  }

  /**
   * Converts a predicate literal to the column's stored type. Any parse failure propagates to the caller, which treats
   * it as "cannot prove full match" (this pruner must stay conservative; an actually invalid query is rejected by the
   * preceding {@link ColumnValueSegmentPruner} or by query execution).
   */
  private static Comparable convertValue(String stringValue, DataType dataType) {
    return dataType.convertInternal(stringValue);
  }

  private static class MinMaxValue {
    final int _index;
    final Comparable _minValue;
    final Comparable _maxValue;

    private MinMaxValue(int index, Comparable minValue, Comparable maxValue) {
      _index = index;
      _minValue = minValue;
      _maxValue = maxValue;
    }
  }
}
