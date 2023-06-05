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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Segment pruner for selection queries.
 * <ul>
 *   <li>For selection query with LIMIT 0, keep 1 segment to create the data schema</li>
 *   <li>For selection only query without filter, keep enough documents to fulfill the LIMIT requirement</li>
 *   <li>
 *     For selection order-by query without filer, if the first order-by expression is an identifier (column), prune
 *     segments based on the column min/max value and keep enough documents to fulfill the LIMIT and OFFSET requirement.
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
    // Only prune selection queries
    // If LIMIT is not 0, only prune segments for selection queries without filter
    return QueryContextUtils.isSelectionQuery(query)
        && (query.getFilter() == null || query.getLimit() == 0);
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty()) {
      return segments;
    }

    // For LIMIT 0 case, keep one segment to create the schema
    int limit = query.getLimit();
    if (limit == 0) {
      return Collections.singletonList(segments.get(0));
    }

    // Skip pruning segments for upsert table because valid doc index is equivalent to a filter
    if (segments.get(0).getValidDocIds() != null) {
      return segments;
    }

    if (query.getOrderByExpressions() == null) {
      return pruneSelectionOnly(segments, query);
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
   * Helper method to prune segments for selection order-by queries without filter.
   * <p>When the first order-by expression is an identifier (column), we can prune segments based on the column min/max
   * value:
   * <ul>
   *   <li>1. Sort all the segments by the column min/max value</li>
   *   <li>2. Pick the top segments until we get enough documents to fulfill the LIMIT and OFFSET requirement</li>
   *   <li>3. Keep the segments that has value overlap with the selected ones; remove the others</li>
   * </ul>
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
    String firstOrderByColumn = firstOrderByExpression.getExpression().getIdentifierName();

    // Extract the column min/max value from each segment
    int numSegments = segments.size();
    List<IndexSegment> selectedSegments = new ArrayList<>(numSegments);
    List<MinMaxValue> minMaxValues = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      IndexSegment segment = segments.get(i);
      DataSourceMetadata dataSourceMetadata = segment.getDataSource(firstOrderByColumn).getDataSourceMetadata();
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
          remainingDocs -= segment.getSegmentMetadata().getTotalDocs();
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
          remainingDocs -= segment.getSegmentMetadata().getTotalDocs();
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
