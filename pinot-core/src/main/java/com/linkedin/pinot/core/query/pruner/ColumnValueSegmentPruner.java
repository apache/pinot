/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;


/**
 * An implementation of SegmentPruner.
 * <p>Pruner will prune segment based on the column value inside the filter.
 */
public class ColumnValueSegmentPruner extends AbstractSegmentPruner {

  @Override
  public void init(Configuration config) {
  }

  @Override
  public boolean prune(@Nonnull IndexSegment segment, @Nonnull ServerQueryRequest queryRequest) {
    FilterQueryTree filterQueryTree = queryRequest.getFilterQueryTree();
    if (filterQueryTree == null) {
      return false;
    }
    // For realtime segment, this map can be null.
    Map<String, ColumnMetadata> columnMetadataMap =
        ((SegmentMetadataImpl) segment.getSegmentMetadata()).getColumnMetadataMap();
    return (columnMetadataMap != null) && pruneSegment(filterQueryTree, columnMetadataMap);
  }

  @Override
  public String toString() {
    return "ColumnValueSegmentPruner";
  }

  /**
   * Helper method to determine if a segment can be pruned based on the column min/max value in segment metadata and
   * the predicates on time column. The algorithm is as follows:
   *
   * <ul>
   *   <li> For leaf node: Returns true if there is a predicate on the column and apply the predicate would result in
   *   filtering out all docs of the segment, false otherwise. </li>
   *   <li> For non-leaf AND node: True if any of its children returned true, false otherwise. </li>
   *   <li> For non-leaf OR node: True if all its children returned true, false otherwise. </li>
   * </ul>
   *
   * @param filterQueryTree Filter tree for the query.
   * @param columnMetadataMap Map from column name to column metadata.
   * @return True if segment can be pruned out, false otherwise.
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean pruneSegment(@Nonnull FilterQueryTree filterQueryTree,
      @Nonnull Map<String, ColumnMetadata> columnMetadataMap) {
    FilterOperator filterOperator = filterQueryTree.getOperator();
    List<FilterQueryTree> children = filterQueryTree.getChildren();

    if (children == null || children.isEmpty()) {
      // Leaf Node

      // Skip operator other than EQUALITY and RANGE
      if ((filterOperator != FilterOperator.EQUALITY) && (filterOperator != FilterOperator.RANGE)) {
        return false;
      }

      ColumnMetadata columnMetadata = columnMetadataMap.get(filterQueryTree.getColumn());
      if (columnMetadata == null) {
        // Should not reach here after DataSchemaSegmentPruner
        return true;
      }

      Comparable minValue = columnMetadata.getMinValue();
      Comparable maxValue = columnMetadata.getMaxValue();

      if (filterOperator == FilterOperator.EQUALITY) {
        // EQUALITY

        // Doesn't have min/max value set in metadata
        if ((minValue == null) || (maxValue == null)) {
          return false;
        }

        // Check if the value is in the min/max range
        FieldSpec.DataType dataType = columnMetadata.getDataType();
        Comparable value = getValue(filterQueryTree.getValue().get(0), dataType);
        return (value.compareTo(minValue) < 0) || (value.compareTo(maxValue) > 0);
      } else {
        // RANGE

        // Get lower/upper boundary value
        FieldSpec.DataType dataType = columnMetadata.getDataType();
        RangePredicate rangePredicate = new RangePredicate(null, filterQueryTree.getValue());
        String lowerBoundary = rangePredicate.getLowerBoundary();
        boolean includeLowerBoundary = rangePredicate.includeLowerBoundary();
        Comparable lowerBoundaryValue = null;
        if (!lowerBoundary.equals(RangePredicate.UNBOUNDED)) {
          lowerBoundaryValue = getValue(lowerBoundary, dataType);
        }
        String upperBoundary = rangePredicate.getUpperBoundary();
        boolean includeUpperBoundary = rangePredicate.includeUpperBoundary();
        Comparable upperBoundaryValue = null;
        if (!upperBoundary.equals(RangePredicate.UNBOUNDED)) {
          upperBoundaryValue = getValue(upperBoundary, dataType);
        }

        // Check if the range is valid
        if ((lowerBoundaryValue != null) && (upperBoundaryValue != null)) {
          if (includeLowerBoundary && includeUpperBoundary) {
            if (lowerBoundaryValue.compareTo(upperBoundaryValue) > 0) {
              return true;
            }
          } else {
            if (lowerBoundaryValue.compareTo(upperBoundaryValue) >= 0) {
              return true;
            }
          }
        }

        // Doesn't have min/max value set in metadata
        if ((minValue == null) || (maxValue == null)) {
          return false;
        }

        if (lowerBoundaryValue != null) {
          if (includeLowerBoundary) {
            if (lowerBoundaryValue.compareTo(maxValue) > 0) {
              return true;
            }
          } else {
            if (lowerBoundaryValue.compareTo(maxValue) >= 0) {
              return true;
            }
          }
        }
        if (upperBoundaryValue != null) {
          if (includeUpperBoundary) {
            if (upperBoundaryValue.compareTo(minValue) < 0) {
              return true;
            }
          } else {
            if (upperBoundaryValue.compareTo(minValue) <= 0) {
              return true;
            }
          }
        }
        return false;
      }
    } else {
      // Parent node
      return pruneNonLeaf(filterQueryTree, columnMetadataMap);
    }
  }
}
