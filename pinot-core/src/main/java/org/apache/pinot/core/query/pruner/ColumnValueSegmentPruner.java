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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;


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

    Map<String, BloomFilterReader> bloomFilterMap = new HashMap<>();
    if (columnMetadataMap != null) {
      for (String column : columnMetadataMap.keySet()) {
        BloomFilterReader bloomFilterReader = segment.getDataSource(column).getBloomFilter();
        if (bloomFilterReader != null) {
          bloomFilterMap.put(column, bloomFilterReader);
        }
      }
    }
    return (columnMetadataMap != null) && pruneSegment(filterQueryTree, columnMetadataMap, bloomFilterMap);
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
      @Nonnull Map<String, ColumnMetadata> columnMetadataMap, Map<String, BloomFilterReader> bloomFilterMap) {
    FilterOperator filterOperator = filterQueryTree.getOperator();
    List<FilterQueryTree> children = filterQueryTree.getChildren();

    if (children == null || children.isEmpty()) {
      // Leaf Node

      //skip expressions
      if(filterQueryTree.getExpression()!= null && !filterQueryTree.getExpression().isColumn()){
        return false;
      }
      // Skip operator other than EQUALITY and RANGE
      if ((filterOperator != FilterOperator.EQUALITY) && (filterOperator != FilterOperator.RANGE)) {
        return false;
      }

      String column = filterQueryTree.getColumn();
      ColumnMetadata columnMetadata = columnMetadataMap.get(column);
      if (columnMetadata == null) {
        // Should not reach here after DataSchemaSegmentPruner
        return true;
      }

      Comparable minValue = columnMetadata.getMinValue();
      Comparable maxValue = columnMetadata.getMaxValue();

      if (filterOperator == FilterOperator.EQUALITY) {
        // EQUALITY
        boolean pruneSegment = false;
        FieldSpec.DataType dataType = columnMetadata.getDataType();
        Comparable value = getValue(filterQueryTree.getValue().get(0), dataType);

        // Check if the value is in the min/max range
        if (minValue != null && maxValue != null) {
          pruneSegment = (value.compareTo(minValue) < 0) || (value.compareTo(maxValue) > 0);
        }

        // If the bloom filter is available for the column, check if the value may exist
        if (!pruneSegment && bloomFilterMap.containsKey(column)) {
          BloomFilterReader bloomFilterReader = bloomFilterMap.get(column);
          pruneSegment = !bloomFilterReader.mightContain(value);
        }

        return pruneSegment;
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
      return pruneNonLeaf(filterQueryTree, columnMetadataMap, bloomFilterMap);
    }
  }
}
