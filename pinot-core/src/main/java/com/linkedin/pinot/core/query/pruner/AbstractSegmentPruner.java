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

import java.util.List;
import java.util.Map;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import javax.annotation.Nonnull;


/**
 * Abstract implementation for {@link SegmentPruner}, containing code common to
 * sub classes.
 *
 */
public abstract class AbstractSegmentPruner implements SegmentPruner {

  public abstract boolean pruneSegment(FilterQueryTree filterQueryTree, Map<String, ColumnMetadata> columnMetadataMap);

  /**
   * Given a non leaf filter query tree node prunes it as follows:
   * <ul>
   *   <li> For 'AND', node is pruned as long as at least one child can prune it. </li>
   *   <li> For 'OR', node is pruned as long as all children can prune it. </li>
   * </ul>
   *
   * @param filterQueryTree Non leaf node in the filter query tree.
   * @param columnMetadataMap Map for column metadata.
   *
   * @return True to prune, false otherwise
   */
  protected boolean pruneNonLeaf(@Nonnull FilterQueryTree filterQueryTree,
      @Nonnull Map<String, ColumnMetadata> columnMetadataMap) {
    List<FilterQueryTree> children = filterQueryTree.getChildren();

    if (children.isEmpty()) {
      return false;
    }

    FilterOperator filterOperator = filterQueryTree.getOperator();
    switch (filterOperator) {
      case AND:
        for (FilterQueryTree child : children) {
          if (pruneSegment(child, columnMetadataMap)) {
            return true;
          }
        }
        return false;

      case OR:
        for (FilterQueryTree child : children) {
          if (!pruneSegment(child, columnMetadataMap)) {
            return false;
          }
        }
        return true;

      default:
        throw new IllegalStateException("Unsupported filter operator: " + filterOperator);
    }
  }

  /**
   * Helper method to get value of the specified type from the given string.
   * @param input Input String for which to get the value
   * @param dataType Data type to construct from the String.
   * @return Comparable value of specified data type built from the input String.
   * @note It is assumed that the 'input' here is a value taken from the query, so this method
   * should not be used to for other internal purposes.
   */
  protected static Comparable getValue(@Nonnull String input, @Nonnull FieldSpec.DataType dataType) {
    try {
      switch (dataType) {
        case INT:
          return Integer.valueOf(input);
        case LONG:
          return Long.valueOf(input);
        case FLOAT:
          return Float.valueOf(input);
        case DOUBLE:
          return Double.valueOf(input);
        case STRING:
          return input;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType);
      }
    } catch (NumberFormatException e) {
      throw new BadQueryRequestException(e);
    }
  }
}
