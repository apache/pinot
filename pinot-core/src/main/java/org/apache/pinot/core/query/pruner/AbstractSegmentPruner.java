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

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;


/**
 * Abstract implementation for {@link SegmentPruner}, containing code common to
 * sub classes.
 *
 */
public abstract class AbstractSegmentPruner implements SegmentPruner {

  public abstract boolean pruneSegment(FilterQueryTree filterQueryTree, Map<String, ColumnMetadata> columnMetadataMap,
      Map<String, BloomFilterReader> bloomFilterMap);

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
      @Nonnull Map<String, ColumnMetadata> columnMetadataMap, Map<String, BloomFilterReader> bloomFilterMap) {
    List<FilterQueryTree> children = filterQueryTree.getChildren();

    if (children.isEmpty()) {
      return false;
    }

    FilterOperator filterOperator = filterQueryTree.getOperator();
    switch (filterOperator) {
      case AND:
        for (FilterQueryTree child : children) {
          if (pruneSegment(child, columnMetadataMap, bloomFilterMap)) {
            return true;
          }
        }
        return false;

      case OR:
        for (FilterQueryTree child : children) {
          if (!pruneSegment(child, columnMetadataMap, bloomFilterMap)) {
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
   * @apiNote It is assumed that the 'input' here is a value taken from the query, so this method should not be used for
   * other internal purposes.
   */
  protected static Comparable getValue(@Nonnull String input, @Nonnull FieldSpec.DataType dataType) {
    try {
      if (dataType != FieldSpec.DataType.BYTES) {
        return (Comparable) dataType.convert(input);
      } else {
        return BytesUtils.toByteArray(input);
      }
    } catch (Exception e) {
      throw new BadQueryRequestException(e);
    }
  }
}
