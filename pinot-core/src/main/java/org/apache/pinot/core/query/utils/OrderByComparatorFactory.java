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
package org.apache.pinot.core.query.utils;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * A utility class used to create comparators that should be used by operators that implements order by semantics.
 */
public class OrderByComparatorFactory {
  private OrderByComparatorFactory() {
  }

  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      TransformResultMetadata[] orderByExpressionMetadata, boolean reverse, boolean nullHandlingEnabled) {
    return getComparator(orderByExpressions, orderByExpressionMetadata, reverse, nullHandlingEnabled, 0,
        orderByExpressions.size());
  }

  /**
   * @param reverse if false, the comparator will order in the direction indicated by the
   * {@link OrderByExpressionContext#isAsc()}. Otherwise, it will be in the opposite direction.
   */
  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      TransformResultMetadata[] orderByExpressionMetadata, boolean reverse, boolean nullHandlingEnabled, int from,
      int to) {
    Preconditions.checkArgument(to <= orderByExpressions.size(),
        "Trying to access %sth position of orderByExpressions with size %s", to, orderByExpressions.size());
    Preconditions.checkArgument(to <= orderByExpressionMetadata.length,
        "Trying to access %sth position of orderByExpressionMetadata with size %s", to,
        orderByExpressionMetadata.length);
    Preconditions.checkArgument(from < to, "FROM (%s) must be lower than TO (%s)", from, to);

    // Compare all single-value columns
    int numOrderByExpressions = to - from;
    List<Integer> valueIndexList = new ArrayList<>(numOrderByExpressions);
    for (int i = from; i < to; i++) {
      if (orderByExpressionMetadata[i].isSingleValue()) {
        valueIndexList.add(i);
      } else {
        // MV columns should not be part of the selection order by only list
        throw new BadQueryRequestException(
            String.format("MV expression: %s should not be included in the ORDER-BY clause",
                orderByExpressions.get(i)));
      }
    }

    int numValuesToCompare = valueIndexList.size();
    int[] valueIndices = new int[numValuesToCompare];
    FieldSpec.DataType[] storedTypes = new FieldSpec.DataType[numValuesToCompare];
    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[numValuesToCompare];
    int ascMult = reverse ? -1 : 1;
    int descMult = reverse ? 1 : -1;
    for (int i = 0; i < numValuesToCompare; i++) {
      int valueIndex = valueIndexList.get(i);
      valueIndices[i] = valueIndex;
      storedTypes[i] = orderByExpressionMetadata[valueIndex].getDataType().getStoredType();
      multipliers[i] = orderByExpressions.get(valueIndex).isAsc() ? ascMult : descMult;
    }

    if (nullHandlingEnabled) {
      return (Object[] o1, Object[] o2) -> {
        for (int i = 0; i < numValuesToCompare; i++) {
          int index = valueIndices[i];
          // TODO: Evaluate the performance of casting to Comparable and avoid the switch
          Object v1 = o1[index];
          Object v2 = o2[index];
          if (v1 == null) {
            // The default null ordering is: 'NULLS LAST', regardless of the ordering direction.
            return v2 == null ? 0 : -multipliers[i];
          } else if (v2 == null) {
            return multipliers[i];
          }
          int result = compareCols(v1, v2, storedTypes[i], multipliers[i]);
          if (result != 0) {
            return result;
          }
        }
        return 0;
      };
    } else {
      return (Object[] o1, Object[] o2) -> {
        for (int i = 0; i < numValuesToCompare; i++) {
          int index = valueIndices[i];
          // TODO: Evaluate the performance of casting to Comparable and avoid the switch
          int result = compareCols(o1[index], o2[index], storedTypes[i], multipliers[i]);
          if (result != 0) {
            return result;
          }
        }
        return 0;
      };
    }
  }

  private static int compareCols(Object v1, Object v2, FieldSpec.DataType type, int multiplier) {

    // TODO: Evaluate the performance of casting to Comparable and avoid the switch
    int result;
    switch (type) {
      case INT:
        result = ((Integer) v1).compareTo((Integer) v2);
        break;
      case LONG:
        result = ((Long) v1).compareTo((Long) v2);
        break;
      case FLOAT:
        result = ((Float) v1).compareTo((Float) v2);
        break;
      case DOUBLE:
        result = ((Double) v1).compareTo((Double) v2);
        break;
      case BIG_DECIMAL:
        result = ((BigDecimal) v1).compareTo((BigDecimal) v2);
        break;
      case STRING:
        result = ((String) v1).compareTo((String) v2);
        break;
      case BYTES:
        result = ((ByteArray) v1).compareTo((ByteArray) v2);
        break;
      // NOTE: Multi-value columns are not comparable, so we should not reach here
      default:
        throw new IllegalStateException();
    }
    return result * multiplier;
  }
}
