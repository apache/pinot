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
package org.apache.pinot.core.operator.filter.predicate;

import com.google.common.base.Equivalence;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.BaseInPredicate;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimestampUtils;


public class PredicateUtils {
  private PredicateUtils() {
  }

  // Bound the initial dictionary id set size to prevent over-allocating when a lot of values do not exist in the
  // dictionary
  private static final int MAX_INITIAL_DICT_ID_SET_SIZE = 1000;

  /**
   * Converts the given predicate value to the stored value based on the data type.
   */
  public static String getStoredValue(String value, DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return getStoredBooleanValue(value);
      case TIMESTAMP:
        return getStoredTimestampValue(value);
      default:
        return value;
    }
  }

  /**
   * Converts the given boolean predicate value to the inner representation (int).
   */
  public static String getStoredBooleanValue(String booleanValue) {
    return Integer.toString(BooleanUtils.toInt(booleanValue));
  }

  /**
   * Converts the given timestamp predicate value to the inner representation (millis since epoch).
   */
  public static String getStoredTimestampValue(String timestampValue) {
    return Long.toString(TimestampUtils.toMillisSinceEpoch(timestampValue));
  }

  /**
   * Returns a dictionary id set of the values in the given IN/NOT_IN predicate.
   */
  public static IntSet getDictIdSet(BaseInPredicate inPredicate, Dictionary dictionary, DataType dataType,
      @Nullable QueryContext queryContext) {
    List<String> values = inPredicate.getValues();
    int hashSetSize = Integer.min(HashUtil.getMinHashSetSize(values.size()), MAX_INITIAL_DICT_ID_SET_SIZE);
    IntSet dictIdSet = new IntOpenHashSet(hashSetSize);
    switch (dataType) {
      case INT:
        int[] intValues = inPredicate.getIntValues();
        for (int value : intValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case LONG:
        long[] longValues = inPredicate.getLongValues();
        for (long value : longValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = inPredicate.getFloatValues();
        for (float value : floatValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = inPredicate.getDoubleValues();
        for (double value : doubleValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = inPredicate.getBigDecimalValues();
        for (BigDecimal value : bigDecimalValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case BOOLEAN:
        int[] booleanValues = inPredicate.getBooleanValues();
        for (int value : booleanValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case TIMESTAMP:
        long[] timestampValues = inPredicate.getTimestampValues();
        for (long value : timestampValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      case STRING:
        if (queryContext == null || values.size() <= Integer.parseInt(queryContext.getQueryOptions()
            .getOrDefault(CommonConstants.Broker.Request.QueryOptionKey.IN_PREDICATE_SORT_THRESHOLD,
                CommonConstants.Broker.Request.QueryOptionValue.DEFAULT_IN_PREDICATE_SORT_THRESHOLD))) {
          for (String value : values) {
            int dictId = dictionary.indexOf(value);
            if (dictId >= 0) {
              dictIdSet.add(dictId);
            }
          }
        } else {
          List<String> sortedValues =
              queryContext.getOrComputeSharedValue(List.class, Equivalence.identity().wrap(inPredicate), k -> {
                List<String> copyValues = new ArrayList<>(values);
                copyValues.sort(null);
                return copyValues;
              });
          dictionary.getDictIds(sortedValues, dictIdSet);
        }
        break;
      case BYTES:
        ByteArray[] bytesValues = inPredicate.getBytesValues();
        for (ByteArray value : bytesValues) {
          int dictId = dictionary.indexOf(value);
          if (dictId >= 0) {
            dictIdSet.add(dictId);
          }
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
    return dictIdSet;
  }
}
