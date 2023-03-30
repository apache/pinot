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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The {@code IdSetAggregationFunction} collects the values for the given expression into an IdSet, which can be used in
 * the second query to optimize the query with huge IN clause generated from another query.
 * <p>The generated IdSet can be backed by RoaringBitmap, Roaring64NavigableMap or BloomFilter based on type of the ids
 * and the function parameters.
 * <p>The function takes an optional second argument as the parameters for the function. There are 3 parameters for the
 * function:
 * <ul>
 *   <li>
 *     sizeThresholdInBytes: When the size of the IdSet exceeds this threshold, convert the IdSet to BloomFilterIdSet to
 *     reduce the size of the IdSet. Directly create BloomFilterIdSet if it is smaller or equal to 0. (Default 8MB)
 *   </li>
 *   <li>
 *     expectedInsertions: Number of expected insertions for the BloomFilter, must be positive. (Default 5M)
 *   </li>
 *   <li>
 *     fpp: Desired false positive probability for the BloomFilter, must be positive and less than 1.0. (Default 0.03)
 *   </li>
 * </ul>
 * <p>Example: IDSET(col, 'sizeThresholdInBytes=1000;expectedInsertions=10000;fpp=0.03')
 */
public class IdSetAggregationFunction extends BaseSingleInputAggregationFunction<IdSet, String> {
  private static final char PARAMETER_DELIMITER = ';';
  private static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';
  private static final String UPPER_CASE_SIZE_THRESHOLD_IN_BYTES = "SIZETHRESHOLDINBYTES";
  private static final String UPPER_CASE_EXPECTED_INSERTIONS = "EXPECTEDINSERTIONS";
  private static final String UPPER_CASE_FPP = "FPP";

  private final int _sizeThresholdInBytes;
  private final int _expectedInsertions;
  private final double _fpp;

  public IdSetAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    if (arguments.size() == 1) {
      _sizeThresholdInBytes = IdSets.DEFAULT_SIZE_THRESHOLD_IN_BYTES;
      _expectedInsertions = IdSets.DEFAULT_EXPECTED_INSERTIONS;
      _fpp = IdSets.DEFAULT_FPP;
    } else {
      ExpressionContext parametersExpression = arguments.get(1);
      Preconditions.checkArgument(parametersExpression.getType() == ExpressionContext.Type.LITERAL,
          "Second argument of IdSet must be literal (parameters)");

      int sizeThresholdInBytes = IdSets.DEFAULT_SIZE_THRESHOLD_IN_BYTES;
      int expectedInsertions = IdSets.DEFAULT_EXPECTED_INSERTIONS;
      double fpp = IdSets.DEFAULT_FPP;
      String parametersString = parametersExpression.getLiteral().getStringValue();
      StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        switch (key.toUpperCase()) {
          case UPPER_CASE_SIZE_THRESHOLD_IN_BYTES:
            sizeThresholdInBytes = Integer.parseInt(value);
            break;
          case UPPER_CASE_EXPECTED_INSERTIONS:
            expectedInsertions = Integer.parseInt(value);
            break;
          case UPPER_CASE_FPP:
            fpp = Double.parseDouble(value);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
      _sizeThresholdInBytes = sizeThresholdInBytes;
      _expectedInsertions = expectedInsertions;
      _fpp = fpp;
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.IDSET;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    IdSet idSet = getIdSet(aggregationResultHolder, storedType);
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT:
          int[] intValuesSV = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            idSet.add(intValuesSV[i]);
          }
          break;
        case LONG:
          long[] longValuesSV = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            idSet.add(longValuesSV[i]);
          }
          break;
        case FLOAT:
          float[] floatValuesSV = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            idSet.add(floatValuesSV[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValuesSV = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            idSet.add(doubleValuesSV[i]);
          }
          break;
        case STRING:
          String[] stringValuesSV = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            idSet.add(stringValuesSV[i]);
          }
          break;
        case BYTES:
          byte[][] bytesValuesSV = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            idSet.add(bytesValuesSV[i]);
          }
          break;
        default:
          throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
      }
    } else {
      switch (storedType) {
        case INT:
          int[][] intValuesMV = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            for (int intValue : intValuesMV[i]) {
              idSet.add(intValue);
            }
          }
          break;
        case LONG:
          long[][] longValuesMV = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            for (long longValue : longValuesMV[i]) {
              idSet.add(longValue);
            }
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            for (float floatValue : floatValuesMV[i]) {
              idSet.add(floatValue);
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            for (double doubleValue : doubleValuesMV[i]) {
              idSet.add(doubleValue);
            }
          }
          break;
        case STRING:
          String[][] stringValuesMV = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            for (String stringValue : stringValuesMV[i]) {
              idSet.add(stringValue);
            }
          }
          break;
        default:
          throw new IllegalStateException("Illegal MV data type for ID_SET aggregation function: " + storedType);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT:
          int[] intValuesSV = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            getIdSet(groupByResultHolder, groupKeyArray[i], DataType.INT).add(intValuesSV[i]);
          }
          break;
        case LONG:
          long[] longValuesSV = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            getIdSet(groupByResultHolder, groupKeyArray[i], DataType.LONG).add(longValuesSV[i]);
          }
          break;
        case FLOAT:
          float[] floatValuesSV = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            getIdSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT).add(floatValuesSV[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValuesSV = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            getIdSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE).add(doubleValuesSV[i]);
          }
          break;
        case STRING:
          String[] stringValuesSV = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            getIdSet(groupByResultHolder, groupKeyArray[i], DataType.STRING).add(stringValuesSV[i]);
          }
          break;
        case BYTES:
          byte[][] bytesValuesSV = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            getIdSet(groupByResultHolder, groupKeyArray[i], DataType.BYTES).add(bytesValuesSV[i]);
          }
          break;
        default:
          throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
      }
    } else {
      switch (storedType) {
        case INT:
          int[][] intValuesMV = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            IdSet idSet = getIdSet(groupByResultHolder, groupKeyArray[i], DataType.INT);
            for (int intValue : intValuesMV[i]) {
              idSet.add(intValue);
            }
          }
          break;
        case LONG:
          long[][] longValuesMV = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            IdSet idSet = getIdSet(groupByResultHolder, groupKeyArray[i], DataType.LONG);
            for (long longValue : longValuesMV[i]) {
              idSet.add(longValue);
            }
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            IdSet idSet = getIdSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT);
            for (float floatValue : floatValuesMV[i]) {
              idSet.add(floatValue);
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            IdSet idSet = getIdSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE);
            for (double doubleValue : doubleValuesMV[i]) {
              idSet.add(doubleValue);
            }
          }
          break;
        case STRING:
          String[][] stringValuesMV = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            IdSet idSet = getIdSet(groupByResultHolder, groupKeyArray[i], DataType.STRING);
            for (String stringValue : stringValuesMV[i]) {
              idSet.add(stringValue);
            }
          }
          break;
        default:
          throw new IllegalStateException("Illegal MV data type for ID_SET aggregation function: " + storedType);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT:
          int[] intValuesSV = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            int intValue = intValuesSV[i];
            for (int groupKey : groupKeysArray[i]) {
              getIdSet(groupByResultHolder, groupKey, DataType.INT).add(intValue);
            }
          }
          break;
        case LONG:
          long[] longValuesSV = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            long longValue = longValuesSV[i];
            for (int groupKey : groupKeysArray[i]) {
              getIdSet(groupByResultHolder, groupKey, DataType.LONG).add(longValue);
            }
          }
          break;
        case FLOAT:
          float[] floatValuesSV = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            float floatValue = floatValuesSV[i];
            for (int groupKey : groupKeysArray[i]) {
              getIdSet(groupByResultHolder, groupKey, DataType.FLOAT).add(floatValue);
            }
          }
          break;
        case DOUBLE:
          double[] doubleValuesSV = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            double doubleValue = doubleValuesSV[i];
            for (int groupKey : groupKeysArray[i]) {
              getIdSet(groupByResultHolder, groupKey, DataType.DOUBLE).add(doubleValue);
            }
          }
          break;
        case STRING:
          String[] stringValuesSV = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            String stringValue = stringValuesSV[i];
            for (int groupKey : groupKeysArray[i]) {
              getIdSet(groupByResultHolder, groupKey, DataType.STRING).add(stringValue);
            }
          }
          break;
        case BYTES:
          byte[][] bytesValuesSV = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            byte[] bytesValue = bytesValuesSV[i];
            for (int groupKey : groupKeysArray[i]) {
              getIdSet(groupByResultHolder, groupKey, DataType.BYTES).add(bytesValue);
            }
          }
          break;
        default:
          throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
      }
    } else {
      switch (storedType) {
        case INT:
          int[][] intValuesMV = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            int[] intValues = intValuesMV[i];
            for (int groupKey : groupKeysArray[i]) {
              IdSet idSet = getIdSet(groupByResultHolder, groupKey, DataType.INT);
              for (int intValue : intValues) {
                idSet.add(intValue);
              }
            }
          }
          break;
        case LONG:
          long[][] longValuesMV = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            long[] longValues = longValuesMV[i];
            for (int groupKey : groupKeysArray[i]) {
              IdSet idSet = getIdSet(groupByResultHolder, groupKey, DataType.LONG);
              for (long longValue : longValues) {
                idSet.add(longValue);
              }
            }
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            float[] floatValues = floatValuesMV[i];
            for (int groupKey : groupKeysArray[i]) {
              IdSet idSet = getIdSet(groupByResultHolder, groupKey, DataType.FLOAT);
              for (float floatValue : floatValues) {
                idSet.add(floatValue);
              }
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            double[] doubleValues = doubleValuesMV[i];
            for (int groupKey : groupKeysArray[i]) {
              IdSet idSet = getIdSet(groupByResultHolder, groupKey, DataType.DOUBLE);
              for (double doubleValue : doubleValues) {
                idSet.add(doubleValue);
              }
            }
          }
          break;
        case STRING:
          String[][] stringValuesMV = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            String[] stringValues = stringValuesMV[i];
            for (int groupKey : groupKeysArray[i]) {
              IdSet idSet = getIdSet(groupByResultHolder, groupKey, DataType.STRING);
              for (String stringValue : stringValues) {
                idSet.add(stringValue);
              }
            }
          }
          break;
        default:
          throw new IllegalStateException("Illegal MV data type for ID_SET aggregation function: " + storedType);
      }
    }
  }

  @Override
  public IdSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    IdSet idSet = aggregationResultHolder.getResult();
    return idSet != null ? idSet : IdSets.emptyIdSet();
  }

  @Override
  public IdSet extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    IdSet idSet = groupByResultHolder.getResult(groupKey);
    return idSet != null ? idSet : IdSets.emptyIdSet();
  }

  @Override
  public IdSet merge(IdSet intermediateResult1, IdSet intermediateResult2) {
    return IdSets.merge(intermediateResult1, intermediateResult2, _sizeThresholdInBytes, _expectedInsertions, _fpp);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(IdSet intermediateResult) {
    try {
      return intermediateResult.toBase64String();
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while serializing IdSet", e);
    }
  }

  /**
   * Returns the IdSet from the result holder or creates a new one if it does not exist.
   */
  private IdSet getIdSet(AggregationResultHolder aggregationResultHolder, DataType valueType) {
    IdSet idSet = aggregationResultHolder.getResult();
    if (idSet == null) {
      idSet = IdSets.create(valueType, _sizeThresholdInBytes, _expectedInsertions, _fpp);
      aggregationResultHolder.setValue(idSet);
    }
    return idSet;
  }

  /**
   * Returns the IdSet for the given group key or creates a new one if it does not exist.
   */
  private IdSet getIdSet(GroupByResultHolder groupByResultHolder, int groupKey, DataType valueType) {
    IdSet idSet = groupByResultHolder.getResult(groupKey);
    if (idSet == null) {
      idSet = IdSets.create(valueType, _sizeThresholdInBytes, _expectedInsertions, _fpp);
      groupByResultHolder.setValueForKey(groupKey, idSet);
    }
    return idSet;
  }
}
