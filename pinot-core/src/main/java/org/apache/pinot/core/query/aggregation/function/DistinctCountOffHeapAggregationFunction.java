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
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.distinct.BaseOffHeapSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap128BitSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap32BitSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap64BitSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Aggregation function to compute the count of distinct values for a column using off-heap memory.
public class DistinctCountOffHeapAggregationFunction
    extends NullableSingleInputAggregationFunction<BaseOffHeapSet, Integer> {
  // Use empty OffHeap32BitSet as a placeholder for empty result
  // NOTE: It is okay to close it (multiple times) since we are never adding values into it
  private static final OffHeap32BitSet EMPTY_PLACEHOLDER = new OffHeap32BitSet(0);

  private final int _initialCapacity;
  private final int _hashBits;

  public DistinctCountOffHeapAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
    if (arguments.size() > 1) {
      Parameters parameters = new Parameters(arguments.get(1).getLiteral().getStringValue());
      _initialCapacity = parameters._initialCapacity;
      _hashBits = parameters._hashBits;
    } else {
      _initialCapacity = Parameters.DEFAULT_INITIAL_CAPACITY;
      _hashBits = Parameters.DEFAULT_HASH_BITS;
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTOFFHEAP;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException(
        "DISTINCT_COUNT_OFF_HEAP cannot be applied to group-by queries. Use DISTINCT_COUNT instead.");
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      // For dictionary-encoded expression, store dictionary ids into the bitmap
      if (blockValSet.isSingleValue()) {
        int[] dictIds = blockValSet.getDictionaryIdsSV();
        BitSet dictIdBitSet = getDictIdBitSet(aggregationResultHolder, dictionary);
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            dictIdBitSet.set(dictIds[i]);
          }
        });
      } else {
        int[][] dictIds = blockValSet.getDictionaryIdsMV();
        BitSet dictIdBitSet = getDictIdBitSet(aggregationResultHolder, dictionary);
        for (int i = 0; i < length; i++) {
          for (int dictId : dictIds[i]) {
            dictIdBitSet.set(dictId);
          }
        }
      }
    } else {
      // For non-dictionary-encoded expression, add values into the value set
      BaseOffHeapSet valueSet = aggregationResultHolder.getResult();
      if (valueSet == null) {
        valueSet = createValueSet(blockValSet.getValueType().getStoredType());
        aggregationResultHolder.setValue(valueSet);
      }
      if (blockValSet.isSingleValue()) {
        addToValueSetSV(length, blockValSet, valueSet);
      } else {
        addToValueSetMV(length, blockValSet, valueSet);
      }
    }
  }

  private static BitSet getDictIdBitSet(AggregationResultHolder aggregationResultHolder, Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._bitSet;
  }

  private BaseOffHeapSet createValueSet(DataType storedType) {
    switch (storedType) {
      case INT:
      case FLOAT:
        return new OffHeap32BitSet(_initialCapacity);
      case LONG:
      case DOUBLE:
        return new OffHeap64BitSet(_initialCapacity);
      default:
        switch (_hashBits) {
          case 32:
            return new OffHeap32BitSet(_initialCapacity);
          case 64:
            return new OffHeap64BitSet(_initialCapacity);
          case 128:
            return new OffHeap128BitSet(_initialCapacity);
          default:
            throw new IllegalStateException();
        }
    }
  }

  private void addToValueSetSV(int length, BlockValSet blockValSet, BaseOffHeapSet valueSet) {
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        OffHeap32BitSet intSet = (OffHeap32BitSet) valueSet;
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            intSet.add(intValues[i]);
          }
        });
        break;
      case LONG:
        OffHeap64BitSet longSet = (OffHeap64BitSet) valueSet;
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            longSet.add(longValues[i]);
          }
        });
        break;
      case FLOAT:
        OffHeap32BitSet floatSet = (OffHeap32BitSet) valueSet;
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            floatSet.add(Float.floatToRawIntBits(floatValues[i]));
          }
        });
        break;
      case DOUBLE:
        OffHeap64BitSet doubleSet = (OffHeap64BitSet) valueSet;
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            doubleSet.add(Double.doubleToRawLongBits(doubleValues[i]));
          }
        });
        break;
      default:
        switch (_hashBits) {
          case 32:
            OffHeap32BitSet valueSet32 = (OffHeap32BitSet) valueSet;
            int[] hashValues32 = blockValSet.get32BitsMurmur3HashValuesSV();
            forEachNotNull(length, blockValSet, (from, to) -> {
              for (int i = from; i < to; i++) {
                valueSet32.add(hashValues32[i]);
              }
            });
            break;
          case 64:
            OffHeap64BitSet valueSet64 = (OffHeap64BitSet) valueSet;
            long[] hashValues64 = blockValSet.get64BitsMurmur3HashValuesSV();
            forEachNotNull(length, blockValSet, (from, to) -> {
              for (int i = from; i < to; i++) {
                valueSet64.add(hashValues64[i]);
              }
            });
            break;
          case 128:
            OffHeap128BitSet valueSet128 = (OffHeap128BitSet) valueSet;
            long[][] hashValues128 = blockValSet.get128BitsMurmur3HashValuesSV();
            forEachNotNull(length, blockValSet, (from, to) -> {
              for (int i = from; i < to; i++) {
                long[] hashValue = hashValues128[i];
                valueSet128.add(hashValue[0], hashValue[1]);
              }
            });
            break;
          default:
            throw new IllegalStateException();
        }
        break;
    }
  }

  private void addToValueSetMV(int length, BlockValSet blockValSet, BaseOffHeapSet valueSet) {
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        OffHeap32BitSet intSet = (OffHeap32BitSet) valueSet;
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int intValue : intValues[i]) {
            intSet.add(intValue);
          }
        }
        break;
      case LONG:
        OffHeap64BitSet longSet = (OffHeap64BitSet) valueSet;
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long longValue : longValues[i]) {
            longSet.add(longValue);
          }
        }
        break;
      case FLOAT:
        OffHeap32BitSet floatSet = (OffHeap32BitSet) valueSet;
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float floatValue : floatValues[i]) {
            floatSet.add(Float.floatToRawIntBits(floatValue));
          }
        }
        break;
      case DOUBLE:
        OffHeap64BitSet doubleSet = (OffHeap64BitSet) valueSet;
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double doubleValue : doubleValues[i]) {
            doubleSet.add(Double.doubleToRawLongBits(doubleValue));
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "DISTINCT_COUNT_OFF_HEAP does not support MV columns of type: " + blockValSet.getValueType()
                + ". Use DISTINCT_COUNT instead.");
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseOffHeapSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return EMPTY_PLACEHOLDER;
    }
    if (result instanceof DictIdsWrapper) {
      return extractAggregationResult((DictIdsWrapper) result);
    } else {
      return (BaseOffHeapSet) result;
    }
  }

  private BaseOffHeapSet extractAggregationResult(DictIdsWrapper dictIdsWrapper) {
    BitSet bitSet = dictIdsWrapper._bitSet;
    int length = bitSet.cardinality();
    Dictionary dictionary = dictIdsWrapper._dictionary;
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        OffHeap32BitSet intSet = new OffHeap32BitSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          intSet.add(dictionary.getIntValue(i));
        }
        return intSet;
      case LONG:
        OffHeap64BitSet longSet = new OffHeap64BitSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          longSet.add(dictionary.getLongValue(i));
        }
        return longSet;
      case FLOAT:
        OffHeap32BitSet floatSet = new OffHeap32BitSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          floatSet.add(Float.floatToRawIntBits(dictionary.getFloatValue(i)));
        }
        return floatSet;
      case DOUBLE:
        OffHeap64BitSet doubleSet = new OffHeap64BitSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          doubleSet.add(Double.doubleToRawLongBits(dictionary.getDoubleValue(i)));
        }
        return doubleSet;
      default:
        switch (_hashBits) {
          case 32:
            OffHeap32BitSet valueSet32 = new OffHeap32BitSet(length);
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
              valueSet32.add(dictionary.get32BitsMurmur3HashValue(i));
            }
            return valueSet32;
          case 64:
            OffHeap64BitSet valueSet64 = new OffHeap64BitSet(length);
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
              valueSet64.add(dictionary.get64BitsMurmur3HashValue(i));
            }
            return valueSet64;
          case 128:
            OffHeap128BitSet valueSet128 = new OffHeap128BitSet(length);
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
              long[] hashValue = dictionary.get128BitsMurmur3HashValue(i);
              valueSet128.add(hashValue[0], hashValue[1]);
            }
            return valueSet128;
          default:
            throw new IllegalStateException();
        }
    }
  }

  /// Extracts the value set from the dictionary.
  public BaseOffHeapSet extractAggregationResult(Dictionary dictionary) {
    int length = dictionary.length();
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        OffHeap32BitSet intSet = new OffHeap32BitSet(length);
        for (int i = 0; i < length; i++) {
          intSet.add(dictionary.getIntValue(i));
        }
        return intSet;
      case LONG:
        OffHeap64BitSet longSet = new OffHeap64BitSet(length);
        for (int i = 0; i < length; i++) {
          longSet.add(dictionary.getLongValue(i));
        }
        return longSet;
      case FLOAT:
        OffHeap32BitSet floatSet = new OffHeap32BitSet(length);
        for (int i = 0; i < length; i++) {
          floatSet.add(Float.floatToRawIntBits(dictionary.getFloatValue(i)));
        }
        return floatSet;
      case DOUBLE:
        OffHeap64BitSet doubleSet = new OffHeap64BitSet(length);
        for (int i = 0; i < length; i++) {
          doubleSet.add(Double.doubleToRawLongBits(dictionary.getDoubleValue(i)));
        }
        return doubleSet;
      default:
        switch (_hashBits) {
          case 32:
            OffHeap32BitSet valueSet32 = new OffHeap32BitSet(length);
            for (int i = 0; i < length; i++) {
              valueSet32.add(dictionary.get32BitsMurmur3HashValue(i));
            }
            return valueSet32;
          case 64:
            OffHeap64BitSet valueSet64 = new OffHeap64BitSet(length);
            for (int i = 0; i < length; i++) {
              valueSet64.add(dictionary.get64BitsMurmur3HashValue(i));
            }
            return valueSet64;
          case 128:
            OffHeap128BitSet valueSet128 = new OffHeap128BitSet(length);
            for (int i = 0; i < length; i++) {
              long[] hashValue = dictionary.get128BitsMurmur3HashValue(i);
              valueSet128.add(hashValue[0], hashValue[1]);
            }
            return valueSet128;
          default:
            throw new IllegalStateException();
        }
    }
  }

  @Override
  public BaseOffHeapSet extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseOffHeapSet merge(BaseOffHeapSet intermediateResult1, BaseOffHeapSet intermediateResult2) {
    assert intermediateResult1 != null && intermediateResult2 != null;
    if (intermediateResult1.isEmpty()) {
      intermediateResult1.close();
      return intermediateResult2;
    }
    intermediateResult1.merge(intermediateResult2);
    intermediateResult2.close();
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(BaseOffHeapSet set) {
    int type;
    if (set instanceof OffHeap32BitSet) {
      type = 0;
    } else if (set instanceof OffHeap64BitSet) {
      type = 1;
    } else if (set instanceof OffHeap128BitSet) {
      type = 2;
    } else {
      throw new IllegalStateException();
    }
    byte[] bytes = set.serialize();
    set.close();
    return new SerializedIntermediateResult(type, bytes);
  }

  @Override
  public BaseOffHeapSet deserializeIntermediateResult(CustomObject customObject) {
    switch (customObject.getType()) {
      case 0:
        return OffHeap32BitSet.deserialize(customObject.getBuffer());
      case 1:
        return OffHeap64BitSet.deserialize(customObject.getBuffer());
      case 2:
        return OffHeap128BitSet.deserialize(customObject.getBuffer());
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(BaseOffHeapSet set) {
    assert set != null;
    int size = set.size();
    set.close();
    return size;
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return finalResult1 + finalResult2;
  }

  /// Helper class to wrap the dictionary ids.
  /// Different from the BaseDistinctAggregateAggregationFunction.DictIdsWrapper, here we use a pre-allocated BitSet
  /// instead of RoaringBitmap for better performance on high cardinality distinct count.
  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final BitSet _bitSet;

    DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _bitSet = new BitSet(dictionary.length());
    }
  }

  /// Helper class to wrap the parameters.
  private static class Parameters {
    static final char PARAMETER_DELIMITER = ';';
    static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';

    static final String INITIAL_CAPACITY_KEY = "INITIALCAPACITY";
    static final int DEFAULT_INITIAL_CAPACITY = 10_000;

    static final String HASH_BITS_KEY = "HASHBITS";
    static final int DEFAULT_HASH_BITS = 64;

    int _initialCapacity = DEFAULT_INITIAL_CAPACITY;
    int _hashBits = DEFAULT_HASH_BITS;

    Parameters(String parametersString) {
      StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        switch (key.toUpperCase()) {
          case INITIAL_CAPACITY_KEY:
            _initialCapacity = Integer.parseInt(value);
            Preconditions.checkArgument(_initialCapacity > 0, "Initial capacity must be > 0, got: %s",
                _initialCapacity);
            break;
          case HASH_BITS_KEY:
            _hashBits = Integer.parseInt(value);
            Preconditions.checkArgument(_hashBits == 32 || _hashBits == 64 || _hashBits == 128,
                "Hash bits must be 32, 64 or 128, got: %s", _hashBits);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }
  }
}
