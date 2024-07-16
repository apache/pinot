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
package org.apache.pinot.tools.anonymizer;

import com.google.common.base.Preconditions;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


public class ArrayBasedGlobalDictionaries implements GlobalDictionaries {
  private final static int INT_BASE_VALUE = 1000;
  private final static long LONG_BASE_VALUE = 100000;
  private static final float FLOAT_BASE_VALUE = 100.23f;
  private static final double DOUBLE_BASE_VALUE = 1000.2375;

  private final Map<String, OrigAndDerivedValueHolder> _columnToGlobalDictionary;

  ArrayBasedGlobalDictionaries() {
    _columnToGlobalDictionary = new HashMap<>();
  }

  /**
   * First step towards building global dictionary
   * by inserting the original values from segments
   * into global dictionary
   * @param origValue original value
   * @param column column name
   * @param columnMetadata column metadata
   * @param cardinality total cardinality of column
   */
  @Override
  public void addOrigValueToGlobalDictionary(Object origValue, String column, ColumnMetadata columnMetadata,
      int cardinality) {
    _columnToGlobalDictionary
        .putIfAbsent(column, new OrigAndDerivedValueHolder(columnMetadata.getDataType(), cardinality));
    OrigAndDerivedValueHolder holder = _columnToGlobalDictionary.get(column);
    if (columnMetadata.getDataType() == FieldSpec.DataType.BYTES) {
      holder.addOrigValue(new ByteArray((byte[]) origValue));
    } else {
      holder.addOrigValue(origValue);
    }
  }

  /**
   * This is the second step where we complete the global dictionaries
   * by sorting the original values to get sort order across all segments
   */
  @Override
  public void sortOriginalValuesInGlobalDictionaries() {
    for (Map.Entry<String, OrigAndDerivedValueHolder> entry : _columnToGlobalDictionary.entrySet()) {
      OrigAndDerivedValueHolder valueHolder = entry.getValue();
      valueHolder.sort();
    }
  }

  /**
   * This is the third and final step where we complete the global
   * dictionaries by generating values:
   *
   * For numeric columns, we generate in order since
   * we start with a base value.
   * For string column, we first generate and then sort
   */
  @Override
  public void addDerivedValuesToGlobalDictionaries() {
    // update global dictionary for each column by adding
    // the corresponding generated value for each orig value
    for (Map.Entry<String, OrigAndDerivedValueHolder> entry : _columnToGlobalDictionary.entrySet()) {
      OrigAndDerivedValueHolder valueHolder = entry.getValue();
      generateDerivedValuesForGlobalDictionary(valueHolder);
    }
  }

  @Override
  public void serialize(String outputDir)
      throws Exception {
    // write global dictionary for each column
    for (String column : _columnToGlobalDictionary.keySet()) {
      PrintWriter dictionaryWriter =
          new PrintWriter(new BufferedWriter(new FileWriter(outputDir + "/" + column + DICT_FILE_EXTENSION)));
      OrigAndDerivedValueHolder holder = _columnToGlobalDictionary.get(column);
      for (int i = 0; i < holder._index; i++) {
        dictionaryWriter.println(holder._origValues[i]);
        dictionaryWriter.println(holder._derivedValues[i]);
      }
      dictionaryWriter.flush();
    }
  }

  @Override
  public Object getDerivedValueForOrigValueSV(String column, Object origValue) {
    OrigAndDerivedValueHolder valueHolder = _columnToGlobalDictionary.get(column);
    return valueHolder.getDerivedValueForOrigValue(origValue);
  }

  @Override
  public Object[] getDerivedValuesForOrigValuesMV(String column, Object[] origMultiValues) {
    OrigAndDerivedValueHolder valueHolder = _columnToGlobalDictionary.get(column);
    int length = origMultiValues.length;
    Object[] derivedMultiValues = new Object[length];
    for (int i = 0; i < length; i++) {
      derivedMultiValues[i] = valueHolder.getDerivedValueForOrigValue(origMultiValues[i]);
    }
    return derivedMultiValues;
  }

  /**
   * Per column holder to store both original values and corresponding
   * derived values in global dictionary.
   */
  private static class OrigAndDerivedValueHolder {
    FieldSpec.DataType _dataType;
    Object[] _origValues;
    Object[] _derivedValues;
    int _index;
    Comparator _comparator;

    OrigAndDerivedValueHolder(FieldSpec.DataType dataType, int totalCardinality) {
      _dataType = dataType;
      // user specified cardinality might be slightly inaccurate depending
      // on when the user determined the cardinality and when the source
      // segments were given to this tool so just provision 10% additional
      // capacity
      _origValues = new Object[totalCardinality + (int) (0.1 * totalCardinality)];
      // we will use index value as the actual total cardinality based
      // on total number of values read from dictionary of each segment
      _index = 0;
      buildComparator();
    }

    void buildComparator() {
      switch (_dataType) {
        case INT:
          _comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
              return ((Integer) o1).compareTo((Integer) o2);
            }
          };
          break;
        case LONG:
          _comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
              return ((Long) o1).compareTo((Long) o2);
            }
          };
          break;
        case FLOAT:
          _comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
              return ((Float) o1).compareTo((Float) o2);
            }
          };
          break;
        case DOUBLE:
          _comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
              return ((Double) o1).compareTo((Double) o2);
            }
          };
          break;
        case STRING:
          _comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
              return ((String) o1).compareTo((String) o2);
            }
          };
          break;
        case BYTES:
          _comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
              return ((ByteArray) o1).compareTo((ByteArray) o2);
            }
          };
          break;
        default:
          throw new UnsupportedOperationException("global dictionary currently does not support: " + _dataType.name());
      }
    }

    void sort() {
      Arrays.sort(_origValues, 0, _index, _comparator);
    }

    void addOrigValue(Object origValue) {
      if (!linearSearch(origValue)) {
        _origValues[_index++] = origValue;
      }
    }

    void setDerivedValues(Object[] derivedValues) {
      Preconditions.checkState(derivedValues.length == _index);
      _derivedValues = derivedValues;
    }

    Object getDerivedValueForOrigValue(Object origValue) {
      int index = binarySearch(0, _index - 1, origValue);
      Preconditions.checkState(index >= 0, "Expecting origValue: " + origValue);
      return _derivedValues[index];
    }

    // used during building global dictionary phase
    // to prevent adding original values already
    // seen from previous segments
    boolean linearSearch(Object key) {
      for (int i = 0; i < _index; i++) {
        if (key == null) {
          if (_origValues[i] == null) {
            return true;
          }
        } else {
          if (_origValues[i].equals(key)) {
            return true;
          }
        }
      }
      return false;
    }

    // used during data generation phase.
    // since the global dictionary is fully built
    // and sorted, we can do binary search
    // TODO: make this iterative
    int binarySearch(int low, int high, Object key) {
      if (low > high) {
        return -1;
      }

      int mid = (low + high) / 2;

      if (_origValues[mid].equals(key)) {
        return mid;
      }

      if (isLessThan(_origValues[mid], key) < 0) {
        return binarySearch(mid + 1, high, key);
      }

      return binarySearch(low, mid - 1, key);
    }

    private int isLessThan(Object o1, Object o2) {
      switch (_dataType) {
        case INT:
          return ((Integer) o1).compareTo((Integer) o2);
        case LONG:
          return ((Long) o1).compareTo((Long) o2);
        case DOUBLE:
          return ((Double) o1).compareTo((Double) o2);
        case FLOAT:
          return ((Float) o1).compareTo((Float) o2);
        case STRING:
          return ((String) o1).compareTo((String) o2);
        case BYTES:
          return ((ByteArray) o1).compareTo((ByteArray) o2);
        default:
          throw new IllegalStateException("unexpected data type: " + _dataType);
      }
    }
  }

  private void generateDerivedValuesForGlobalDictionary(OrigAndDerivedValueHolder valueHolder) {
    int cardinality = valueHolder._index;
    switch (valueHolder._dataType) {
      case INT:
        valueHolder.setDerivedValues(generateDerivedIntValuesForGD(cardinality));
        break;
      case LONG:
        valueHolder.setDerivedValues(generateDerivedLongValuesForGD(cardinality));
        break;
      case FLOAT:
        valueHolder.setDerivedValues(generateDerivedFloatValuesForGD(cardinality));
        break;
      case DOUBLE:
        valueHolder.setDerivedValues(generateDerivedDoubleValuesForGD(cardinality));
        break;
      case STRING:
        valueHolder.setDerivedValues(generateDerivedStringValuesForGD(valueHolder));
        break;
      case BYTES:
        valueHolder.setDerivedValues(generateDerivedByteValuesForGD(valueHolder));
        break;
      default:
        throw new UnsupportedOperationException(
            "global dictionary currently does not support: " + valueHolder._dataType.name());
    }
  }

  private Integer[] generateDerivedIntValuesForGD(int cardinality) {
    Integer[] values = new Integer[cardinality];
    for (int i = 0; i < cardinality; i++) {
      values[i] = INT_BASE_VALUE + i;
    }
    return values;
  }

  private Long[] generateDerivedLongValuesForGD(int cardinality) {
    Long[] values = new Long[cardinality];
    for (int i = 0; i < cardinality; i++) {
      values[i] = LONG_BASE_VALUE + (long) i;
    }
    return values;
  }

  private Float[] generateDerivedFloatValuesForGD(int cardinality) {
    Float[] values = new Float[cardinality];
    for (int i = 0; i < cardinality; i++) {
      values[i] = FLOAT_BASE_VALUE + (float) i;
    }
    return values;
  }

  private Double[] generateDerivedDoubleValuesForGD(int cardinality) {
    Double[] values = new Double[cardinality];
    for (int i = 0; i < cardinality; i++) {
      values[i] = DOUBLE_BASE_VALUE + (double) i;
    }
    return values;
  }

  private String[] generateDerivedStringValuesForGD(OrigAndDerivedValueHolder valueHolder) {
    int cardinality = valueHolder._index;
    String[] values = new String[cardinality];
    for (int i = 0; i < cardinality; i++) {
      String val = (String) valueHolder._origValues[i];
      if (val == null || val.equals("") || val.equals(" ") || val.equals("null")) {
        values[i] = "null";
      } else {
        int origValLength = val.length();
        values[i] = RandomStringUtils.randomAlphanumeric(origValLength);
      }
    }
    Arrays.sort(values);
    return values;
  }

  private ByteArray[] generateDerivedByteValuesForGD(OrigAndDerivedValueHolder valueHolder) {
    int cardinality = valueHolder._index;
    ByteArray[] values = new ByteArray[cardinality];
    Random random = new Random();
    for (int i = 0; i < cardinality; i++) {
      ByteArray byteArray = (ByteArray) valueHolder._origValues[i];
      if (byteArray == null || byteArray.length() == 0) {
        values[i] = new ByteArray(new byte[0]);
      } else {
        int origValLength = byteArray.length();
        byte[] generated = new byte[origValLength];
        random.nextBytes(generated);
        values[i] = new ByteArray(generated);
      }
    }
    Arrays.sort(values);
    return values;
  }
}
