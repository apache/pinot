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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


public class MapBasedGlobalDictionaries implements GlobalDictionaries {
  private final static int INT_BASE_VALUE = 1000;
  private final static long LONG_BASE_VALUE = 100000;
  private static final float FLOAT_BASE_VALUE = 100.23f;
  private static final double DOUBLE_BASE_VALUE = 1000.2375;

  private final Map<String, OrigAndDerivedValueHolder> _columnToGlobalDictionary;

  MapBasedGlobalDictionaries() {
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
    FieldSpec.DataType dataType = columnMetadata.getDataType();
    _columnToGlobalDictionary.putIfAbsent(column, new OrigAndDerivedValueHolder(dataType));
    OrigAndDerivedValueHolder origAndDerivedValueHolder = _columnToGlobalDictionary.get(column);
    if (dataType == FieldSpec.DataType.BYTES) {
      origAndDerivedValueHolder.setOrigValue(new ByteArray((byte[]) origValue));
    } else {
      origAndDerivedValueHolder.setOrigValue(origValue);
    }
  }

  /**
   * This is the second step where we complete the global dictionaries
   * by sorting the original values to get sort order across all segments
   */
  @Override
  public void sortOriginalValuesInGlobalDictionaries() {
    // NO-OP since we use a sorted map so the global dictionary
    // is already sorted
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
      OrigAndDerivedValueHolder origAndDerivedValueHolder = entry.getValue();
      generateDerivedValuesForGlobalDictionary(origAndDerivedValueHolder);
    }
  }

  @Override
  public void serialize(String outputDir)
      throws Exception {
    // write global dictionary for each column
    for (String column : _columnToGlobalDictionary.keySet()) {
      PrintWriter dictionaryWriter =
          new PrintWriter(new BufferedWriter(new FileWriter(outputDir + "/" + column + DICT_FILE_EXTENSION)));
      OrigAndDerivedValueHolder origAndDerivedValueHolder = _columnToGlobalDictionary.get(column);
      Set<Map.Entry<Object, DerivedValue>> entries = origAndDerivedValueHolder._origAndDerivedValues.entrySet();
      Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = entries.iterator();
      while (sortedIterator.hasNext()) {
        Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
        dictionaryWriter.println(entry.getKey());
        dictionaryWriter.println(entry.getValue()._derivedValue);
      }
      dictionaryWriter.flush();
    }
  }

  @Override
  public Object getDerivedValueForOrigValueSV(String column, Object origValue) {
    OrigAndDerivedValueHolder origAndDerivedValueHolder = _columnToGlobalDictionary.get(column);
    TreeMap<Object, DerivedValue> sortedMap = origAndDerivedValueHolder._origAndDerivedValues;
    return sortedMap.get(origValue)._derivedValue;
  }

  @Override
  public Object[] getDerivedValuesForOrigValuesMV(String column, Object[] origMultiValues) {
    OrigAndDerivedValueHolder origAndDerivedValueHolder = _columnToGlobalDictionary.get(column);
    TreeMap<Object, DerivedValue> sortedMap = origAndDerivedValueHolder._origAndDerivedValues;
    int length = origMultiValues.length;
    Object[] derivedMultiValues = new Object[length];
    for (int i = 0; i < length; i++) {
      derivedMultiValues[i] = sortedMap.get(origMultiValues[i])._derivedValue;
    }
    return derivedMultiValues;
  }

  private static class OrigAndDerivedValueHolder {
    FieldSpec.DataType _dataType;
    TreeMap<Object, DerivedValue> _origAndDerivedValues;

    OrigAndDerivedValueHolder(FieldSpec.DataType dataType) {
      _dataType = dataType;
      _origAndDerivedValues = new TreeMap<>();
    }

    void setOrigValue(Object origValue) {
      if (!_origAndDerivedValues.containsKey(origValue)) {
        _origAndDerivedValues.put(origValue, new DerivedValue());
      }
    }
  }

  /**
   * This wrapper is needed so that we can set the derived values in global dictionary
   * in sorted order while iterating over it. When we iterate over TreeMap, we get the
   * map's entries as a set. Since doing a put() while iteration is not going to work,
   * we create a wrapper and simply set the derived value to whatever the generated
   * value is as we iterate on the sorted map to go over all original dictionary values.
   *
   * TODO: May be if we can do entry.setValue() on Map.Entry, then we don't need this wrapper
   * and we can safely update the TreeMap during iteration.
   */
  private static class DerivedValue {
    Object _derivedValue;

    DerivedValue() {
    }
  }

  private void generateDerivedValuesForGlobalDictionary(OrigAndDerivedValueHolder origAndDerivedValueHolder) {
    switch (origAndDerivedValueHolder._dataType) {
      case INT:
        generateDerivedIntValuesForGD(origAndDerivedValueHolder._origAndDerivedValues);
        break;
      case LONG:
        generateDerivedLongValuesForGD(origAndDerivedValueHolder._origAndDerivedValues);
        break;
      case FLOAT:
        generateDerivedFloatValuesForGD(origAndDerivedValueHolder._origAndDerivedValues);
        break;
      case DOUBLE:
        generateDerivedDoubleValuesForGD(origAndDerivedValueHolder._origAndDerivedValues);
        break;
      case STRING:
        generateDerivedStringValuesForGD(origAndDerivedValueHolder._origAndDerivedValues);
        break;
      case BYTES:
        generateDerivedByteValuesForGD(origAndDerivedValueHolder._origAndDerivedValues);
        break;
      default:
        throw new UnsupportedOperationException(
            "global dictionary currently does not support: " + origAndDerivedValueHolder._dataType.name());
    }
  }

  private void generateDerivedIntValuesForGD(TreeMap<Object, DerivedValue> sortedMap) {
    Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = sortedMap.entrySet().iterator();
    int i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      DerivedValue derivedValue = entry.getValue();
      derivedValue._derivedValue = INT_BASE_VALUE + i++;
    }
  }

  private void generateDerivedLongValuesForGD(TreeMap<Object, DerivedValue> sortedMap) {
    Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = sortedMap.entrySet().iterator();
    int i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      DerivedValue derivedValue = entry.getValue();
      derivedValue._derivedValue = LONG_BASE_VALUE + i++;
    }
  }

  private void generateDerivedFloatValuesForGD(TreeMap<Object, DerivedValue> sortedMap) {
    Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = sortedMap.entrySet().iterator();
    int i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      DerivedValue derivedValue = entry.getValue();
      derivedValue._derivedValue = FLOAT_BASE_VALUE + i++;
    }
  }

  private void generateDerivedDoubleValuesForGD(TreeMap<Object, DerivedValue> sortedMap) {
    Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = sortedMap.entrySet().iterator();
    int i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      DerivedValue derivedValue = entry.getValue();
      derivedValue._derivedValue = DOUBLE_BASE_VALUE + i++;
    }
  }

  private String[] generateDerivedStringValuesForGD(TreeMap<Object, DerivedValue> sortedMap) {
    Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = sortedMap.entrySet().iterator();
    int cardinality = sortedMap.size();
    String[] values = new String[cardinality];
    int i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      String val = (String) entry.getKey();
      if (val == null || val.equals("") || val.equals(" ") || val.equals("null")) {
        values[i++] = "null";
      } else {
        int origValLength = val.length();
        values[i++] = RandomStringUtils.randomAlphanumeric(origValLength);
      }
    }
    Arrays.sort(values);
    sortedIterator = sortedMap.entrySet().iterator();
    i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      DerivedValue derivedValue = entry.getValue();
      derivedValue._derivedValue = values[i++];
    }
    return values;
  }

  private ByteArray[] generateDerivedByteValuesForGD(TreeMap<Object, DerivedValue> sortedMap) {
    Iterator<Map.Entry<Object, DerivedValue>> sortedIterator = sortedMap.entrySet().iterator();
    int cardinality = sortedMap.size();
    ByteArray[] values = new ByteArray[cardinality];
    Random random = new Random();
    int i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      ByteArray byteArray = (ByteArray) entry.getKey();
      if (byteArray == null || byteArray.length() == 0) {
        values[i++] = new ByteArray(new byte[0]);
      } else {
        int origValLength = byteArray.length();
        byte[] generated = new byte[origValLength];
        random.nextBytes(generated);
        values[i++] = new ByteArray(generated);
      }
    }
    Arrays.sort(values);
    sortedIterator = sortedMap.entrySet().iterator();
    i = 0;
    while (sortedIterator.hasNext()) {
      Map.Entry<Object, DerivedValue> entry = sortedIterator.next();
      DerivedValue derivedValue = entry.getValue();
      derivedValue._derivedValue = values[i++];
    }
    return values;
  }
}
