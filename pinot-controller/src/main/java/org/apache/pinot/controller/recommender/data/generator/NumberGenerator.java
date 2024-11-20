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
package org.apache.pinot.controller.recommender.data.generator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Random;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Sep 12, 2014
 */

public class NumberGenerator implements Generator {
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;

  private final int _cardinality;
  private final DataType _columnType;
  private final double _numberOfValuesPerEntry;

  private final int _initialValue;
  private final Random _random;

  private int _counter = 0;

  public NumberGenerator(Integer cardinality, DataType type, Double numberOfValuesPerEntry) {
    this(cardinality, type, numberOfValuesPerEntry, new Random(System.currentTimeMillis()));
  }

  @VisibleForTesting
  NumberGenerator(Integer cardinality, DataType type, Double numberOfValuesPerEntry, Random random) {
    _cardinality = cardinality;
    _numberOfValuesPerEntry =
        numberOfValuesPerEntry != null ? numberOfValuesPerEntry : DEFAULT_NUMBER_OF_VALUES_PER_ENTRY;
    Preconditions.checkState(_numberOfValuesPerEntry >= 1,
        "Number of values per entry (should be >= 1): " + _numberOfValuesPerEntry);
    _columnType = type;
    _random = random;
    _initialValue = random.nextInt(100);
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    if (_numberOfValuesPerEntry == 1) {
      return getNextNumber();
    }
    return MultiValueGeneratorHelper.generateMultiValueEntries(_numberOfValuesPerEntry, _random, this::getNextNumber);
  }

  private Number getNextNumber() {
    if (_counter == _cardinality) {
      _counter = 0;
    }
    int newValue = _initialValue + _counter;
    _counter++;
    switch (_columnType) {
      case INT:
        return newValue;
      case LONG:
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
      case DATE:
      case TIME:
        return (long) newValue;
      case FLOAT:
        return newValue + 0.5f;
      case DOUBLE:
        return newValue + 0.5;
      default:
        throw new RuntimeException("number generator can only accept a column of type number and this : " + _columnType
            + " is not a supported number type");
    }
  }

  public static void main(String[] args) {
    final NumberGenerator gen = new NumberGenerator(10000000, DataType.LONG, null);
    gen.init();
    for (int i = 0; i < 1000; i++) {
      System.out.println(gen.next());
    }
  }
}
