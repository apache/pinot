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

import com.google.common.base.Preconditions;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;


/**
 * Sep 12, 2014
 */

public class StringGenerator implements Generator {
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;
  private static final int DEFAULT_LENGTH_OF_EACH_STRING = 10;

  private final int _cardinality;
  private final Random _rand;
  private final double _numberOfValuesPerEntry;

  private final String _initialValue;
  private final int _counterLength;
  private int _counter = 0;

  public StringGenerator(Integer cardinality, Double numberOfValuesPerEntry, Integer lengthOfEachString) {
    _cardinality = cardinality;
    _numberOfValuesPerEntry =
        numberOfValuesPerEntry != null ? numberOfValuesPerEntry : DEFAULT_NUMBER_OF_VALUES_PER_ENTRY;
    lengthOfEachString = lengthOfEachString != null ? lengthOfEachString : DEFAULT_LENGTH_OF_EACH_STRING;
    Preconditions.checkState(_numberOfValuesPerEntry >= 1,
        "Number of values per entry (should be >= 1): " + _numberOfValuesPerEntry);
    _counterLength = String.valueOf(_cardinality).length();
    int initValueSize = lengthOfEachString - _counterLength;
    Preconditions.checkState(initValueSize >= 0,
        String.format("Cannot generate %d unique string with length %d", _cardinality, lengthOfEachString));
    _initialValue = RandomStringUtils.randomAlphabetic(initValueSize);
    _rand = new Random(System.currentTimeMillis());
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    if (_numberOfValuesPerEntry == 1) {
      return getNextString();
    }
    return MultiValueGeneratorHelper.generateMultiValueEntries(_numberOfValuesPerEntry, _rand, this::getNextString);
  }

  private String getNextString() {
    if (_counter == _cardinality) {
      _counter = 0;
    }
    _counter++;
    return _initialValue + StringUtils.leftPad(String.valueOf(_counter), _counterLength, '0');
  }

  public static void main(String[] args) {
    final StringGenerator gen = new StringGenerator(10000, null, null);
    gen.init();
    for (int i = 0; i < 1000000; i++) {
      System.out.println(gen.next());
    }
  }
}
