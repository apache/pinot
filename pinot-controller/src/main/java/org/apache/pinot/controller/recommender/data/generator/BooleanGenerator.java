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
import java.util.Random;


public class BooleanGenerator implements Generator {

  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;

  private final double _numberOfValuesPerEntry;
  private final Random _random;

  public BooleanGenerator(Double numberOfValuesPerEntry) {
    this(numberOfValuesPerEntry, new Random(System.currentTimeMillis()));
  }

  @VisibleForTesting
  BooleanGenerator(Double numberOfValuesPerEntry, Random random) {
    _numberOfValuesPerEntry =
        numberOfValuesPerEntry != null ? numberOfValuesPerEntry : DEFAULT_NUMBER_OF_VALUES_PER_ENTRY;
    _random = random;
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    if (_numberOfValuesPerEntry == 1) {
      return getNextBooleanAsInteger();
    }
    return MultiValueGeneratorHelper
        .generateMultiValueEntries(_numberOfValuesPerEntry, _random, this::getNextBooleanAsInteger);
  }

  private int getNextBooleanAsInteger() {
    return _random.nextBoolean() ? 1 : 0;
  }
}
