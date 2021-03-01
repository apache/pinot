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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;


/**
 * Sep 12, 2014
 */

public class StringGenerator implements Generator {
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;
  private static final int DEFAULT_LENGTH_OF_EACH_STRING = 10;

  private final int cardinality;
  private final Random rand;
  private final double numberOfValuesPerEntry;
  private final int lengthOfEachString;

  private List<String> vals;

  public StringGenerator(Integer cardinality, Double numberOfValuesPerEntry, Integer lengthOfEachString) {
    this.cardinality = cardinality;
    this.numberOfValuesPerEntry =
        numberOfValuesPerEntry != null ? numberOfValuesPerEntry : DEFAULT_NUMBER_OF_VALUES_PER_ENTRY;
    this.lengthOfEachString = lengthOfEachString != null ? lengthOfEachString : DEFAULT_LENGTH_OF_EACH_STRING;
    Preconditions.checkState(this.numberOfValuesPerEntry >= 1,
        "Number of values per entry (should be >= 1): " + this.numberOfValuesPerEntry);
    rand = new Random(System.currentTimeMillis());
  }

  @Override
  public void init() {
    final Set<String> uniqueStrings = new HashSet<String>();
    for (int i = 0; i < cardinality; i++) {
      while (!uniqueStrings.add(RandomStringUtils.randomAlphabetic(lengthOfEachString))) {
        uniqueStrings.add(RandomStringUtils.randomAlphabetic(lengthOfEachString));
      }
    }
    vals = new ArrayList<String>(uniqueStrings);
  }

  @Override
  public Object next() {
    if (numberOfValuesPerEntry == 1) {
      return getNextString();
    }
    return MultiValueGeneratorHelper.generateMultiValueEntries(numberOfValuesPerEntry, rand, this::getNextString);
  }

  private String getNextString() {
    return vals.get(rand.nextInt(cardinality));
  }

  public static void main(String[] args) {
    final StringGenerator gen = new StringGenerator(10000, null, null);
    gen.init();
    for (int i = 0; i < 1000000; i++) {
      System.out.println(gen.next());
    }
  }
}
