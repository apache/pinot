/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.data.generator;

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

  private static final int lengthOfEachString = 10;

  private final int cardinality;
  private final Random rand;
  private List<String> vals;

  public StringGenerator(Integer cardinality) {
    this.cardinality = cardinality.intValue();
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
    return vals.get(rand.nextInt(cardinality));
  }

  public static void main(String[] args) {
    final StringGenerator gen = new StringGenerator(10000);
    gen.init();
    for (int i = 0; i < 1000000; i++) {
      System.out.println(gen.next());
    }
  }
}
