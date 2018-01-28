/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldRole;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.IntRange;


/**
 * Sep 12, 2014
 */

public class StringGenerator implements Generator {

  private static final int lengthOfEachString = 10;

  private final int cardinality;
  private final Random rand;
  private List<String> vals;
  private final FieldRole role;
  private  final IntRange lengthRange;
  private  int nextStringIndex = 0;

  public StringGenerator()
  {
    this.cardinality = 1000;
    rand = new Random(System.currentTimeMillis());
    this.role = FieldRole.TYPICAL;
    lengthRange = new IntRange(10,100);
    nextStringIndex = 0;
  }

  public StringGenerator(Integer cardinality) {
    this.cardinality = cardinality.intValue();
    rand = new Random(System.currentTimeMillis());
    this.role = FieldRole.TYPICAL;
    lengthRange = new IntRange(10,100);
    nextStringIndex = 0;
  }

  public StringGenerator(Integer cardinality, FieldRole role, IntRange lengthRange) {
    this.cardinality = cardinality.intValue();
    rand = new Random(System.currentTimeMillis());
    this.role = role;
    this.lengthRange = lengthRange;
    nextStringIndex = 0;
  }

  @Override
  public void init() {
    final Set<String> uniqueStrings = new HashSet<String>();

    int stringLengthRange = lengthRange.getMaximumInteger() - lengthRange.getMinimumInteger();
    for (int i = 0; i < cardinality; i++) {
      int lengthOfString = lengthRange.getMinimumInteger() + rand.nextInt(stringLengthRange);
      while (!uniqueStrings.add(RandomStringUtils.randomAlphabetic(lengthOfString))) {
        uniqueStrings.add(RandomStringUtils.randomAlphabetic(lengthOfString));
      }
    }
    vals = new ArrayList<String>(uniqueStrings);
  }

  @Override
  public Object next() {
    if(role != FieldRole.ID)
    {
      return vals.get(rand.nextInt(cardinality));
    }
    else
    {
      if(nextStringIndex >= cardinality)
        nextStringIndex = 0;

      return vals.get(nextStringIndex++);

    }
  }

  public static void main(String[] args) {
    final StringGenerator gen = new StringGenerator(10000);
    gen.init();
    for (int i = 0; i < 1000000; i++) {
      System.out.println(gen.next());
    }
  }
}
