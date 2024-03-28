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
package org.apache.pinot.core.operator.filter.predicate;

import java.math.BigDecimal;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;


public class PredicateEvaluatorTestUtils {
  private PredicateEvaluatorTestUtils() {
  }

  public static final Random RANDOM = new Random();

  public static void fillRandom(int[] randomValues) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = RANDOM.nextInt();
    }
  }

  public static void fillRandom(long[] randomValues) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = RANDOM.nextLong();
    }
  }

  public static void fillRandom(float[] randomValues) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = RANDOM.nextFloat();
    }
  }

  public static void fillRandom(double[] randomValues) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = RANDOM.nextDouble();
    }
  }

  public static void fillRandom(BigDecimal[] randomValues) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = BigDecimal.valueOf(RANDOM.nextDouble());
    }
  }

  public static void fillRandom(String[] randomValues, int maxStringLength) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = RandomStringUtils.random(maxStringLength);
    }
  }

  public static void fillRandom(byte[][] randomValues, int maxStringLength) {
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = RandomStringUtils.random(maxStringLength).getBytes();
    }
  }
}
