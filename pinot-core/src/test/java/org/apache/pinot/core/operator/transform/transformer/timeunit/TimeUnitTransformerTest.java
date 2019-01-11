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
package org.apache.pinot.core.operator.transform.transformer.timeunit;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.joda.time.DurationFieldType;
import org.joda.time.chrono.ISOChronology;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TimeUnitTransformerTest {
  private static final int NUM_ROWS = 100;
  private static final Random RANDOM = new Random();

  private final long[] _input = new long[NUM_ROWS];
  private final long[] _output = new long[NUM_ROWS];

  @BeforeClass
  public void setUp() {
    long currentTimeMs = System.currentTimeMillis();
    for (int i = 0; i < NUM_ROWS; i++) {
      _input[i] = Math.abs(RANDOM.nextLong()) % currentTimeMs;
    }
  }

  @Test
  public void testJavaTimeUnit() {
    TimeUnitTransformer timeUnitTransformer =
        TimeUnitTransformerFactory.getTimeUnitTransformer(TimeUnit.MILLISECONDS, "dAyS");
    Assert.assertTrue(timeUnitTransformer instanceof JavaTimeUnitTransformer);
    timeUnitTransformer.transform(_input, _output, NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      long expected = TimeUnit.MILLISECONDS.toDays(_input[i]);
      Assert.assertEquals(_output[i], expected);
    }
  }

  @Test
  public void testCustomTimeUnit() {
    TimeUnitTransformer timeUnitTransformer =
        TimeUnitTransformerFactory.getTimeUnitTransformer(TimeUnit.MILLISECONDS, "wEeKs");
    Assert.assertTrue(timeUnitTransformer instanceof CustomTimeUnitTransformer);
    timeUnitTransformer.transform(_input, _output, NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = DurationFieldType.weeks().getField(ISOChronology.getInstanceUTC()).getDifference(_input[i], 0L);
      Assert.assertEquals(_output[i], expected);
    }

    timeUnitTransformer = TimeUnitTransformerFactory.getTimeUnitTransformer(TimeUnit.MILLISECONDS, "mOnThS");
    Assert.assertTrue(timeUnitTransformer instanceof CustomTimeUnitTransformer);
    timeUnitTransformer.transform(_input, _output, NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = DurationFieldType.months().getField(ISOChronology.getInstanceUTC()).getDifference(_input[i], 0L);
      Assert.assertEquals(_output[i], expected);
    }

    timeUnitTransformer = TimeUnitTransformerFactory.getTimeUnitTransformer(TimeUnit.MILLISECONDS, "yEaRs");
    Assert.assertTrue(timeUnitTransformer instanceof CustomTimeUnitTransformer);
    timeUnitTransformer.transform(_input, _output, NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = DurationFieldType.years().getField(ISOChronology.getInstanceUTC()).getDifference(_input[i], 0L);
      Assert.assertEquals(_output[i], expected);
    }
  }
}
