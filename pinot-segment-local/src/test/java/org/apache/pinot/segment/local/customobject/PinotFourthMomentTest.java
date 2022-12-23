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

package org.apache.pinot.segment.local.customobject;

import java.util.Random;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PinotFourthMomentTest {

  @Test
  public void shouldCombineMoments() {
    // Given:
    Random r = new Random();
    double[] xs = IntStream.generate(r::nextInt)
        .limit(100)
        .mapToDouble(i -> (double) i)
        .toArray();

    PinotFourthMoment a = new PinotFourthMoment();
    PinotFourthMoment b = new PinotFourthMoment();
    PinotFourthMoment c = new PinotFourthMoment();

    // When:
    for (int i = 0; i < xs.length; i++) {
      a.increment(xs[i]);
      (i < xs.length / 2 ? b : c).increment(xs[i]);
    }
    b.combine(c);

    // Then:
    assertEquals(b.skew(), a.skew(), .01);
    assertEquals(b.kurtosis(), a.kurtosis(), .01);
  }

  @Test
  public void shouldCombineLeftEmptyMoments() {
    // Given:
    Random r = new Random();
    double[] xs = IntStream.generate(r::nextInt)
        .limit(100)
        .mapToDouble(i -> (double) i)
        .toArray();

    PinotFourthMoment a = new PinotFourthMoment();
    PinotFourthMoment b = new PinotFourthMoment();

    // When:
    for (double x : xs) {
      a.increment(x);
    }

    b.combine(a);

    // Then:
    assertEquals(b.kurtosis(), a.kurtosis(), .01);
  }

  @Test
  public void shouldCombineRightEmptyMoments() {
    // Given:
    Random r = new Random();
    double[] xs = IntStream.generate(r::nextInt)
        .limit(100)
        .mapToDouble(i -> (double) i)
        .toArray();

    PinotFourthMoment a = new PinotFourthMoment();
    PinotFourthMoment b = new PinotFourthMoment();

    // When:
    for (double x : xs) {
      a.increment(x);
    }

    double kurtosisBeforeCombine = a.kurtosis();
    a.combine(b);

    // Then:
    assertEquals(a.kurtosis(), kurtosisBeforeCombine, .01);
  }
}
