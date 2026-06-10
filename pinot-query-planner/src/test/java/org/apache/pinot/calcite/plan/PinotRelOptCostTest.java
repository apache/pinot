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
package org.apache.pinot.calcite.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCost;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for {@link PinotRelOptCost} and {@link PinotRelOptCost.Factory}.
 */
public class PinotRelOptCostTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static PinotRelOptCost cost(double rows, double cpu, double io) {
    return new PinotRelOptCost(rows, cpu, io);
  }

  // ---------------------------------------------------------------------------
  // Ordering: row count dominates
  // ---------------------------------------------------------------------------

  @Test
  public void testRowCountDominates() {
    // Fewer rows is always cheaper, regardless of cpu/io
    RelOptCost cheap = cost(100, 1000, 1000);
    RelOptCost expensive = cost(1000, 0, 0);

    assertTrue(cheap.isLt(expensive), "100 rows < 1000 rows");
    assertTrue(cheap.isLe(expensive), "100 rows <= 1000 rows");
    assertFalse(expensive.isLt(cheap));
    assertFalse(expensive.isLe(cheap));
  }

  @Test
  public void testEqualRowsCpuBreaksTie() {
    RelOptCost lowCpu = cost(500, 10, 999);
    RelOptCost highCpu = cost(500, 100, 0);

    assertTrue(lowCpu.isLt(highCpu), "same rows, lower cpu is cheaper");
    assertTrue(lowCpu.isLe(highCpu));
    assertFalse(highCpu.isLt(lowCpu));
    assertFalse(highCpu.isLe(lowCpu));
  }

  @Test
  public void testEqualRowsAndCpuIoBreaksTie() {
    RelOptCost lowIo = cost(500, 10, 5);
    RelOptCost highIo = cost(500, 10, 50);

    assertTrue(lowIo.isLt(highIo), "same rows+cpu, lower io is cheaper");
    assertTrue(lowIo.isLe(highIo));
    assertFalse(highIo.isLt(lowIo));
    assertFalse(highIo.isLe(lowIo));
  }

  @Test
  public void testIsLeReflexive() {
    RelOptCost c = cost(42, 7, 3);
    assertTrue(c.isLe(c), "cost.isLe(itself) must be true");
  }

  @Test
  public void testIdenticalCostsAreLeButNotLt() {
    RelOptCost a = cost(100, 200, 300);
    RelOptCost b = cost(100, 200, 300);

    assertTrue(a.isLe(b));
    assertTrue(b.isLe(a));
    assertFalse(a.isLt(b));
    assertFalse(b.isLt(a));
  }

  // ---------------------------------------------------------------------------
  // INFINITY / HUGE ordering
  // ---------------------------------------------------------------------------

  @Test
  public void testInfinityGreaterThanEverything() {
    RelOptCost huge = PinotRelOptCost.HUGE;
    RelOptCost inf = PinotRelOptCost.INFINITY;
    RelOptCost large = cost(Double.MAX_VALUE / 2, Double.MAX_VALUE / 2, Double.MAX_VALUE / 2);

    assertTrue(huge.isLt(inf), "HUGE < INFINITY");
    assertTrue(large.isLt(inf), "large finite cost < INFINITY");
    assertFalse(inf.isLe(huge), "INFINITY is not <= HUGE");
    assertFalse(inf.isLt(inf), "INFINITY is not < INFINITY");
    assertTrue(inf.isLe(inf), "INFINITY <= INFINITY");
  }

  @Test
  public void testZeroLeqTiny() {
    assertTrue(PinotRelOptCost.ZERO.isLe(PinotRelOptCost.TINY), "ZERO <= TINY");
    assertTrue(PinotRelOptCost.ZERO.isLt(PinotRelOptCost.TINY), "ZERO < TINY");
  }

  // ---------------------------------------------------------------------------
  // Total order sanity (no cycles)
  // ---------------------------------------------------------------------------

  @Test
  public void testTotalOrderConsistency() {
    List<PinotRelOptCost> costs = new ArrayList<>(Arrays.asList(
        cost(0, 0, 0),
        cost(1, 0, 0),
        cost(1, 1, 0),
        cost(1, 1, 1),
        cost(100, 0, 0),
        cost(100, 50, 0),
        cost(100, 50, 25),
        cost(1000, 1000, 1000),
        PinotRelOptCost.HUGE,
        PinotRelOptCost.INFINITY
    ));

    Collections.shuffle(costs);

    // Sort using isLt — verify no anti-symmetry violation
    costs.sort((a, b) -> {
      if (a.isLt(b)) {
        return -1;
      }
      if (b.isLt(a)) {
        return 1;
      }
      return 0;
    });

    // Verify strict ascending order (no cost is simultaneously < its successor and its successor < it)
    for (int i = 0; i < costs.size() - 1; i++) {
      PinotRelOptCost a = costs.get(i);
      PinotRelOptCost b = costs.get(i + 1);
      assertTrue(a.isLe(b), "after sort, cost[" + i + "] must be <= cost[" + (i + 1) + "]");
      assertFalse(b.isLt(a), "no cycle: cost[" + (i + 1) + "] must not be < cost[" + i + "]");
    }
  }

  // ---------------------------------------------------------------------------
  // Arithmetic: plus
  // ---------------------------------------------------------------------------

  @Test
  public void testPlusComponentwise() {
    RelOptCost a = cost(10, 20, 30);
    RelOptCost b = cost(1, 2, 3);
    RelOptCost sum = a.plus(b);

    assertEquals(sum.getRows(), 11.0);
    assertEquals(sum.getCpu(), 22.0);
    assertEquals(sum.getIo(), 33.0);
  }

  @Test
  public void testPlusWithInfinityReturnsInfinity() {
    RelOptCost c = cost(10, 10, 10);
    assertSame(c.plus(PinotRelOptCost.INFINITY), PinotRelOptCost.INFINITY);
    assertSame(PinotRelOptCost.INFINITY.plus(c), PinotRelOptCost.INFINITY);
  }

  // ---------------------------------------------------------------------------
  // Arithmetic: minus
  // ---------------------------------------------------------------------------

  @Test
  public void testMinusComponentwise() {
    RelOptCost a = cost(100, 50, 30);
    RelOptCost b = cost(40, 20, 10);
    RelOptCost diff = a.minus(b);

    assertEquals(diff.getRows(), 60.0);
    assertEquals(diff.getCpu(), 30.0);
    assertEquals(diff.getIo(), 20.0);
  }

  @Test
  public void testMinusInfinityReturnsInfinity() {
    assertSame(PinotRelOptCost.INFINITY.minus(cost(1, 1, 1)), PinotRelOptCost.INFINITY);
  }

  // ---------------------------------------------------------------------------
  // Arithmetic: multiplyBy
  // ---------------------------------------------------------------------------

  @Test
  public void testMultiplyByScalesComponents() {
    RelOptCost c = cost(10, 20, 30);
    RelOptCost scaled = c.multiplyBy(2.5);

    assertEquals(scaled.getRows(), 25.0);
    assertEquals(scaled.getCpu(), 50.0);
    assertEquals(scaled.getIo(), 75.0);
  }

  @Test
  public void testMultiplyByZeroGivesZeroComponents() {
    RelOptCost c = cost(100, 200, 300);
    RelOptCost result = c.multiplyBy(0.0);

    assertEquals(result.getRows(), 0.0);
    assertEquals(result.getCpu(), 0.0);
    assertEquals(result.getIo(), 0.0);
  }

  @Test
  public void testMultiplyByOnInfinityStaysInfinite() {
    RelOptCost result = PinotRelOptCost.INFINITY.multiplyBy(0.5);
    assertSame(result, PinotRelOptCost.INFINITY, "INFINITY.multiplyBy stays INFINITY");
    assertTrue(result.isInfinite());
  }

  // ---------------------------------------------------------------------------
  // Arithmetic: divideBy
  // ---------------------------------------------------------------------------

  @Test
  public void testDivideByGeometricMean() {
    // a = (2,4,8), b = (1,2,4)  → ratios 2,2,2 → geo-mean = 2
    RelOptCost a = cost(2, 4, 8);
    RelOptCost b = cost(1, 2, 4);
    assertEquals(a.divideBy(b), 2.0, 1.0e-9);
  }

  @Test
  public void testDivideByWhenDenominatorHasZeroComponents() {
    // b has rows=0 (skipped), cpu and io non-zero
    RelOptCost a = cost(0, 6, 9);
    RelOptCost b = cost(0, 2, 3);
    // only cpu and io contribute: ratios 3,3 → geo-mean = 3
    assertEquals(a.divideBy(b), 3.0, 1.0e-9);
  }

  @Test
  public void testDivideByAllZerosReturnsOne() {
    RelOptCost a = cost(0, 0, 0);
    RelOptCost b = cost(0, 0, 0);
    assertEquals(a.divideBy(b), 1.0, 1.0e-9);
  }

  // ---------------------------------------------------------------------------
  // equals and isEqWithEpsilon
  // ---------------------------------------------------------------------------

  @Test
  public void testEqualsExact() {
    RelOptCost a = cost(1.0, 2.0, 3.0);
    RelOptCost b = cost(1.0, 2.0, 3.0);
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void testEqualsNotWithSmallDelta() {
    double delta = PinotRelOptCost.EPSILON * 10; // larger than epsilon → not equal
    RelOptCost a = cost(1.0, 2.0, 3.0);
    RelOptCost b = cost(1.0 + delta, 2.0, 3.0);
    assertFalse(a.equals(b));
  }

  @Test
  public void testIsEqWithEpsilonTrueForTinyDelta() {
    double delta = PinotRelOptCost.EPSILON / 2; // within epsilon → equal with epsilon
    RelOptCost a = cost(1.0, 2.0, 3.0);
    RelOptCost b = cost(1.0 + delta, 2.0 + delta, 3.0 + delta);
    assertTrue(a.isEqWithEpsilon(b), "within epsilon → isEqWithEpsilon should be true");
    assertFalse(a.equals(b), "but exact equals should be false");
  }

  @Test
  public void testIsEqWithEpsilonFalseForLargeDelta() {
    double delta = PinotRelOptCost.EPSILON * 10;
    RelOptCost a = cost(1.0, 2.0, 3.0);
    RelOptCost b = cost(1.0 + delta, 2.0, 3.0);
    assertFalse(a.isEqWithEpsilon(b));
  }

  @Test
  public void testEqualsSelf() {
    RelOptCost c = cost(5, 5, 5);
    assertTrue(c.equals(c));
    assertTrue(c.isEqWithEpsilon(c));
  }

  @Test
  public void testEqualsObjectOverload() {
    PinotRelOptCost a = cost(1, 2, 3);
    PinotRelOptCost b = cost(1, 2, 3);
    assertEquals(a, b); // uses equals(Object)
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsObjectOverloadMismatchedType() {
    PinotRelOptCost a = cost(1, 2, 3);
    assertFalse(a.equals("not a cost"));
    assertFalse(a.equals(null));
  }

  // ---------------------------------------------------------------------------
  // isInfinite
  // ---------------------------------------------------------------------------

  @Test
  public void testIsInfiniteConstants() {
    assertTrue(PinotRelOptCost.INFINITY.isInfinite());
    assertFalse(PinotRelOptCost.HUGE.isInfinite());
    assertFalse(PinotRelOptCost.ZERO.isInfinite());
    assertFalse(PinotRelOptCost.TINY.isInfinite());
  }

  @Test
  public void testIsInfiniteForComponentInfinity() {
    assertTrue(cost(Double.POSITIVE_INFINITY, 0, 0).isInfinite());
    assertTrue(cost(0, Double.POSITIVE_INFINITY, 0).isInfinite());
    assertTrue(cost(0, 0, Double.POSITIVE_INFINITY).isInfinite());
  }

  // ---------------------------------------------------------------------------
  // Factory
  // ---------------------------------------------------------------------------

  @Test
  public void testFactoryMakeCost() {
    PinotRelOptCost.Factory factory = PinotRelOptCost.Factory.INSTANCE;
    RelOptCost c = factory.makeCost(5, 10, 15);
    assertEquals(c.getRows(), 5.0);
    assertEquals(c.getCpu(), 10.0);
    assertEquals(c.getIo(), 15.0);
  }

  @Test
  public void testFactoryConstants() {
    PinotRelOptCost.Factory factory = PinotRelOptCost.Factory.INSTANCE;
    assertSame(factory.makeInfiniteCost(), PinotRelOptCost.INFINITY);
    assertSame(factory.makeHugeCost(), PinotRelOptCost.HUGE);
    assertSame(factory.makeZeroCost(), PinotRelOptCost.ZERO);
    assertSame(factory.makeTinyCost(), PinotRelOptCost.TINY);
  }

  @Test
  public void testFactoryInfiniteCostIsInfinite() {
    assertTrue(PinotRelOptCost.Factory.INSTANCE.makeInfiniteCost().isInfinite());
  }

  @Test
  public void testFactoryHugeCostNotInfinite() {
    assertFalse(PinotRelOptCost.Factory.INSTANCE.makeHugeCost().isInfinite());
  }

  // ---------------------------------------------------------------------------
  // toString
  // ---------------------------------------------------------------------------

  @Test
  public void testToStringFiniteCost() {
    String s = cost(1.0, 2.0, 3.0).toString();
    assertTrue(s.contains("rows"), "toString should mention rows");
    assertTrue(s.contains("cpu"), "toString should mention cpu");
    assertTrue(s.contains("io"), "toString should mention io");
  }

  @Test
  public void testToStringInfinity() {
    assertNotNull(PinotRelOptCost.INFINITY.toString());
  }
}
