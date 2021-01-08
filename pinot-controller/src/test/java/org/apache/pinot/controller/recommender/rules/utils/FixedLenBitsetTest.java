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

package org.apache.pinot.controller.recommender.rules.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.testng.annotations.Test;


/**
 * Tests the {@link FixedLenBitset} class based on different scenarios.
 */
public class FixedLenBitsetTest {

  /**
   * Tests {@link FixedLenBitset#add(int)} with positive integer.
   */
  @Test
  public void testAdd() {
    int size = 8;
    int integerToAdd = 2;
    FixedLenBitset fixedLenBitset = new FixedLenBitset(size);
    fixedLenBitset.add(integerToAdd);
    int cardinality = fixedLenBitset.getCardinality();
    assertEquals(cardinality, 1);
    assertTrue(fixedLenBitset.contains(integerToAdd));
    assertEquals(fixedLenBitset.getSize(), size);
  }

  /**
   * Tests {@link FixedLenBitset#add(int)} with negative integer.
   */
  @Test
  public void testAddNegativeInteger() {
    int size = 8;
    int integerToAdd = -1;
    FixedLenBitset fixedLenBitset = new FixedLenBitset(size);
    fixedLenBitset.add(integerToAdd);
    int cardinality = fixedLenBitset.getCardinality();
    assertEquals(cardinality, 0);
    assertEquals(fixedLenBitset.getSize(), size);
  }

  /**
   * Tests {@link FixedLenBitset#add(int)} with integer greater than bitset size.
   */
  @Test
  public void testAddIntegerGreaterThanSize() {
    int size = 8;
    int integerToAdd = 10;
    FixedLenBitset fixedLenBitset = new FixedLenBitset(size);
    fixedLenBitset.add(integerToAdd);
    int cardinality = fixedLenBitset.getCardinality();
    assertEquals(cardinality, 0);
    assertEquals(fixedLenBitset.getSize(), size);
  }

  /**
   * Tests {@link FixedLenBitset#add(int)} with already present integer.
   */
  @Test
  public void testAddAlreadyPresentInteger() {
    int size = 8;
    int integerToAdd = 2;
    FixedLenBitset fixedLenBitset = new FixedLenBitset(size);
    //adding first time
    fixedLenBitset.add(integerToAdd);
    int cardinality = fixedLenBitset.getCardinality();
    assertEquals(cardinality, 1);
    //adding same integer again
    fixedLenBitset.add(integerToAdd);
    cardinality = fixedLenBitset.getCardinality();
    assertEquals(cardinality, 1);
    assertEquals(fixedLenBitset.getSize(), size);
  }

  /**
   * Tests {@link FixedLenBitset#union(FixedLenBitset)}.
   */
  @Test
  public void testUnion() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(25, 16, 22, 25);
    List<Integer> integerList2 = Arrays.asList(32, 60, 54, 25, 16);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }

    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList2) {
      fixedLenBitset2.add(integerToAdd);
    }
    fixedLenBitset1.union(fixedLenBitset2);

    List<Integer> unionList = new ArrayList<>();
    unionList.addAll(integerList1);
    unionList.addAll(integerList2);
    int expectedCardinality = unionList.stream().distinct().collect(Collectors.toList()).size();

    int cardinality = fixedLenBitset1.getCardinality();
    assertEquals(cardinality, expectedCardinality);
    assertTrue(fixedLenBitset1.hasCandidateDim());
    assertTrue(fixedLenBitset2.hasCandidateDim());
    for (Integer addedIntegers : unionList) {
      assertTrue(fixedLenBitset1.contains(addedIntegers));
    }
  }

  /**
   * Tests {@link FixedLenBitset#union(FixedLenBitset)} with empty bitset.
   */
  @Test
  public void testUnionWithEmptySecondBitset() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(25, 16, 22, 25);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }

    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);

    fixedLenBitset1.union(fixedLenBitset2);

    int expectedCardinality = integerList1.stream().distinct().collect(Collectors.toList()).size();

    int cardinality = fixedLenBitset1.getCardinality();
    assertEquals(cardinality, expectedCardinality);
    assertTrue(fixedLenBitset1.hasCandidateDim());
    assertTrue(fixedLenBitset2.isEmpty());
    for (Integer addedIntegers : integerList1) {
      assertTrue(fixedLenBitset1.contains(addedIntegers));
    }
  }

  /**
   * Tests {@link FixedLenBitset#intersect(FixedLenBitset)}.
   */
  @Test
  public void testIntersect() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(25, 16, 22, 25);
    List<Integer> integerList2 = Arrays.asList(32, 60, 54, 25, 16);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }

    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList2) {
      fixedLenBitset2.add(integerToAdd);
    }
    fixedLenBitset1.intersect(fixedLenBitset2);

    List<Integer> intersectList = new ArrayList<>();
    intersectList.addAll(integerList1);
    intersectList.retainAll(integerList2);
    int expectedCardinality = intersectList.stream().distinct().collect(Collectors.toList()).size();

    int cardinality = fixedLenBitset1.getCardinality();
    assertEquals(cardinality, expectedCardinality);
    assertTrue(fixedLenBitset1.hasCandidateDim());
    assertTrue(fixedLenBitset2.hasCandidateDim());
    for (Integer addedIntegers : intersectList) {
      assertTrue(fixedLenBitset1.contains(addedIntegers));
    }
  }

  /**
   * Tests {@link FixedLenBitset#intersect(FixedLenBitset)} with empty bitset.
   */
  @Test
  public void testIntersectWithEmptySecondBitset() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(25, 16, 22, 25);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }

    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);
    fixedLenBitset1.intersect(fixedLenBitset2);

    int expectedCardinality = integerList1.stream().distinct().collect(Collectors.toList()).size();

    int cardinality = fixedLenBitset1.getCardinality();
    assertEquals(cardinality, expectedCardinality);
    assertTrue(fixedLenBitset1.hasCandidateDim());
    assertTrue(fixedLenBitset2.isEmpty());
    for (Integer addedIntegers : integerList1) {
      assertFalse(fixedLenBitset1.contains(addedIntegers));
    }
  }

  /**
   * Tests {@link FixedLenBitset#contains(FixedLenBitset)} for true condition.
   */
  @Test
  public void testContainsTrueCase() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(32, 60, 54, 25, 16);
    List<Integer> integerList2 = Arrays.asList(60, 16, 54);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }
    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList2) {
      fixedLenBitset2.add(integerToAdd);
    }

    boolean contains = fixedLenBitset1.contains(fixedLenBitset2);

    assertEquals(contains, integerList1.containsAll(integerList2));

  }

  /**
   * Tests {@link FixedLenBitset#contains(FixedLenBitset)} with empty bitset.
   */
  @Test
  public void testContainsWithEmptySecondBitset() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(32, 60, 54, 25, 16);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }
    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);

    boolean contains = fixedLenBitset1.contains(fixedLenBitset2);

    assertEquals(contains, true);

  }

  /**
   * Tests {@link FixedLenBitset#contains(FixedLenBitset)} for false condition.
   */
  @Test
  public void testContainsFalseCase() {
    int size = 64;

    List<Integer> integerList1 = Arrays.asList(32, 54, 25, 16);
    List<Integer> integerList2 = Arrays.asList(60, 17, 54);

    FixedLenBitset fixedLenBitset1 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList1) {
      fixedLenBitset1.add(integerToAdd);
    }
    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList2) {
      fixedLenBitset2.add(integerToAdd);
    }

    boolean contains = fixedLenBitset1.contains(fixedLenBitset2);

    assertEquals(contains, integerList1.containsAll(integerList2));

  }

  /**
   * Tests {@link FixedLenBitset#getOffsets()}.
   */
  @Test
  public void testGetOffsets() {
    int size = 64;
    List<Integer> integerList = Arrays.asList(32, 54, 25, 16);
    FixedLenBitset fixedLenBitset = new FixedLenBitset(size);
    for (Integer integerToAdd : integerList) {
      fixedLenBitset.add(integerToAdd);
    }
    List<Integer> offsets = fixedLenBitset.getOffsets();
    assertTrue(integerList.containsAll(offsets));
    assertTrue(offsets.containsAll(integerList));
  }

}
