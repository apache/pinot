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

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class UuidGeneratorTest {

  /// Single-value generation must yield canonical, parseable UUID strings.
  @Test
  public void testSingleValueProducesCanonicalUuids() {
    UuidGenerator generator = new UuidGenerator(1000, 1.0, new Random(42));
    generator.init();
    for (int i = 0; i < 100; i++) {
      String value = (String) generator.next();
      // Round-trips through UUID.fromString and is already in canonical lowercase form.
      assertEquals(UUID.fromString(value).toString(), value);
    }
  }

  /// Distinct values must be bounded by the requested cardinality, and the low-order counter must cycle.
  @Test
  public void testCardinalityBoundsAndCycles() {
    int cardinality = 3;
    UuidGenerator generator = new UuidGenerator(cardinality, 1.0, new Random(42));
    generator.init();

    String first = (String) generator.next();
    String second = (String) generator.next();
    String third = (String) generator.next();
    String fourth = (String) generator.next();

    Set<String> distinct = new HashSet<>(List.of(first, second, third, fourth));
    assertEquals(distinct.size(), cardinality, "distinct values must not exceed cardinality");
    assertEquals(fourth, first, "the counter must cycle back to the first value after cardinality entries");
  }

  /// A null numberOfValuesPerEntry must default to single-value generation (a bare string, not a list).
  @Test
  public void testNullNumberOfValuesPerEntryDefaultsToSingleValue() {
    UuidGenerator generator = new UuidGenerator(1000, null, new Random(42));
    generator.init();
    assertTrue(generator.next() instanceof String, "null numberOfValuesPerEntry must yield a single string value");
  }

  /// Multi-value generation must return a list of parseable UUID strings.
  @Test
  public void testMultiValueReturnsListOfUuids() {
    UuidGenerator generator = new UuidGenerator(1000, 3.0, new Random(42));
    generator.init();

    Object next = generator.next();
    assertTrue(next instanceof List, "multi-value entry must be a List");
    List<?> values = (List<?>) next;
    // For an integer numberOfValuesPerEntry the count is deterministic (see MultiValueGeneratorHelper).
    assertEquals(values.size(), 3);
    for (Object value : values) {
      String uuid = (String) value;
      assertEquals(UUID.fromString(uuid).toString(), uuid);
    }
  }
}
