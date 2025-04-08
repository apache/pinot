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
package org.apache.pinot.query.planner.physical.v2.mapping;

import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotDistMappingTest {
  @Test
  public void testIdentityMapping() {
    // Test identity mapping returns the expected mapping.
    PinotDistMapping mapping = PinotDistMapping.identity(10);
    assertEquals(mapping.getSourceCount(), 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(mapping.getTarget(i), i);
    }
    // Test getMappedKeys always returns the same values as the input.
    List<List<Integer>> testKeys = List.of(
        List.of(0),
        List.of(9),
        List.of(1, 3, 5),
        List.of(0, 2, 4),
        List.of(4, 2, 9)
    );
    for (List<Integer> keys : testKeys) {
      assertEquals(mapping.getMappedKeys(keys), keys);
    }
  }

  @Test
  public void testOutOfBoundsSource() {
    // When the passed source index is out of bounds wrt the sourceCount in the mapping, we should get an exception.
    PinotDistMapping mapping = new PinotDistMapping(5);
    assertThrows(IllegalArgumentException.class, () -> mapping.getTarget(-1));
    assertThrows(IllegalArgumentException.class, () -> mapping.getTarget(5));
    assertThrows(IllegalArgumentException.class, () -> mapping.set(-1, 2));
    assertThrows(IllegalArgumentException.class, () -> mapping.set(5, 2));
    assertThrows(IllegalArgumentException.class, () -> mapping.getMappedKeys(List.of(5)));
  }

  @Test
  public void testSet() {
    // Test setting a mapping value.
    PinotDistMapping mapping = new PinotDistMapping(5);
    mapping.set(0, 2);
    assertEquals(mapping.getTarget(0), 2);
    assertEquals(mapping.getTarget(1), -1);
    assertEquals(mapping.getTarget(2), -1);
    assertEquals(mapping.getTarget(3), -1);
    assertEquals(mapping.getTarget(4), -1);

    // Test setting multiple mapping values.
    mapping.set(1, 3);
    mapping.set(2, 4);
    assertEquals(mapping.getTarget(0), 2);
    assertEquals(mapping.getTarget(1), 3);
    assertEquals(mapping.getTarget(2), 4);
    assertEquals(mapping.getTarget(3), -1);
    assertEquals(mapping.getTarget(4), -1);

    // Test setting a mapping value to an invalid index.
    assertThrows(IllegalArgumentException.class, () -> mapping.set(-1, 2));
    assertThrows(IllegalArgumentException.class, () -> mapping.set(5, 2));
  }

  @Test
  public void testGetMappedKeys() {
    {
      // Test when all passed keys are mapped.
      PinotDistMapping mapping = new PinotDistMapping(5);
      mapping.set(0, 2);
      mapping.set(1, 3);
      mapping.set(2, 4);
      List<Integer> keys = List.of(0, 1, 2);
      List<Integer> expectedMappedKeys = List.of(2, 3, 4);
      assertEquals(mapping.getMappedKeys(keys), expectedMappedKeys);
    }
    {
      // Test when one of the keys is not mapped
      PinotDistMapping mapping = new PinotDistMapping(5);
      mapping.set(0, 2);
      mapping.set(1, 3);
      List<Integer> keys = List.of(0, 1, 2);
      List<Integer> expectedMappedKeys = List.of();
      assertEquals(mapping.getMappedKeys(keys), expectedMappedKeys);
    }
    {
      // Test getting mapped keys with an invalid key.
      PinotDistMapping mapping = new PinotDistMapping(5);
      mapping.set(0, 2);
      mapping.set(1, 3);
      List<Integer> keys = List.of(0, 1, 5);
      assertThrows(IllegalArgumentException.class, () -> mapping.getMappedKeys(keys));
    }
  }
}
