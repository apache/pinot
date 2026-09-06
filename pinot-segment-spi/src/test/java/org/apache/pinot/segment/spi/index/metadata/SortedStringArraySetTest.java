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
package org.apache.pinot.segment.spi.index.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// The sorted-array view stands in for the `TreeMap` key set the segment metadata used to hand out, so it has to
/// answer every navigation query the same way a `TreeSet` of the same elements does.
public class SortedStringArraySetTest {
  private static final String[] ELEMENTS = {"b", "d", "f", "h"};

  private static NavigableSet<String> set() {
    return new SortedStringArraySet(ELEMENTS.clone());
  }

  private static NavigableSet<String> reference() {
    return new TreeSet<>(List.of(ELEMENTS));
  }

  @Test
  public void testMatchesTreeSetForEveryProbe() {
    NavigableSet<String> set = set();
    NavigableSet<String> reference = reference();
    assertEquals(set, reference);
    assertEquals(set.hashCode(), reference.hashCode());
    assertEquals(set.size(), reference.size());
    assertEquals(new ArrayList<>(set), new ArrayList<>(reference));
    assertEquals(set.first(), reference.first());
    assertEquals(set.last(), reference.last());
    assertNull(set.comparator());
    for (String probe : List.of("a", "b", "c", "d", "e", "f", "g", "h", "i")) {
      assertEquals(set.contains(probe), reference.contains(probe), probe);
      assertEquals(set.floor(probe), reference.floor(probe), probe);
      assertEquals(set.ceiling(probe), reference.ceiling(probe), probe);
      assertEquals(set.lower(probe), reference.lower(probe), probe);
      assertEquals(set.higher(probe), reference.higher(probe), probe);
      assertEquals(new ArrayList<>(set.headSet(probe, true)), new ArrayList<>(reference.headSet(probe, true)), probe);
      assertEquals(new ArrayList<>(set.headSet(probe, false)), new ArrayList<>(reference.headSet(probe, false)), probe);
      assertEquals(new ArrayList<>(set.tailSet(probe, true)), new ArrayList<>(reference.tailSet(probe, true)), probe);
      assertEquals(new ArrayList<>(set.tailSet(probe, false)), new ArrayList<>(reference.tailSet(probe, false)), probe);
      assertEquals(new ArrayList<>(set.subSet(probe, true, "i", false)),
          new ArrayList<>(reference.subSet(probe, true, "i", false)), probe);
    }
    assertEquals(new ArrayList<>(set.descendingSet()), new ArrayList<>(reference.descendingSet()));
    List<String> descending = new ArrayList<>();
    set.descendingIterator().forEachRemaining(descending::add);
    assertEquals(descending, List.of("h", "f", "d", "b"));
  }

  @Test
  public void testEmptyAndExhaustedIteration() {
    NavigableSet<String> empty = new SortedStringArraySet(new String[0]);
    assertTrue(empty.isEmpty());
    assertEquals(empty.size(), 0);
    assertFalse(empty.iterator().hasNext());
    assertThrows(NoSuchElementException.class, empty::first);
    assertThrows(NoSuchElementException.class, empty::last);
    assertThrows(NoSuchElementException.class, () -> empty.iterator().next());
    assertThrows(NoSuchElementException.class, () -> empty.descendingIterator().next());
    assertFalse(set().subSet("c", true, "c", true).iterator().hasNext());
  }

  /// It is a view of the segment metadata's own array, so every mutator has to bounce rather than silently narrow
  /// the columns of a loaded segment.
  @Test
  public void testUnmodifiable() {
    NavigableSet<String> set = set();
    assertThrows(UnsupportedOperationException.class, () -> set.add("a"));
    assertThrows(UnsupportedOperationException.class, () -> set.remove("b"));
    assertThrows(UnsupportedOperationException.class, () -> set.removeAll(List.of("b")));
    assertThrows(UnsupportedOperationException.class, () -> set.retainAll(List.of("b")));
    assertThrows(UnsupportedOperationException.class, set::clear);
    assertThrows(UnsupportedOperationException.class, set::pollFirst);
    assertThrows(UnsupportedOperationException.class, set::pollLast);
    assertThrows(UnsupportedOperationException.class, () -> set.iterator().remove());
    assertThrows(IllegalArgumentException.class, () -> set.subSet("f", true, "b", true));
  }
}
