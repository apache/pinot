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
package org.apache.pinot.common.utils.helix;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Checks that [ImmutableSortedArrayMap] behaves like a [TreeMap] for reads and rejects every mutation.
public class ImmutableSortedArrayMapTest {

  private static Map<String, String> unsortedSample() {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("Server_c_8098", "ONLINE");
    map.put("Server_a_8098", "CONSUMING");
    map.put("Server_b_8098", "OFFLINE");
    map.put("Server_d_8098", null);
    return map;
  }

  @Test
  public void testReadsMatchTreeMap() {
    Map<String, String> source = unsortedSample();
    TreeMap<String, String> expected = new TreeMap<>(source);
    ImmutableSortedArrayMap<String> map = ImmutableSortedArrayMap.copyOf(source);

    assertEquals(map.size(), expected.size());
    assertFalse(map.isEmpty());
    assertNull(map.comparator());
    assertEquals(new ArrayList<>(map.keySet()), new ArrayList<>(expected.keySet()));
    assertEquals(new ArrayList<>(map.values()), new ArrayList<>(expected.values()));
    assertEquals(new ArrayList<>(map.entrySet()), new ArrayList<>(expected.entrySet()));
    assertEquals(map.toString(), expected.toString());
    assertEquals(map.firstKey(), expected.firstKey());
    assertEquals(map.lastKey(), expected.lastKey());
    for (String key : List.of("Server_a_8098", "Server_d_8098", "Server_e_8098", "")) {
      assertEquals(map.get(key), expected.get(key), key);
      assertEquals(map.containsKey(key), expected.containsKey(key), key);
      assertEquals(map.getOrDefault(key, "x"), expected.getOrDefault(key, "x"), key);
    }
    // Lookups with a non-interned equal key and with keys of another type
    assertEquals(map.get(new String("Server_b_8098")), "OFFLINE");
    assertNull(map.get(42));
    assertFalse(map.containsKey(null));
    assertNull(map.get(null));
    assertTrue(map.containsValue("ONLINE"));
    assertTrue(map.containsValue(null));
    assertFalse(map.containsValue("ERROR"));
    assertFalse(map.containsValue(42));
    assertTrue(map.entrySet().contains(new AbstractMap.SimpleEntry<>("Server_a_8098", "CONSUMING")));
    assertFalse(map.entrySet().contains(new AbstractMap.SimpleEntry<>("Server_a_8098", "ONLINE")));
    assertTrue(map.values().contains(null));
  }

  @Test
  public void testEqualsAndHashCodeParity() {
    Map<String, String> source = unsortedSample();
    TreeMap<String, String> treeMap = new TreeMap<>(source);
    HashMap<String, String> hashMap = new HashMap<>(source);
    ImmutableSortedArrayMap<String> map = ImmutableSortedArrayMap.copyOf(source);

    assertEquals(map, treeMap);
    assertEquals(treeMap, map);
    assertEquals(map, hashMap);
    assertEquals(hashMap, map);
    assertEquals(map.hashCode(), treeMap.hashCode());
    assertEquals(map.hashCode(), hashMap.hashCode());
    assertEquals(map, ImmutableSortedArrayMap.copyOf(hashMap));

    Set<String> hashSet = new HashSet<>(source.keySet());
    assertEquals(map.keySet(), hashSet);
    assertEquals(hashSet, map.keySet());
    assertEquals(map.keySet(), treeMap.keySet());
    assertEquals(map.keySet().hashCode(), hashSet.hashCode());
    assertEquals(map.entrySet(), treeMap.entrySet());
    assertEquals(treeMap.entrySet(), map.entrySet());
    assertEquals(map.entrySet().hashCode(), treeMap.entrySet().hashCode());

    // A HashMap keyed by key sets works across implementations, as in ReplicaGroupInstanceSelector
    Map<Set<String>, String> byKeySet = new HashMap<>();
    byKeySet.put(treeMap.keySet(), "value");
    assertEquals(byKeySet.get(map.keySet()), "value");
    byKeySet.clear();
    byKeySet.put(map.keySet(), "value");
    assertEquals(byKeySet.get(hashSet), "value");

    Map<String, String> different = new TreeMap<>(source);
    different.put("Server_a_8098", "ONLINE");
    assertFalse(map.equals(different));
    assertFalse(different.equals(map));
    assertFalse(map.equals(ImmutableSortedArrayMap.copyOf(different)));
  }

  @Test
  public void testSortedViews() {
    TreeMap<String, String> expected = new TreeMap<>(unsortedSample());
    ImmutableSortedArrayMap<String> map = ImmutableSortedArrayMap.copyOf(expected);
    for (String from : List.of("", "Server_a_8098", "Server_b", "Server_c_8098", "Z")) {
      assertEquals(map.tailMap(from), expected.tailMap(from), from);
      assertEquals(map.headMap(from), expected.headMap(from), from);
      for (String to : List.of("Server_b", "Server_c_8098", "Z")) {
        if (from.compareTo(to) <= 0) {
          SortedMap<String, String> subMap = map.subMap(from, to);
          assertEquals(subMap, expected.subMap(from, to), from + "-" + to);
          assertEquals(new ArrayList<>(subMap.keySet()), new ArrayList<>(expected.subMap(from, to).keySet()));
        }
      }
    }
    assertThrows(IllegalArgumentException.class, () -> map.subMap("b", "a"));
    assertThrows(NullPointerException.class, () -> map.headMap(null));
    assertThrows(NoSuchElementException.class, () -> ImmutableSortedArrayMap.empty().firstKey());
    assertThrows(NoSuchElementException.class, () -> ImmutableSortedArrayMap.empty().lastKey());
  }

  @Test
  public void testDuplicateKeysLastWins() {
    String[] keys = {"b", "a", "b", "c", "a"};
    Object[] values = {"1", "2", "3", "4", "5"};
    ImmutableSortedArrayMap<String> map = ImmutableSortedArrayMap.sortAndWrap(keys, values, keys.length);
    assertEquals(map, Map.of("a", "5", "b", "3", "c", "4"));
    assertEquals(new ArrayList<>(map.keySet()), List.of("a", "b", "c"));

    // Sorted input with a trailing capacity is trimmed
    String[] sortedKeys = {"a", "b", null, null};
    Object[] sortedValues = {"1", "2", null, null};
    assertEquals(ImmutableSortedArrayMap.sortAndWrap(sortedKeys, sortedValues, 2), Map.of("a", "1", "b", "2"));
    assertSame(ImmutableSortedArrayMap.sortAndWrap(new String[0], new Object[0], 0), ImmutableSortedArrayMap.empty());
  }

  @Test
  public void testMutatorsThrow() {
    ImmutableSortedArrayMap<String> map = ImmutableSortedArrayMap.copyOf(unsortedSample());
    assertThrows(UnsupportedOperationException.class, () -> map.put("x", "y"));
    assertThrows(UnsupportedOperationException.class, () -> map.remove("Server_a_8098"));
    assertThrows(UnsupportedOperationException.class, () -> map.remove("Server_a_8098", "CONSUMING"));
    assertThrows(UnsupportedOperationException.class, () -> map.putAll(Map.of("x", "y")));
    assertThrows(UnsupportedOperationException.class, map::clear);
    assertThrows(UnsupportedOperationException.class, () -> map.putIfAbsent("x", "y"));
    assertThrows(UnsupportedOperationException.class, () -> map.replace("Server_a_8098", "y"));
    assertThrows(UnsupportedOperationException.class, () -> map.replace("Server_a_8098", "CONSUMING", "y"));
    assertThrows(UnsupportedOperationException.class, () -> map.replaceAll((k, v) -> v));
    assertThrows(UnsupportedOperationException.class, () -> map.computeIfAbsent("x", k -> "y"));
    assertThrows(UnsupportedOperationException.class, () -> map.computeIfPresent("Server_a_8098", (k, v) -> v));
    assertThrows(UnsupportedOperationException.class, () -> map.compute("x", (k, v) -> v));
    assertThrows(UnsupportedOperationException.class, () -> map.merge("x", "y", (a, b) -> a));
    assertThrows(UnsupportedOperationException.class, () -> map.keySet().remove("Server_a_8098"));
    assertThrows(UnsupportedOperationException.class, () -> map.keySet().add("x"));
    assertThrows(UnsupportedOperationException.class, () -> map.keySet().clear());
    assertThrows(UnsupportedOperationException.class, () -> map.keySet().retainAll(Set.of()));
    assertThrows(UnsupportedOperationException.class, () -> map.values().remove("ONLINE"));
    assertThrows(UnsupportedOperationException.class, () -> map.values().clear());
    assertThrows(UnsupportedOperationException.class, () -> map.entrySet().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> map.entrySet().remove(new AbstractMap.SimpleEntry<>("Server_a_8098", "CONSUMING")));
    assertThrows(UnsupportedOperationException.class, () -> map.entrySet().iterator().next().setValue("x"));
    Iterator<String> iterator = map.keySet().iterator();
    iterator.next();
    assertThrows(UnsupportedOperationException.class, iterator::remove);
    assertEquals(map, new TreeMap<>(unsortedSample()));
  }

  @Test
  public void testTreeMapCopyIsLinear() {
    // TreeMap takes the buildFromSorted path for a SortedMap with the same (natural) comparator, so the copy made by
    // HelixProperty does not compare keys. This test checks the result, the comparator() == null check enables the
    // fast path.
    ImmutableSortedArrayMap<String> map = ImmutableSortedArrayMap.copyOf(unsortedSample());
    TreeMap<String, String> copy = new TreeMap<>();
    copy.putAll(map);
    assertEquals(copy, map);
    assertEquals(new ArrayList<>(copy.keySet()), new ArrayList<>(map.keySet()));
  }
}
