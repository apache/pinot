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
package org.apache.pinot.spi.data;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OpenStructKeyFlattenerTest {

  private final Map<String, Object> _flat = new LinkedHashMap<>();
  private final Set<String> _containers = new LinkedHashSet<>();
  /// How many times each key was emitted. `_flat` cannot show a key emitted twice -- the second write
  /// overwrites the first -- and a second emission is exactly what breaks a caller pairing values with a
  /// presence bitmap.
  private final Map<String, Integer> _emissions = new LinkedHashMap<>();

  private void flatten(Map<String, Object> document, int maxDepth) {
    _flat.clear();
    _containers.clear();
    _emissions.clear();
    OpenStructKeyFlattener.flatten(document, maxDepth, (path, value, container) -> {
      _flat.put(path, value);
      _emissions.merge(path, 1, Integer::sum);
      if (container) {
        _containers.add(path);
      }
    });
  }

  private static Map<String, Object> map(Object... keyValues) {
    // LinkedHashMap, not Map.of: several cases nest a null value, and iteration order is asserted.
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((String) keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  @Test
  public void testDefaultDepthLeavesDocumentUntouched() {
    Map<String, Object> nested = map("os", "ios");
    flatten(map("device", nested, "page", "home"), OpenStructKeyFlattener.NO_FLATTENING);

    // The nested map is passed through by reference, exactly as an un-flattened caller would see it.
    assertEquals(_flat, map("device", nested, "page", "home"));
    assertTrue(_containers.isEmpty(), "depth 1 must not classify anything as a container");
  }

  @Test
  public void testDepthTwoSplitsOneLevel() {
    flatten(map("device", map("os", "ios", "ver", 17), "page", "home"), 2);

    assertEquals(_flat.get("device.os"), "ios");
    assertEquals(_flat.get("device.ver"), 17);
    assertEquals(_flat.get("page"), "home");
    assertEquals(_containers, Set.of("device"));
  }

  @Test
  public void testContainerKeptAsJsonText() {
    flatten(map("device", map("os", "ios")), 2);

    // col['device'] still answers with the whole object, so splitting the leaves out is additive.
    assertEquals(_flat.get("device"), "{\"os\":\"ios\"}");
  }

  @Test
  public void testObjectAtDepthLimitIsNotDroppedOnlyUnaddressable() {
    flatten(map("a", map("b", map("c", 1))), 2);

    assertEquals(_flat.keySet(), Set.of("a", "a.b"));
    // 'a.b' is at the limit, so its own leaves get no keys -- but its content survives as JSON.
    assertEquals(_flat.get("a.b"), "{\"c\":1}");
    assertEquals(_containers, Set.of("a", "a.b"));
  }

  @Test
  public void testDepthThreeReachesGrandchild() {
    flatten(map("a", map("b", map("c", 1))), 3);

    assertEquals(_flat.get("a.b.c"), 1);
  }

  @Test
  public void testListsAreLeaves() {
    List<Object> tags = List.of("x", "y");
    flatten(map("tags", tags, "rows", List.of(map("k", 1))), 3);

    // A list has no key names to build a path from, so it stays one value whatever it holds.
    assertEquals(_flat.get("tags"), tags);
    assertEquals(_flat.keySet(), Set.of("tags", "rows"));
    assertTrue(_containers.isEmpty());
  }

  @Test
  public void testEmptyNestedObject() {
    flatten(map("device", map()), 3);

    assertEquals(_flat, map("device", "{}"));
    assertEquals(_containers, Set.of("device"));
  }

  @Test
  public void testNullValuesArePassedThroughForTheCallerToDrop() {
    flatten(map("a", null, "b", map("c", null)), 2);

    assertTrue(_flat.containsKey("a"));
    assertEquals(_flat.get("a"), null);
    assertEquals(_flat.get("b.c"), null);
  }

  @Test
  public void testKeyContainingSeparatorIsNotReinterpreted() {
    // A literal dot in a key is left alone; it is only ambiguous with a path, never rewritten.
    flatten(map("a.b", 1, "a", map("b", 2)), 2);

    assertEquals(_flat.get("a.b"), 1, "the document's own key is the value of 'a.b'");
    assertEquals(_emissions.get("a.b"), (Integer) 1, "a key is emitted at most once per document");
    assertEquals(_flat.get("a"), "{\"b\":2}", "the container is still emitted whole");
  }

  @Test
  public void testLiteralKeyWinsWhateverTheDocumentOrder() {
    // Same document, nested object first. Which name a value gets cannot depend on JSON key order.
    flatten(map("a", map("b", 2), "a.b", 1), 2);

    assertEquals(_flat.get("a.b"), 1);
    assertEquals(_emissions.get("a.b"), (Integer) 1);
  }

  @Test
  public void testLiteralKeyOfAnInnerObjectWins() {
    // The collision is two levels down: 'a' holds both a literal 'b.c' and a 'b' object holding 'c'.
    flatten(map("a", map("b.c", 1, "b", map("c", 2))), 3);

    assertEquals(_flat.get("a.b.c"), 1);
    assertEquals(_emissions.get("a.b.c"), (Integer) 1);
  }

  @Test
  public void testCollidingSynthesizedPathsKeepTheFirst() {
    // Neither 'a.b.c' is literal: one is 'a.b' + 'c', the other 'a' + 'b.c'. Nothing makes one of them
    // more the document's own than the other, so the first emission stands and the second is dropped.
    flatten(map("a.b", map("c", 1), "a", map("b.c", 2)), 3);

    assertEquals(_flat.get("a.b.c"), 1);
    assertEquals(_emissions.get("a.b.c"), (Integer) 1);
  }

  @Test
  public void testShadowedContainerStillHasItsLeavesSplitOut() {
    // 'a.b' names the literal 1, so the object under a -> b is not emitted as a container. Its own leaves
    // are separate keys and are not in collision with anything.
    flatten(map("a.b", 1, "a", map("b", map("c", 2))), 3);

    assertEquals(_flat.get("a.b"), 1);
    assertEquals(_flat.get("a.b.c"), 2);
    assertEquals(_containers, Set.of("a"), "'a.b' is the literal value, not the shadowed container");
  }

  @Test
  public void testNullKeySkipped() {
    Map<String, Object> document = new HashMap<>();
    document.put(null, "x");
    document.put("ok", 1);
    flatten(document, 2);

    assertEquals(_flat, map("ok", 1));
  }
}
