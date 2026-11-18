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
package org.apache.pinot.segment.local.segment.index.openstruct;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.spi.data.OpenStructKeyFlattener;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class OpenStructSparseBlobReaderTest {
  private static final String[] BLOBS = {
      "{\"region\":\"us\",\"latencyMs\":42}",
      null,
      "{\"region\":\"eu\"}",
      "{\"latencyMs\":7}",
  };

  private static OpenStructSparseBlobReader reader() {
    return new OpenStructSparseBlobReader(new FakeStringForwardIndex(BLOBS),
        FakeStringForwardIndex.nullVector(BLOBS), BLOBS.length);
  }

  @Test
  public void testReturnsValueNodePerDocAndKey() {
    OpenStructSparseBlobReader blob = reader();
    JsonNode node = blob.getValue(0, "region", null);
    assertEquals(node.asText(), "us");
    assertEquals(blob.getValue(0, "latencyMs", null).asInt(), 42);
    assertNull(blob.getValue(2, "latencyMs", null));   // key missing in doc
    assertNull(blob.getValue(1, "region", null));      // empty doc
  }

  @Test
  public void testComputesPresenceBitmapPerKey() {
    OpenStructSparseBlobReader blob = reader();
    ImmutableRoaringBitmap present = blob.computePresence("region");
    assertTrue(present.contains(0));
    assertFalse(present.contains(1));
    assertTrue(present.contains(2));
    assertFalse(present.contains(3));
  }

  @Test
  public void testThrowsOnMalformedJson() {
    String[] blobs = {"{not json"};
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(new FakeStringForwardIndex(blobs),
        FakeStringForwardIndex.nullVector(blobs), blobs.length);
    assertThrows(IllegalStateException.class, () -> blob.getValue(0, "k", null));
  }

  @Test
  public void testWorksWithoutNullVector() {
    String[] blobs = {"{\"region\":\"us\"}", ""};
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(new FakeStringForwardIndex(blobs), null,
        blobs.length);
    assertEquals(blob.getValue(0, "region", null).asText(), "us");
    assertNull(blob.getValue(1, "region", null));
  }

  private static OpenStructSparseBlobReader readerOf(String... blobs) {
    return new OpenStructSparseBlobReader(new FakeStringForwardIndex(blobs), FakeStringForwardIndex.nullVector(blobs),
        blobs.length);
  }

  @Test
  public void testDottedKeyWalksNestedObjects() {
    OpenStructSparseBlobReader blob = readerOf("{\"o\":{\"z\":2,\"w\":{\"x\":\"deep\"}},\"n\":5}");
    assertEquals(blob.getValue(0, "o.z", null).asInt(), 2);
    assertEquals(blob.getValue(0, "o.w.x", null).asText(), "deep");
    // A path that ends on an object returns the object, like a top-level key does.
    assertEquals(blob.getValue(0, "o.w", null).toString(), "{\"x\":\"deep\"}");
    assertEquals(blob.getValue(0, "n", null).asInt(), 5);
  }

  @Test
  public void testLiteralDottedKeyWinsOverPath() {
    // Same rule as OpenStructKeyFlattener: a key the document carries is never shadowed by a path.
    assertEquals(readerOf("{\"a.b\":100,\"a\":{\"b\":200}}").getValue(0, "a.b", null).asInt(), 100);
    // The literal wins at an inner level too.
    assertEquals(readerOf("{\"a\":{\"b.c\":1,\"b\":{\"c\":2}}}").getValue(0, "a.b.c", null).asInt(), 1);
    // Two synthesized paths: the first in document order wins, as in the flattener.
    assertEquals(readerOf("{\"a.b\":{\"c\":1},\"a\":{\"b.c\":2}}").getValue(0, "a.b.c", null).asInt(), 1);
  }

  @Test
  public void testDottedKeyThatCannotBeWalkedIsAbsent() {
    OpenStructSparseBlobReader blob = readerOf("{\"tags\":[\"x\",\"y\"],\"s\":\"text\",\"o\":{\"z\":2},\"nul\":null}");
    assertNull(blob.getValue(0, "tags.0", null));    // arrays are not indexed into
    assertNull(blob.getValue(0, "s.len", null));     // scalar intermediate
    assertNull(blob.getValue(0, "o.missing", null)); // missing leaf
    assertNull(blob.getValue(0, "nope.z", null));    // missing intermediate
    assertNull(blob.getValue(0, "nul.z", null));     // null intermediate
  }

  /// Oracle: for every document, each key `OpenStructKeyFlattener` emits (unbounded depth) resolves to the value the
  /// flattener emits for it, and probe paths it does not emit resolve to nothing. Covers literal-wins at every
  /// level, collisions between synthesized paths, empty segments, arrays, nulls and scalars.
  @Test
  public void testResolveMatchesFlattener() throws Exception {
    String[] docs = {
        "{\"o\":{\"z\":2,\"w\":{\"x\":\"deep\"}},\"n\":5}",
        "{\"a.b\":100,\"a\":{\"b\":200}}",
        "{\"a\":{\"b.c\":1,\"b\":{\"c\":2}}}",
        "{\"a.b\":{\"c\":1},\"a\":{\"b.c\":2}}",
        "{\"o\":{\"\":1},\"\":{\"b\":2},\"e\":{\"\":{\"x\":3}}}",
        "{\"tags\":[\"x\",{\"y\":1}],\"s\":\"text\",\"nul\":null,\"o\":{\"z\":null,\"q\":{}}}",
        "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":1}}}},\"a.b.c\":{\"d\":9}}",
    };
    String[] probes = {"o.missing", "tags.0", "tags.1.y", "s.len", "nul.z", "nope.z", "o.q.r", "a.x", "..", "x."};
    int checked = 0;
    for (String json : docs) {
      JsonNode blob = JsonUtils.stringToJsonNode(json);
      @SuppressWarnings("unchecked")
      Map<String, Object> document = JsonUtils.stringToObject(json, Map.class);
      Map<String, Object> emitted = new LinkedHashMap<>();
      OpenStructKeyFlattener.flatten(document, Integer.MAX_VALUE, (path, value, container) -> emitted.put(path,
          value));
      for (Map.Entry<String, Object> entry : emitted.entrySet()) {
        JsonNode resolved = OpenStructSparseBlobReader.resolve(blob, entry.getKey());
        assertNotNull(resolved, json + " -> " + entry.getKey());
        Object value = entry.getValue();
        JsonNode expected = value instanceof String text && resolved.isContainerNode()
            ? JsonUtils.stringToJsonNode(text) : JsonUtils.objectToJsonNode(value);
        assertEquals(resolved, expected, json + " -> " + entry.getKey());
        checked++;
      }
      for (String probe : probes) {
        if (!emitted.containsKey(probe)) {
          assertNull(OpenStructSparseBlobReader.resolve(blob, probe), json + " -> " + probe);
        }
      }
    }
    assertTrue(checked >= 30, "only " + checked + " flattened keys checked");
  }

  @Test
  public void testPresenceFollowsDottedPath() {
    OpenStructSparseBlobReader blob = readerOf("{\"o\":{\"z\":1}}", "{\"o\":{}}", "{\"o\":{\"z\":null}}", "{}");
    ImmutableRoaringBitmap present = blob.computePresence("o.z");
    assertTrue(present.contains(0));
    assertFalse(present.contains(1));
    assertFalse(present.contains(2)); // explicit JSON null is absent, as for top-level keys
    assertFalse(present.contains(3));
  }

  @Test
  public void testExplicitJsonNullIsPresentButNullNode() {
    String[] blobs = {"{\"region\":null}"};
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(new FakeStringForwardIndex(blobs),
        FakeStringForwardIndex.nullVector(blobs), blobs.length);
    JsonNode node = blob.getValue(0, "region", null);
    assertNotNull(node);
    assertTrue(node.isNull());
    // computePresence treats explicit JSON null as absent — matching the virtual forward
    // reader's default-folding semantics.
    ImmutableRoaringBitmap presence = blob.computePresence("region");
    assertFalse(presence.contains(0));
  }
}
