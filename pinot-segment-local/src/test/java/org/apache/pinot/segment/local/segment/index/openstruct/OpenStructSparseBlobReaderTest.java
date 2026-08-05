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
