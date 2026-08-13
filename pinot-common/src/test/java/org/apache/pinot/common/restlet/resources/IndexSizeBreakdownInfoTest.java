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
package org.apache.pinot.common.restlet.resources;

import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Pins the wire contract of `indexSizeBreakdown` entries. The field names are part of a public REST response, so a
/// rename would break clients silently.
public class IndexSizeBreakdownInfoTest {

  @Test
  public void testJsonRoundTrip()
      throws Exception {
    IndexSizeBreakdownInfo original = new IndexSizeBreakdownInfo(32000000000L, 17);

    String json = JsonUtils.objectToString(original);
    assertTrue(json.contains("\"sizePerReplicaInBytes\""), "Unexpected payload: " + json);
    assertTrue(json.contains("\"segmentsWithStats\""), "Unexpected payload: " + json);

    IndexSizeBreakdownInfo deserialized = JsonUtils.stringToObject(json, IndexSizeBreakdownInfo.class);
    assertEquals(deserialized, original);
    assertEquals(deserialized.getSizePerReplicaInBytes(), 32000000000L);
    assertEquals(deserialized.getSegmentsWithStats(), 17);
  }

  /// Sizes exceed the int range on real tables, so the field has to stay a long.
  @Test
  public void testHandlesSizesBeyondIntRange()
      throws Exception {
    long size = 32_000_000_000L;
    assertTrue(size > Integer.MAX_VALUE, "Test value should exceed the int range");
    IndexSizeBreakdownInfo deserialized =
        JsonUtils.stringToObject(JsonUtils.objectToString(new IndexSizeBreakdownInfo(size, 1)),
            IndexSizeBreakdownInfo.class);
    assertEquals(deserialized.getSizePerReplicaInBytes(), size);
  }

  /// A mixed-version controller may see fields it does not know; unknown properties must not fail deserialization.
  @Test
  public void testToleratesUnknownProperties()
      throws Exception {
    String json = "{\"sizePerReplicaInBytes\":123,\"segmentsWithStats\":4,\"somethingNewer\":\"x\"}";
    IndexSizeBreakdownInfo deserialized = JsonUtils.stringToObject(json, IndexSizeBreakdownInfo.class);
    assertEquals(deserialized.getSizePerReplicaInBytes(), 123L);
    assertEquals(deserialized.getSegmentsWithStats(), 4);
  }

  /// Absent fields default rather than throwing, so an older server that omits one still deserializes.
  @Test
  public void testMissingFieldsDefault()
      throws Exception {
    IndexSizeBreakdownInfo deserialized =
        JsonUtils.stringToObject("{\"sizePerReplicaInBytes\":9}", IndexSizeBreakdownInfo.class);
    assertEquals(deserialized.getSizePerReplicaInBytes(), 9L);
    assertEquals(deserialized.getSegmentsWithStats(), 0);
  }

  /// The breakdown is carried as a map keyed by IndexType#getId(), so it has to survive round-tripping in that shape.
  @Test
  public void testRoundTripsInsideAMap()
      throws Exception {
    Map<String, IndexSizeBreakdownInfo> breakdown =
        Map.of("forward_index", new IndexSizeBreakdownInfo(32000000000L, 17), "bloom_filter",
            new IndexSizeBreakdownInfo(2000000000L, 17));

    String json = JsonUtils.objectToString(breakdown);
    Map<String, IndexSizeBreakdownInfo> deserialized =
        JsonUtils.stringToObject(json, new com.fasterxml.jackson.core.type.TypeReference<>() {
        });
    assertEquals(deserialized, breakdown);
  }
}
