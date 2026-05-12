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

import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CompressionStatsSummaryTest {

  @Test
  public void testGetters() {
    CompressionStatsSummary summary = new CompressionStatsSummary(100000, 40000, 2.5, 8, 10, true);
    assertEquals(summary.getRawForwardIndexSizePerReplicaInBytes(), 100000);
    assertEquals(summary.getCompressedForwardIndexSizePerReplicaInBytes(), 40000);
    assertEquals(summary.getCompressionRatio(), 2.5, 0.001);
    assertEquals(summary.getSegmentsWithStats(), 8);
    assertEquals(summary.getTotalSegments(), 10);
    assertTrue(summary.isPartialCoverage());
  }

  @Test
  public void testFullCoverage() {
    CompressionStatsSummary summary = new CompressionStatsSummary(50000, 25000, 2.0, 5, 5, false);
    assertEquals(summary.getSegmentsWithStats(), 5);
    assertEquals(summary.getTotalSegments(), 5);
    assertFalse(summary.isPartialCoverage());
  }

  @Test
  public void testJsonRoundTrip()
      throws Exception {
    CompressionStatsSummary original = new CompressionStatsSummary(200000, 80000, 2.5, 3, 4, true);
    String json = JsonUtils.objectToString(original);

    assertTrue(json.contains("rawForwardIndexSizePerReplicaInBytes"));
    assertTrue(json.contains("compressedForwardIndexSizePerReplicaInBytes"));
    assertTrue(json.contains("compressionRatio"));
    assertTrue(json.contains("segmentsWithStats"));
    assertTrue(json.contains("totalSegments"));
    assertTrue(json.contains("isPartialCoverage"));

    CompressionStatsSummary deserialized = JsonUtils.stringToObject(json, CompressionStatsSummary.class);
    assertEquals(deserialized.getRawForwardIndexSizePerReplicaInBytes(), 200000);
    assertEquals(deserialized.getCompressedForwardIndexSizePerReplicaInBytes(), 80000);
    assertEquals(deserialized.getCompressionRatio(), 2.5, 0.001);
    assertEquals(deserialized.getSegmentsWithStats(), 3);
    assertEquals(deserialized.getTotalSegments(), 4);
    assertTrue(deserialized.isPartialCoverage());
  }

  @Test
  public void testJsonIgnoresUnknownFields()
      throws Exception {
    String json = "{\"rawForwardIndexSizePerReplicaInBytes\":1000,\"compressedForwardIndexSizePerReplicaInBytes\":500,"
        + "\"compressionRatio\":2.0,\"segmentsWithStats\":1,\"totalSegments\":1,\"isPartialCoverage\":false,"
        + "\"unknownFutureField\":\"ignored\"}";
    CompressionStatsSummary summary = JsonUtils.stringToObject(json, CompressionStatsSummary.class);
    assertEquals(summary.getRawForwardIndexSizePerReplicaInBytes(), 1000);
    assertFalse(summary.isPartialCoverage());
  }
}
