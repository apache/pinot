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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MutableOpenStructIndexTest {
  private PinotDataBufferMemoryManager _memMgr;

  @BeforeMethod
  public void setUp() {
    _memMgr = new DirectMemoryManager(MutableOpenStructIndexTest.class.getName());
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    _memMgr.close();
  }

  private static ComplexFieldSpec openStructSpec() {
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
  }

  @Test
  public void testAddAndGetKeys()
      throws IOException {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(
        "metrics", "testTable_REALTIME", openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 1000)) {

      idx.index(0, Map.of("clicks", 42L, "impressions", 100L));
      idx.index(1, Map.of("clicks", 7L, "revenue", "1.5"));

      Set<String> keys = idx.getKeys();
      assertTrue(keys.contains("clicks"), "Expected 'clicks' in keys");
      assertTrue(keys.contains("impressions"), "Expected 'impressions' in keys");
      assertTrue(keys.contains("revenue"), "Expected 'revenue' in keys");
      assertEquals(keys.size(), 3);
    }
  }

  @Test
  public void testIndexNullIsNoop()
      throws IOException {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(
        "metrics", "testTable_REALTIME", openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 1000)) {

      idx.index(0, null);

      assertTrue(idx.getKeys().isEmpty(), "Expected no keys after indexing null");
      assertNull(idx.getKeyColumn("clicks"));
    }
  }

  @Test
  public void testFillRateTracking()
      throws IOException {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(
        "metrics", "testTable_REALTIME", openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 1000)) {

      for (int docId = 0; docId < 10; docId++) {
        if (docId < 7) {
          idx.index(docId, Map.of("clicks", (long) (docId + 1)));
        } else {
          // docs 7,8,9 have no "clicks" key
          idx.index(docId, Map.of("impressions", 100L));
        }
      }

      MutableKeyColumn clicksCol = idx.getKeyColumn("clicks");
      assertNotNull(clicksCol, "Expected 'clicks' column to exist");
      assertEquals(clicksCol.getNumNonNullDocs(), 7,
          "Expected 7 non-null docs for 'clicks'");
    }
  }

  @Test
  public void testTypeInferenceFromValue()
      throws IOException {
    // No childFieldSpecs — type inference from rawValue
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME", spec,
        OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
      idx.index(0, java.util.Map.of("clicks", 5L));
      assertEquals(idx.getKeyColumn("clicks").getStoredType(), DataType.LONG);
      idx.index(1, java.util.Map.of("country", "US"));
      assertEquals(idx.getKeyColumn("country").getStoredType(), DataType.STRING);
    }
  }

  @Test
  public void testImplementsOpenStructIndexReader() throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME", openStructSpec(),
        OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
      assertTrue(idx instanceof org.apache.pinot.segment.spi.index.reader.OpenStructIndexReader);
    }
  }

  @Test
  public void testGetIndexesReturnsForwardIndexForMaterializedKey() throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME", openStructSpec(),
        OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      Map<org.apache.pinot.segment.spi.index.IndexType, org.apache.pinot.segment.spi.index.IndexReader> indexes =
          idx.getIndexes("clicks");
      assertNotNull(indexes.get(org.apache.pinot.segment.spi.index.StandardIndexes.forward()));
    }
  }

  @Test
  public void testGetIndexesUnknownKeyReturnsEmpty() throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME", openStructSpec(),
        OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
      assertTrue(idx.getIndexes("missing").isEmpty());
    }
  }

  @Test
  public void testGetColumnMetadataReturnsKeyMetadata() throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME", openStructSpec(),
        OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      assertNotNull(idx.getColumnMetadata("clicks"));
      assertEquals(idx.getColumnMetadata("clicks").getColumnName(), "clicks");
      assertNull(idx.getColumnMetadata("absent"));
    }
  }

  /// Both failure meters are keyed on the OPEN_STRUCT column, never on the per-key materialized
  /// name. These fire on malformed input, so an id-like key would otherwise mint one meter per id,
  /// and meters are never removed from the registry.
  @Test
  public void testFailureMetersAreKeyedOnColumnNotKey()
      throws IOException {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME",
          openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
        // Unmappable value on a fresh key: falls back to STRING and meters an inference failure.
        idx.index(0, Map.of("req-42", Map.of("a", 1)));
        // Unmappable value on a key already typed LONG: dropped by coercion, metered there only.
        idx.index(1, Map.of("clicks", 5L));
        idx.index(2, Map.of("clicks", Map.of("a", 1)));
      }

      verify(metrics).addMeteredTableValue("testTable_REALTIME", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES, 1L);
      verify(metrics).addMeteredTableValue("testTable_REALTIME", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES, 1L);
      verify(metrics, never()).addMeteredTableValue(anyString(), eq("metrics$req-42"), any(), anyLong());
      verify(metrics, never()).addMeteredTableValue(anyString(), eq("metrics$clicks"), any(), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// A later unmappable value on a key whose type fell back to STRING is stored as its serialized
  /// form, so it is counted every time — not just on the first sighting that established the type.
  /// The accumulated count is emitted once at close.
  @Test
  public void testInferenceFailuresAccumulatePerValueAndMeterOnceOnClose()
      throws IOException {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME",
          openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
        for (int docId = 0; docId < 3; docId++) {
          idx.index(docId, Map.of("payload", Map.of("a", docId)));
        }

        verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
            eq(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES), anyLong());
      }

      verify(metrics, times(1)).addMeteredTableValue("testTable_REALTIME", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES, 3L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testIgnoredKeyNeverAllocatesColumn()
      throws Exception {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"ignoredKeys\": [\"debug\"]}", OpenStructIndexConfig.class);
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(
        "metrics", "testTable_REALTIME", openStructSpec(), config, _memMgr, 1000)) {
      idx.index(0, Map.of("debug", "noise", "clicks", 5L));

      assertNull(idx.getKeyColumn("debug"), "Ignored key must never allocate a column");
      assertTrue(idx.getKeys().contains("clicks"), "Non-ignored key must still be indexed");
      assertEquals(idx.getKeys().size(), 1);
      assertEquals(idx.getMapValue(0), Map.of("clicks", 5L));
    }
  }

  @Test
  public void testIgnoredKeyMeteredOnceOnClose()
      throws IOException {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"ignoredKeys\": [\"debug\"]}", OpenStructIndexConfig.class);
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex(
          "metrics", "testTable_REALTIME", openStructSpec(), config, _memMgr, 1000)) {
        idx.index(0, Map.of("debug", "a"));
        idx.index(1, Map.of("debug", "b", "clicks", 1L));

        verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
            eq(ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS), anyLong());
      }

      verify(metrics, times(1)).addMeteredTableValue("testTable_REALTIME", "metrics",
          ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS, 2L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testIgnoredKeyWithNullValueNotMetered()
      throws IOException {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"ignoredKeys\": [\"debug\"]}", OpenStructIndexConfig.class);
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      Map<String, Object> row = new HashMap<>();
      row.put("debug", null);
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex(
          "metrics", "testTable_REALTIME", openStructSpec(), config, _memMgr, 1000)) {
        idx.index(0, row);
      }

      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testCoercionFailuresMeteredOnceOnClose()
      throws IOException {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME",
          openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
        // Establish "clicks" as LONG, then feed three unmappable values that fail coercion.
        idx.index(0, Map.of("clicks", 5L));
        idx.index(1, Map.of("clicks", Map.of("a", 1)));
        idx.index(2, Map.of("clicks", Map.of("a", 2)));
        idx.index(3, Map.of("clicks", Map.of("a", 3)));

        verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
            eq(ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES), anyLong());
      }

      verify(metrics, times(1)).addMeteredTableValue("testTable_REALTIME", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES, 3L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testNoFailureMetersEmittedWhenNoFailures()
      throws IOException {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME",
          openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
        idx.index(0, Map.of("clicks", 5L, "country", "US"));
        idx.index(1, Map.of("clicks", 7L));
      }

      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES), anyLong());
      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES), anyLong());
      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testInferenceCheckSkippedForNonStringKeys()
      throws IOException {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME",
          openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
        // "clicks" establishes as LONG. Unmappable values on it are a coercion concern only —
        // inference must never run for a key whose established type is not STRING.
        idx.index(0, Map.of("clicks", 5L));
        idx.index(1, Map.of("clicks", Map.of("a", 1)));
        idx.index(2, Map.of("clicks", Map.of("a", 2)));
      }

      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES), anyLong());
      verify(metrics, times(1)).addMeteredTableValue("testTable_REALTIME", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES, 2L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// A STRING-established key does run the per-row inference check, so this pins the other side of
  /// that branch: a later *inferable* value must not be counted. Without the `inferDataType == null`
  /// guard inside meterIfUninferable, every row on such a key would meter a failure.
  @Test
  public void testInferenceCheckOnStringKeyDoesNotMeterInferableValues()
      throws IOException {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", "testTable_REALTIME",
          openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
        // "country" has no child spec and infers as STRING, so needsInferenceCheck() is true and
        // meterIfUninferable runs on every later row — but both values infer cleanly.
        idx.index(0, Map.of("country", "US"));
        idx.index(1, Map.of("country", "CA"));
        idx.index(2, Map.of("country", "MX"));
      }

      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES), anyLong());
      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }
}
