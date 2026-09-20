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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// A value nested inside an OPEN_STRUCT document has no key of its own, so nothing can materialize
/// or filter it. With `maxNestedKeyDepth` raised, the path becomes the key: `device.os` is an
/// ordinary key that gets its own column, while `device` keeps answering with the whole object.
public class OpenStructNestedKeyTest {

  private static final String COLUMN = "props";
  private static final int NUM_DOCS = 10;

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("OpenStructNestedKeyTest").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  private static ComplexFieldSpec spec(Map<String, FieldSpec> children) {
    return new ComplexFieldSpec(COLUMN, DataType.OPEN_STRUCT, true, children);
  }

  private static OpenStructIndexConfig config(int maxNestedKeyDepth, Set<String> denseKeys) {
    return new OpenStructIndexConfig(false, null, -1, denseKeys, 0.5, null, null, null, null, maxNestedKeyDepth);
  }

  private OpenStructColumnSplitter splitter(OpenStructIndexConfig config, Map<String, FieldSpec> children) {
    return new OpenStructColumnSplitter(_tempDir, COLUMN, "testTable_OFFLINE", spec(children), config);
  }

  /// `{"device": {"os": ..., "ver": ...}, "page": ...}` on every doc.
  private static Map<String, Object> doc(int docId) {
    Map<String, Object> device = new HashMap<>();
    device.put("os", docId % 2 == 0 ? "ios" : "android");
    device.put("ver", (long) docId);
    Map<String, Object> document = new HashMap<>();
    document.put("device", device);
    document.put("page", "home");
    return document;
  }

  private static String materialized(String key) {
    return OpenStructNaming.materializedColumnName(COLUMN, key);
  }

  private static Set<String> sparseKeys(OpenStructColumnSplitter splitter)
      throws Exception {
    PropertiesConfiguration parentProps = splitter.getMaterializedColumnMetadata().get(COLUMN);
    assertNotNull(parentProps);
    String manifest = parentProps.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(COLUMN, V1Constants.MetadataKeys.Column.SPARSE_KEYS));
    if (manifest == null) {
      return Set.of();
    }
    return new HashSet<>(JsonUtils.stringToObject(manifest, new TypeReference<List<String>>() { }));
  }

  @Test
  public void testDefaultDepthLeavesNestedValuesUnaddressable()
      throws Exception {
    OpenStructColumnSplitter s = splitter(config(1, null), Map.of());
    for (int d = 0; d < NUM_DOCS; d++) {
      s.add(doc(d), d);
    }
    s.seal();

    // The pre-existing shape: one key for the whole object, and no way to name what is inside it.
    assertTrue(s.getResolvedDenseKeys().contains("device"));
    assertFalse(s.getResolvedDenseKeys().contains("device.os"));
    assertFalse(s.getMaterializedColumnMetadata().containsKey(materialized("device.os")));
  }

  @Test
  public void testNestedLeafBecomesItsOwnDenseColumn()
      throws Exception {
    OpenStructColumnSplitter s = splitter(config(2, null), Map.of());
    for (int d = 0; d < NUM_DOCS; d++) {
      s.add(doc(d), d);
    }
    s.seal();

    assertTrue(s.getResolvedDenseKeys().containsAll(Set.of("device.os", "device.ver", "page")));

    // The leaf keeps the type it would have had as a top-level key, not the container's STRING.
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(materialized("device.ver"));
    assertNotNull(props, "nested leaf must be materialized as its own column");
    ColumnMetadataImpl metadata =
        ColumnMetadataImpl.fromPropertiesConfiguration(props, NUM_DOCS, materialized("device.ver"));
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.LONG);
    assertEquals(metadata.getTotalDocs(), NUM_DOCS);
    assertEquals(metadata.getParentColumn(), COLUMN);
  }

  @Test
  public void testContainerStaysReadableAsJson()
      throws Exception {
    OpenStructColumnSplitter s = splitter(config(2, null), Map.of());
    for (int d = 0; d < NUM_DOCS; d++) {
      s.add(doc(d), d);
    }
    s.seal();

    // Splitting the leaves out must not cost the caller props['device']: the container is still a
    // key, and it now holds real JSON rather than a Java map rendering.
    Set<String> sparse = sparseKeys(s);
    assertTrue(sparse.contains("device"), "container must survive as a key; sparse keys were " + sparse);
    assertFalse(s.getResolvedDenseKeys().contains("device"),
        "a container of JSON text must not spend a dense column automatically");
  }

  @Test
  public void testConfiguredDenseKeyStillMaterializesContainer()
      throws Exception {
    OpenStructColumnSplitter s = splitter(config(2, Set.of("device")), Map.of());
    for (int d = 0; d < NUM_DOCS; d++) {
      s.add(doc(d), d);
    }
    s.seal();

    // The hold-out is only for automatic selection; naming the key in the table config still wins.
    assertTrue(s.getResolvedDenseKeys().contains("device"));
    assertTrue(s.getMaterializedColumnMetadata().containsKey(materialized("device")));
  }

  @Test
  public void testDeclaredChildSpecAppliesToPathKey()
      throws Exception {
    // A declared child spec is what pins a key's type across segments; it must be addressable by
    // the same dotted path the data produces.
    Map<String, FieldSpec> children =
        Map.of("device.ver", new DimensionFieldSpec("device.ver", DataType.STRING, true));
    OpenStructColumnSplitter s = splitter(config(2, null), children);
    for (int d = 0; d < NUM_DOCS; d++) {
      s.add(doc(d), d);
    }
    s.seal();

    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(materialized("device.ver"));
    assertNotNull(props);
    ColumnMetadataImpl metadata =
        ColumnMetadataImpl.fromPropertiesConfiguration(props, NUM_DOCS, materialized("device.ver"));
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.STRING);
  }

  @Test
  public void testObjectBelowDepthLimitIsNotLost()
      throws Exception {
    OpenStructColumnSplitter s = splitter(config(2, null), Map.of());
    for (int d = 0; d < NUM_DOCS; d++) {
      Map<String, Object> inner = new HashMap<>();
      inner.put("c", (long) d);
      Map<String, Object> mid = new HashMap<>();
      mid.put("b", inner);
      Map<String, Object> document = new HashMap<>();
      document.put("a", mid);
      s.add(document, d);
    }
    s.seal();

    // depth 2 reaches 'a.b' but not 'a.b.c'; 'a.b' still carries its subtree as JSON, so raising the
    // limit later is the only thing that changes, not whether the data is there.
    assertFalse(s.getResolvedDenseKeys().contains("a.b.c"));
    Set<String> allKeys = new HashSet<>(s.getResolvedDenseKeys());
    allKeys.addAll(sparseKeys(s));
    assertEquals(allKeys, Set.of("a", "a.b"));
  }

  @Test
  public void testSparseNestedLeafRoundTripsThroughJsonBlob()
      throws Exception {
    // A leaf too rare to earn a column must still be reachable under its path key from the blob.
    OpenStructColumnSplitter s = splitter(config(2, null), Map.of());
    Map<String, Object> rare = new HashMap<>();
    rare.put("os", "ios");
    Map<String, Object> first = new HashMap<>();
    first.put("device", rare);
    s.add(first, 0);
    for (int d = 1; d < NUM_DOCS; d++) {
      s.add(Map.of("page", "home"), d);
    }
    s.seal();

    assertTrue(sparseKeys(s).contains("device.os"));
  }
}
