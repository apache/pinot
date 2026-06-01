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

import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class OpenStructColumnSplitterTest {

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("OpenStructColumnSplitterTest").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  private ComplexFieldSpec spec() {
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
  }

  private OpenStructIndexConfig config(double minFillRate, int maxDenseKeys, Set<String> denseKeys) {
    return new OpenStructIndexConfig(false, null, maxDenseKeys, denseKeys, minFillRate, null);
  }

  @Test
  public void testClassifyByFillRate()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      Map<String, Object> doc = d < 7 ? Map.of("clicks", (long) d) : Map.of();
      s.add(doc, d);
    }
    Set<String> dense = s.classify();
    assertTrue(dense.contains("clicks"));
  }

  @Test
  public void testExplicitDenseKeysAlwaysMaterialized()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.99, -1, Set.of("rare")));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 100; d++) {
      s.add(Map.of(), d);
    }
    Set<String> dense = s.classify();
    assertTrue(dense.contains("rare"));
  }

  @Test
  public void testRareKeyDroppedFromDense()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 100; d++) {
      s.add(Map.of(), d);
    }
    Set<String> dense = s.classify();
    assertFalse(dense.contains("rare"));
  }

  @Test
  public void testMaxDenseKeysCap()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.1, 1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("a", "x", "b", "y", "c", "z"), d);
    }
    Set<String> dense = s.classify();
    assertEquals(dense.size(), 1);
  }

  @Test
  public void testZeroDocsIsNoop()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    s.seal();
    assertTrue(s.getResolvedDenseKeys().isEmpty());
  }

  @Test
  public void testSealEmitsParentMetadataForDense()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("clicks", (long) d), d);
    }
    s.seal();
    String denseCol = OpenStructNaming.materializedColumnName("metrics", "clicks");
    Map<String, PropertiesConfiguration> meta = s.getMaterializedColumnMetadata();
    PropertiesConfiguration denseProps = meta.get(denseCol);
    assertEquals(denseProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN)), "metrics");

    PropertiesConfiguration parentProps = meta.get("metrics");
    assertNotNull(parentProps);
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.DATA_TYPE)), "OPEN_STRUCT");
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.COLUMN_TYPE)), "COMPLEX");
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN)), "false");
  }

  @Test
  public void testSparseJsonColumnWritten()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.9, -1, null));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 10; d++) {
      s.add(Map.of(), d);
    }
    s.seal();
    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    assertTrue(s.getMaterializedColumnMetadata().containsKey(sparseCol));
  }
}
