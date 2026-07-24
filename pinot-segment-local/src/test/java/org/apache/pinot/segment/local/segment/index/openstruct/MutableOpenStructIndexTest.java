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
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
}
