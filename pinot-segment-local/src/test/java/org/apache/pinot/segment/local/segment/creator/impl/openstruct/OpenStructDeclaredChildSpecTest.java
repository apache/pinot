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
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// A materialized child used to be synthesized as a dimension of the key's *stored* type, so a key declared
/// TIMESTAMP, BOOLEAN or UUID landed as the LONG, INT or BYTES it happens to be stored as. The logical type was
/// lost, along with every operator that depends on knowing which it was, and an absent document read the stored
/// type's default rather than the declared one — a different value from the one
/// `OpenStructDataSource#getValueFieldSpec` resolves for a key absent from the whole segment.
///
/// See https://github.com/apache/pinot/issues/19466
public class OpenStructDeclaredChildSpecTest {

  private static final String COLUMN = "props";
  private static final int NUM_DOCS = 10;

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("OpenStructDeclaredChildSpecTest").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  /// Seals a segment whose `props` column declares `key` with `declared`, filling every doc with `value`.
  private ColumnMetadataImpl sealAndRead(String key, @Nullable FieldSpec declared, Object value)
      throws Exception {
    Map<String, FieldSpec> children = new HashMap<>();
    if (declared != null) {
      children.put(key, declared);
    }
    ComplexFieldSpec parent = new ComplexFieldSpec(COLUMN, DataType.OPEN_STRUCT, true, children);
    OpenStructColumnSplitter splitter = new OpenStructColumnSplitter(_tempDir, COLUMN, "testTable_OFFLINE", parent,
        new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null, null, null));
    for (int d = 0; d < NUM_DOCS; d++) {
      // The last doc omits the key, so the absent-doc default is exercised too.
      splitter.add(d == NUM_DOCS - 1 ? Map.of() : Map.of(key, value), d);
    }
    splitter.seal();

    String materializedCol = OpenStructNaming.materializedColumnName(COLUMN, key);
    PropertiesConfiguration props = splitter.getMaterializedColumnMetadata().get(materializedCol);
    assertNotNull(props, "key must be materialized: " + key);
    return ColumnMetadataImpl.fromPropertiesConfiguration(props, NUM_DOCS, materializedCol);
  }

  @Test
  public void testDeclaredUuidKeepsItsType()
      throws Exception {
    ColumnMetadataImpl metadata = sealAndRead("device_id",
        new DimensionFieldSpec("device_id", DataType.UUID, true),
        UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"));

    // Stored as BYTES either way; what changed is that the column still says it is a UUID.
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.UUID);
    assertEquals(metadata.getFieldSpec().getDataType().getStoredType(), DataType.BYTES);
  }

  @Test
  public void testDeclaredTimestampKeepsItsType()
      throws Exception {
    ColumnMetadataImpl metadata = sealAndRead("seen_at",
        new DimensionFieldSpec("seen_at", DataType.TIMESTAMP, true),
        new Timestamp(1789700000000L));

    assertEquals(metadata.getFieldSpec().getDataType(), DataType.TIMESTAMP);
  }

  @Test
  public void testDeclaredBooleanKeepsItsType()
      throws Exception {
    ColumnMetadataImpl metadata = sealAndRead("is_cached",
        new DimensionFieldSpec("is_cached", DataType.BOOLEAN, true), true);

    assertEquals(metadata.getFieldSpec().getDataType(), DataType.BOOLEAN);
  }

  @Test
  public void testDeclaredDefaultNullValueIsWhatAbsentDocsRead()
      throws Exception {
    DimensionFieldSpec declared = new DimensionFieldSpec("region", DataType.STRING, true);
    declared.setDefaultNullValue("unknown");
    ColumnMetadataImpl metadata = sealAndRead("region", declared, "us-east");

    // The doc that omitted the key must read the declared default, which is also what getValueFieldSpec resolves
    // for a key missing from the segment entirely.
    assertEquals(metadata.getFieldSpec().getDefaultNullValue(), "unknown");
    assertEquals(metadata.getMinValue(), "unknown", "the absent doc contributes the declared default to stats");
    assertEquals(metadata.getMaxValue(), "us-east");
  }

  @Test
  public void testUndeclaredKeyStillUsesTheInferredStoredType()
      throws Exception {
    ColumnMetadataImpl metadata = sealAndRead("latency_ms", null, 240);

    assertEquals(metadata.getFieldSpec().getDataType(), DataType.INT);
    assertEquals(metadata.getFieldSpec().getDefaultNullValue(), Integer.MIN_VALUE);
  }

  @Test
  public void testDeclaredTypeWinsOverTheInferredOne()
      throws Exception {
    // The value infers as INT; the declaration says LONG and must be what the column reports.
    ColumnMetadataImpl metadata = sealAndRead("count",
        new DimensionFieldSpec("count", DataType.LONG, true), 240);

    assertEquals(metadata.getFieldSpec().getDataType(), DataType.LONG);
  }
}
