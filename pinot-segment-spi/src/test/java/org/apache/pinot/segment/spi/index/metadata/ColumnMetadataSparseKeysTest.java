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
package org.apache.pinot.segment.spi.index.metadata;

import java.io.File;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ColumnMetadataSparseKeysTest {
  private static final String COLUMN = "metrics";

  private File _tempFile;

  @AfterMethod
  public void cleanup() {
    if (_tempFile != null) {
      _tempFile.delete();
      _tempFile = null;
    }
  }

  private PropertiesConfiguration baseParentProps() {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setListDelimiterHandler(new LegacyListDelimiterHandler(','));
    config.setProperty(Column.getKeyFor(COLUMN, Column.COLUMN_NAME), COLUMN);
    config.setProperty(Column.getKeyFor(COLUMN, Column.DATA_TYPE), FieldSpec.DataType.OPEN_STRUCT.name());
    config.setProperty(Column.getKeyFor(COLUMN, Column.COLUMN_TYPE), FieldSpec.FieldType.COMPLEX.name());
    config.setProperty(Column.getKeyFor(COLUMN, Column.IS_SINGLE_VALUED), true);
    config.setProperty(Column.getKeyFor(COLUMN, Column.TOTAL_DOCS), 10);
    config.setProperty(Column.getKeyFor(COLUMN, Column.CARDINALITY), 10);
    config.setProperty(Column.getKeyFor(COLUMN, Column.HAS_SPARSE_COLUMN), true);
    return config;
  }

  private PropertiesConfiguration saveAndReload(PropertiesConfiguration config) throws Exception {
    _tempFile = File.createTempFile("sparse-keys-metadata", ".properties");
    CommonsConfigurationUtils.saveToFile(config, _tempFile);
    return CommonsConfigurationUtils.fromFile(_tempFile);
  }

  @Test
  public void testSparseKeysListRoundTripsThroughSaveLoad() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), "[\"region\",\"latencyMs\",\"statusCode\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertEquals(metadata.getSparseKeys(), List.of("region", "latencyMs", "statusCode"));
  }

  /// A single-element manifest has no comma, so LegacyListDelimiterHandler returns the raw String
  /// on reload instead of a List — the only case exercising that branch of the rejoin logic.
  @Test
  public void testSingleSparseKeyRoundTripsThroughSaveLoad() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), "[\"onlyKey\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertEquals(metadata.getSparseKeys(), List.of("onlyKey"));
  }

  @Test
  public void testCommaInsideKeyRoundTrips() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), "[\"region\",\"weird,key\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertEquals(metadata.getSparseKeys(), List.of("region", "weird,key"));
  }

  @Test
  public void testSparseKeyLiteralDollarBraceIsNotInterpolated() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor("other", Column.DATA_TYPE), "SUBSTITUTED_VALUE");
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS),
        "[\"${column.other.dataType}\",\"plainKey\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertEquals(metadata.getSparseKeys(), List.of("${column.other.dataType}", "plainKey"));
  }

  @Test
  public void testQuoteInsideKeyRoundTrips() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), "[\"quo\\\"te\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertEquals(metadata.getSparseKeys(), List.of("quo\"te"));
  }

  /// Known limitation, pinned so a fix is noticed rather than silently changing behaviour. A
  /// multi-key manifest contains commas, so `LegacyListDelimiterHandler` treats it as a list and
  /// applies escape processing, consuming the `\\` that Jackson emitted. The rejoined value is then
  /// invalid JSON (`\s` is not a JSON escape) and segment load fails. A single-key manifest has no
  /// comma, is handled as a scalar, and is unaffected — see [#testSingleBackslashKeyIsUnaffected].
  /// Writer-side escaping in `OpenStructColumnSplitter` would fix it.
  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Failed to parse sparse-key manifest.*")
  public void testBackslashInsideKeyIsNotSupportedWithMultipleKeys() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), "[\"region\",\"back\\\\slash\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
  }

  @Test
  public void testSingleBackslashKeyIsUnaffected() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), "[\"back\\\\slash\"]");

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertEquals(metadata.getSparseKeys(), List.of("back\\slash"));
  }

  @Test
  public void testSparseKeysAbsentParsesAsNull() {
    ColumnMetadataImpl metadata =
        ColumnMetadataImpl.fromPropertiesConfiguration(baseParentProps(), 10, COLUMN);
    assertNull(metadata.getSparseKeys());
  }

  @Test
  public void testEmptySparseKeysListParsesAsNull() throws Exception {
    PropertiesConfiguration config = baseParentProps();
    config.setProperty(Column.getKeyFor(COLUMN, Column.SPARSE_KEYS), List.of());

    PropertiesConfiguration reloaded = saveAndReload(config);
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(reloaded, 10, COLUMN);
    assertNull(metadata.getSparseKeys());
  }

  @Test
  public void testSparseKeysParticipateInValueObjectMethods() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, DataType.STRING, true);
    ColumnMetadataImpl first = ColumnMetadataImpl.builder()
        .setFieldSpec(fieldSpec)
        .setSparseKeys(List.of("region", "latencyMs"))
        .build();
    ColumnMetadataImpl same = ColumnMetadataImpl.builder()
        .setFieldSpec(fieldSpec)
        .setSparseKeys(List.of("region", "latencyMs"))
        .build();
    ColumnMetadataImpl different = ColumnMetadataImpl.builder()
        .setFieldSpec(fieldSpec)
        .setSparseKeys(List.of("region"))
        .build();

    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertNotEquals(first, different);
    assertTrue(first.toString().contains("_sparseKeys=[region, latencyMs]"));
  }
}
