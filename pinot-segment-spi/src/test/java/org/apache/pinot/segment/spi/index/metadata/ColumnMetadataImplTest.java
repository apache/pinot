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

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link ColumnMetadataImpl#fromPropertiesConfiguration} focused on the
 * {@code FORWARD_INDEX_ENCODING} property's rolling-upgrade behavior.
 *
 * <p>{@code FORWARD_INDEX_ENCODING} was added in this release. Old segments built before this release won't have
 * the key in {@code metadata.properties}, so {@link ColumnMetadataImpl#fromPropertiesConfiguration} falls back to
 * deriving the encoding from {@code HAS_DICTIONARY}: dict means {@code DICTIONARY}-encoded forward, no dict means
 * {@code RAW}. The new "shared dictionary on RAW forward" segment shape is only representable when the key is
 * explicitly written by the new segment creator.
 */
public class ColumnMetadataImplTest {

  /**
   * Old-segment fallback path: no FORWARD_INDEX_ENCODING in metadata, dict present → encoding inferred as DICTIONARY.
   */
  @Test
  public void fallsBackToDictionaryEncodingWhenKeyAbsentAndHasDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    // FORWARD_INDEX_ENCODING intentionally NOT set, simulating an old segment.

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.DICTIONARY,
        "Old segments without FORWARD_INDEX_ENCODING and HAS_DICTIONARY=true must infer DICTIONARY encoding");
  }

  /**
   * Old-segment fallback path: no FORWARD_INDEX_ENCODING in metadata, no dict → encoding inferred as RAW.
   */
  @Test
  public void fallsBackToRawEncodingWhenKeyAbsentAndNoDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), false);
    // FORWARD_INDEX_ENCODING intentionally NOT set, simulating an old raw-forward segment.

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertFalse(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.RAW,
        "Old segments without FORWARD_INDEX_ENCODING and HAS_DICTIONARY=false must infer RAW encoding");
  }

  /**
   * New shared-dict shape: FORWARD_INDEX_ENCODING=RAW + HAS_DICTIONARY=true. The new segment creator writes both
   * keys; the metadata loader must honor the explicit FORWARD_INDEX_ENCODING and not fall back to inference.
   */
  @Test
  public void honorsExplicitRawEncodingEvenWhenHasDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    config.setProperty(Column.getKeyFor("col", Column.FORWARD_INDEX_ENCODING), EncodingType.RAW.name());

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.RAW,
        "Explicit FORWARD_INDEX_ENCODING=RAW must override inference even when HAS_DICTIONARY=true (shared-dict)");
  }

  /**
   * New segment with explicit FORWARD_INDEX_ENCODING=DICTIONARY; verify it round-trips.
   */
  @Test
  public void honorsExplicitDictionaryEncoding() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    config.setProperty(Column.getKeyFor("col", Column.FORWARD_INDEX_ENCODING), EncodingType.DICTIONARY.name());

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.DICTIONARY);
  }

  @Test
  public void parentColumnRoundtrip() {
    ColumnMetadataImpl meta = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("metrics$cpu", DataType.DOUBLE, true))
        .setParentColumn("metrics")
        .build();
    assertEquals(meta.getParentColumn(), "metrics");
    assertTrue(meta.isMaterializedChild());
  }

  /**
   * Verify the PARENT_COLUMN key in metadata.properties round-trips through
   * {@link ColumnMetadataImpl#fromPropertiesConfiguration}.
   */
  @Test
  public void parentColumnReadFromPropertiesConfig() {
    PropertiesConfiguration config = baseConfig("metrics$cpu");
    config.setProperty(Column.getKeyFor("metrics$cpu", Column.PARENT_COLUMN), "metrics");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "metrics$cpu");

    assertEquals(metadata.getParentColumn(), "metrics");
    assertTrue(metadata.isMaterializedChild());
  }

  @Test
  public void transformFunctionRoundtrip() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    ColumnMetadataImpl meta = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", DataType.STRING, true))
        .setTransformFunction(transformFunction)
        .build();

    assertEquals(meta.getTransformFunction(), transformFunction);
  }

  @Test
  public void transformFunctionReadFromPropertiesConfig() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    String escapedTransformFunction =
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(transformFunction);
    assertNotNull(escapedTransformFunction);
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION), escapedTransformFunction);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertEquals(metadata.getTransformFunction(), transformFunction);
  }

  @Test
  public void missingTransformFunctionIsBackwardCompatible() {
    PropertiesConfiguration config = baseConfig("col");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertNull(metadata.getTransformFunction());
  }

  private static PropertiesConfiguration baseConfig(String column) {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_NAME), column);
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_TYPE), FieldType.DIMENSION.name());
    config.setProperty(Column.getKeyFor(column, Column.DATA_TYPE), DataType.STRING.name());
    config.setProperty(Column.getKeyFor(column, Column.IS_SINGLE_VALUED), true);
    config.setProperty(Column.getKeyFor(column, Column.CARDINALITY), 1);
    return config;
  }
}
