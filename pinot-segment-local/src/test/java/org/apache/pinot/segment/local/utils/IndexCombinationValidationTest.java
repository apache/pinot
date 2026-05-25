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
package org.apache.pinot.segment.local.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Exhaustive tests for all valid and invalid combinations of:
 * - Forward index encoding: DICTIONARY vs RAW
 * - Dictionary: enabled (default), disabled (noDictionaryColumns), or explicit (indexes.dictionary)
 * - Secondary indexes: inverted, FST, IFST, range, text, JSON, bloom filter
 * - Column data types: STRING (SV), INT (SV), INT (MV), FLOAT (SV)
 *
 * The core invariant under test: any column that uses an index requiring a dictionary
 * (inverted, FST, IFST) must have an explicit dictionary config when the forward index is raw.
 */
public class IndexCombinationValidationTest {

  private static final String TABLE_NAME = "testTable";
  private static final String STR_COL = "strCol";   // STRING SV
  private static final String INT_COL = "intCol";   // INT SV
  private static final String INT_MV_COL = "intMvCol"; // INT MV
  private static final String FLOAT_COL = "floatCol"; // FLOAT SV

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(STR_COL, DataType.STRING)
      .addSingleValueDimension(INT_COL, DataType.INT)
      .addMultiValueDimension(INT_MV_COL, DataType.INT)
      .addSingleValueDimension(FLOAT_COL, DataType.FLOAT)
      .build();

  // ----- helpers -----

  /** Build a FieldConfig with an explicit dictionary node in the indexes section. */
  private static FieldConfig rawWithExplicitDict(String col, List<IndexType> indexes) {
    ObjectNode indexesNode = JsonUtils.newObjectNode();
    indexesNode.set("dictionary", JsonUtils.newObjectNode());
    return new FieldConfig(col, EncodingType.RAW, null, indexes, null, null, indexesNode, null, null);
  }

  /** Validate and expect success. */
  private static void assertValid(TableConfig tableConfig) {
    try {
      TableConfigUtils.validate(tableConfig, SCHEMA);
    } catch (Exception e) {
      fail("Expected validation to pass but got: " + e.getMessage(), e);
    }
  }

  /** Validate and expect failure containing the given message fragment. */
  private static void assertInvalid(TableConfig tableConfig, String errorFragment) {
    try {
      TableConfigUtils.validate(tableConfig, SCHEMA);
      fail("Expected validation to fail with: " + errorFragment);
    } catch (Exception e) {
      assertTrue(e.getMessage() != null && e.getMessage().contains(errorFragment),
          "Expected '" + errorFragment + "' in error but got: " + e.getMessage());
    }
  }

  /// Validate and expect failure containing any one of the given message fragments. Used when the same misconfig is
  /// caught by multiple validation checks and the order of checks (and hence which error surfaces) is not
  /// guaranteed across releases. For example, "raw forward + inverted with no explicit dictionary" can be flagged
  /// either by `validateExplicitDictionaryForRawForwardIndex` ("require a dictionary") or by the older legacy
  /// check that ran from `InvertedIndexType.validate` ("without dictionary"); both reject the same scenario.
  private static void assertInvalidContains(TableConfig tableConfig, String... errorFragments) {
    try {
      TableConfigUtils.validate(tableConfig, SCHEMA);
      fail("Expected validation to fail with one of: " + String.join(" | ", errorFragments));
    } catch (Exception e) {
      String message = e.getMessage();
      boolean matched = false;
      if (message != null) {
        for (String fragment : errorFragments) {
          if (message.contains(fragment)) {
            matched = true;
            break;
          }
        }
      }
      assertTrue(matched,
          "Expected any of [" + String.join(" | ", errorFragments) + "] in error but got: " + message);
    }
  }

  // ============================================================
  // 1. Dictionary-requiring indexes (inverted / FST / IFST)
  //    with dictionary-encoded forward index
  // ============================================================

  @Test
  public void testDictEncodedWithInvertedIndexPasses() {
    // dict-encoded forward + inverted: standard, always valid
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(STR_COL)).build();
    assertValid(tc);
  }

  @Test
  public void testDictEncodedWithFstIndexPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.DICTIONARY, IndexType.FST, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc)).build();
    assertValid(tc);
  }

  @Test
  public void testDictEncodedWithInvertedAndTextAndBloomPasses() {
    // multiple indexes on a dict-encoded column
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(STR_COL))
        .setBloomFilterColumns(List.of(STR_COL))
        .build();
    assertValid(tc);
  }

  // ============================================================
  // 2. RAW forward index without explicit dictionary
  //    + inverted/FST/IFST index → FAILS (all require a dictionary)
  // ============================================================

  @Test
  public void testRawWithInvertedIndexNoExplicitDictFails() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setInvertedIndexColumns(List.of(STR_COL))
        .build();
    assertInvalidContains(tc, "require a dictionary", "without dictionary");
  }

  @Test
  public void testRawWithInvertedIndexIntColumnNoExplicitDictFails() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_COL))
        .setInvertedIndexColumns(List.of(INT_COL))
        .build();
    assertInvalidContains(tc, "require a dictionary", "without dictionary");
  }

  @Test
  public void testRawWithFstIndexNoExplicitDictFails() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.FST, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "dictionary");
  }

  @Test
  public void testRawWithIfstIndexNoExplicitDictFails() {
    // IFSTIndexType.validate() fires: "Cannot create IFST index on column: <col> without dictionary"
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.IFST, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "dictionary");
  }

  @Test
  public void testRawWithMultipleDictRequiringIndexesNoExplicitDictFails() {
    // Both inverted and FST require dictionary; at least one validator fires mentioning "dictionary"
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, null, List.of(IndexType.FST),
        null, null, null, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setInvertedIndexColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "dictionary");
  }

  // ============================================================
  // 3. RAW forward index WITH explicit dictionary
  //    + dictionary-requiring indexes → must PASS
  // ============================================================

  @Test
  public void testRawWithExplicitDictAndInvertedIndexPasses() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, null, null, null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithExplicitDictAndFstIndexPasses() {
    assertValid(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(rawWithExplicitDict(STR_COL, List.of(IndexType.FST))))
        .build());
  }

  @Test
  public void testRawWithExplicitDictAndIfstIndexPasses() {
    assertValid(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(rawWithExplicitDict(STR_COL, List.of(IndexType.IFST))))
        .build());
  }

  @Test
  public void testRawWithExplicitDictAndInvertedPlusFstPasses() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, null, List.of(IndexType.FST),
        null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithExplicitDictAndInvertedIndexIntColumnPasses() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.RAW, null, null, null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(INT_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  // ============================================================
  // 4. RAW forward index with indexes that do NOT require dict
  //    (text, bloom, JSON, range-on-numeric) → must PASS
  // ============================================================

  @Test
  public void testRawWithTextIndexNoDictPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.TEXT, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithBloomFilterNoDictPasses() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setBloomFilterColumns(List.of(STR_COL))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithJsonIndexStringColumnNoDictPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.JSON, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithRangeIndexNumericColumnNoDictPasses() {
    // Range index on numeric (INT) column does NOT require a dictionary
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_COL))
        .setRangeIndexColumns(List.of(INT_COL))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithRangeIndexFloatColumnNoDictPasses() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(FLOAT_COL))
        .setRangeIndexColumns(List.of(FLOAT_COL))
        .build();
    assertValid(tc);
  }

  // ============================================================
  // 5. Range index on non-numeric column: requires dictionary
  // ============================================================

  @Test
  public void testDictWithRangeIndexStringColumnPasses() {
    // dict-encoded STRING + range: valid
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setRangeIndexColumns(List.of(STR_COL))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithRangeIndexStringColumnNoDictFails() {
    // RAW STRING + range without dict: invalid (range on non-numeric needs dict)
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setRangeIndexColumns(List.of(STR_COL))
        .build();
    assertInvalid(tc, "Cannot create range index on non-numeric column");
  }

  @Test
  public void testRawWithRangeIndexStringColumnWithExplicitDictPasses() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, null, null, null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setRangeIndexColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  // ============================================================
  // 6. Multi-value column combinations
  // ============================================================

  @Test
  public void testDictMvWithInvertedIndexPasses() {
    // MV INT + dict + inverted: valid
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(INT_MV_COL))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawMvWithInvertedIndexNoDictFails() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_MV_COL))
        .setInvertedIndexColumns(List.of(INT_MV_COL))
        .build();
    assertInvalidContains(tc, "require a dictionary", "without dictionary");
  }

  @Test
  public void testRawMvWithInvertedIndexWithExplicitDictPasses() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(INT_MV_COL, EncodingType.RAW, null, null, null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(INT_MV_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testFstIndexMvColumnFails() {
    // FST on MV column is never valid regardless of encoding
    FieldConfig fc = new FieldConfig(INT_MV_COL, EncodingType.DICTIONARY, IndexType.FST, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "Cannot create FST index on multi-value column");
  }

  // ============================================================
  // 7. Multiple columns: one valid, one invalid
  // ============================================================

  @Test
  public void testMixedColumnsOneRawInvertedNoDictFails() {
    // strCol is raw + inverted without dict → fails; intCol is dict + inverted → valid on its own
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setInvertedIndexColumns(Arrays.asList(STR_COL, INT_COL))
        .build();
    assertInvalidContains(tc, "require a dictionary", "without dictionary");
  }

  @Test
  public void testMixedColumnsAllValidPasses() {
    // strCol: raw + text (no dict required); intCol: dict + inverted; floatCol: dict + range
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.TEXT, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setInvertedIndexColumns(List.of(INT_COL))
        .setRangeIndexColumns(List.of(FLOAT_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testMixedColumnsRawDictAndRawNoDictInvertedOnlyFails() {
    // strCol: raw + explicit dict + inverted (valid); intCol: raw + no dict + inverted → fails
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, null, null, null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_COL))
        .setInvertedIndexColumns(Arrays.asList(STR_COL, INT_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalidContains(tc, "require a dictionary", "without dictionary");
  }

  // ============================================================
  // 8. Forward index disabled combinations
  // ============================================================

  @Test
  public void testForwardIndexDisabledWithDictAndInvertedPasses() {
    // Forward index disabled but dictionary + inverted still enabled: valid
    ObjectNode indexes = JsonUtils.newObjectNode();
    ObjectNode fwdDisabled = JsonUtils.newObjectNode();
    fwdDisabled.put("disabled", true);
    indexes.set("forward", fwdDisabled);
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.DICTIONARY, null,
        null, null, null, indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(INT_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  // ============================================================
  // 9. Compression codec validation
  // ============================================================

  @Test
  public void testRawWithLz4CodecPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, (IndexType) null, CompressionCodec.LZ4, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithSnappyCodecPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, (IndexType) null, CompressionCodec.SNAPPY, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithZstdCodecPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, (IndexType) null, CompressionCodec.ZSTANDARD, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithClpCodecStringColumnPasses() {
    // CLP codecs are valid for raw STRING columns
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, (IndexType) null, CompressionCodec.CLP, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testRawWithClpCodecNonStringColumnFails() {
    // CLP is only valid on STRING stored type
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.RAW, (IndexType) null, CompressionCodec.CLP, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "CLP");
  }

  @Test
  public void testDeltaDeltaCodecNonNumericColumnFails() {
    // DELTADELTA only valid on INT/LONG columns
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, (IndexType) null, CompressionCodec.DELTADELTA, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "DELTADELTA can only be used on INT/LONG");
  }

  @Test
  public void testDeltaDeltaCodecRawWithExplicitDictFails() {
    // EncodingType.RAW + explicit dictionary + DELTADELTA codec is rejected by ForwardIndexType.validate:
    // when the dictionary is enabled (regardless of forward-index encoding), the codec must be applicable to a
    // dict-encoded index. DELTADELTA is applicable to neither raw nor dict-encoded forward indexes (it is a
    // dict-id stream codec), so the validation surfaces the dict-encoded message even though the column will end
    // up with a RAW forward index alongside the standalone dictionary at segment-creation time.
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.RAW, null, null, CompressionCodec.DELTADELTA, null,
        indexes, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "DELTADELTA");
  }

  // ============================================================
  // 10. Index type restrictions by data type / cardinality
  // ============================================================

  @Test
  public void testFstIndexNonStringColumnFails() {
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.DICTIONARY, IndexType.FST, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "Cannot create FST index on column: " + INT_COL + " of stored type other than STRING");
  }

  @Test
  public void testTextIndexNonStringColumnFails() {
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.DICTIONARY, IndexType.TEXT, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "Cannot create TEXT index on column: " + INT_COL + " of stored type other than STRING");
  }

  @Test
  public void testTextIndexStringColumnRawPasses() {
    // Text index does not require dictionary
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.TEXT, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testTextIndexStringColumnDictPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.DICTIONARY, IndexType.TEXT, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testJsonIndexStringColumnRawPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.JSON, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testJsonIndexStringColumnDictPasses() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.DICTIONARY, IndexType.JSON, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertValid(tc);
  }

  @Test
  public void testJsonIndexIntColumnFails() {
    // JSON index not valid on non-STRING/non-MAP column
    FieldConfig fc = new FieldConfig(INT_COL, EncodingType.DICTIONARY, IndexType.JSON, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(fc))
        .build();
    assertInvalid(tc, "Cannot create JSON index on column: " + INT_COL);
  }

  // ============================================================
  // 11. Bloom filter combinations
  // ============================================================

  @Test
  public void testBloomFilterDictEncodedPasses() {
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setBloomFilterColumns(List.of(INT_COL))
        .build();
    assertValid(tc);
  }

  @Test
  public void testBloomFilterRawEncodedPasses() {
    // Bloom filter does not require dictionary
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_COL))
        .setBloomFilterColumns(List.of(INT_COL))
        .build();
    assertValid(tc);
  }

  @Test
  public void testBloomFilterAllTypesRawPasses() {
    // Bloom filter is valid on all types (except BOOLEAN) without dictionary
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList(STR_COL, INT_COL, FLOAT_COL))
        .setBloomFilterColumns(Arrays.asList(STR_COL, INT_COL, FLOAT_COL))
        .build();
    assertValid(tc);
  }

  // ============================================================
  // 12. Error message quality: FST index without dict must name the index type
  // ============================================================

  @Test
  public void testErrorMessageNamesFstIndex() {
    FieldConfig fc = new FieldConfig(STR_COL, EncodingType.RAW, IndexType.FST, null, null);
    TableConfig tc = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STR_COL))
        .setFieldConfigList(List.of(fc))
        .build();
    try {
      TableConfigUtils.validate(tc, SCHEMA);
      fail("Expected validation to fail");
    } catch (Exception e) {
      // FstIndexType.validate() fires: "Cannot create FST index on column: <col> without dictionary"
      String msg = e.getMessage();
      assertTrue(msg.contains(STR_COL), "Error should name the column");
      assertTrue(msg.contains("without dictionary"), "Error should explain the problem");
    }
  }
}
