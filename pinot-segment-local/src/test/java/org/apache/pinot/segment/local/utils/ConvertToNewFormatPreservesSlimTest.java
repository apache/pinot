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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.spi.index.MergedColumnConfigDeserializer.ConfigDeclaredTwiceException;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


/**
 * Pins the gap-filling contract for {@link TableConfigUtils#createTableConfigFromOldFormat} /
 * {@code AbstractIndexType#convertToNewFormat}: <b>migration only writes
 * {@code FieldConfig.indexes[prettyName]} when the column does not already carry a JsonNode for
 * that index type</b>. Columns already in new format keep their slim shape verbatim; legacy-only
 * inputs are translated as before. Same-column same-type coexistence is rejected upstream with
 * {@code ConfigDeclaredTwiceException}; different index types on the same column translate
 * independently.
 *
 * <p>This contract is load-bearing for downstream APIs (e.g. StarTree Preview / InferIndex) that
 * round-trip user-supplied {@code FieldConfig.indexes} JsonNodes through the migration step and
 * back into a response: without the gap-filling rule the user's slim shape gets fattened on
 * every round-trip because the typed-POJO bean serializer always emits all keys.
 */
public class ConvertToNewFormatPreservesSlimTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // ---- Pure new-format input survives byte-for-byte ----

  @Test
  public void newFormatForwardSlimPreservedVerbatim() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("compressionCodec", "SNAPPY");
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode forward = indexesOf(migrated, "c1").get("forward");
    assertNotNull(forward);
    assertEquals(forward.size(), 1, "Single user key survives verbatim. Got: " + forward);
    assertEquals(forward.get("compressionCodec").asText(), "SNAPPY");
  }

  @Test
  public void newFormatBloomSlimPreservedVerbatim() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("bloom").put("fpp", 0.1);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode bloom = indexesOf(migrated, "c1").get("bloom");
    assertNotNull(bloom);
    assertEquals(bloom.size(), 1);
    assertEquals(bloom.get("fpp").asDouble(), 0.1);
  }

  @Test
  public void newFormatExplicitDefaultValuePreserved() {
    // User explicitly sets a value that happens to equal today's default. Migration must keep
    // the explicit key so a future default flip cannot silently swallow user intent.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("rawIndexWriterVersion", 4);
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode forward = indexesOf(migrated, "c1").get("forward");
    assertEquals(forward.size(), 1);
    assertEquals(forward.get("rawIndexWriterVersion").asInt(), 4);
  }

  @Test
  public void newFormatExplicitFalsePreserved() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("deriveNumDocsPerChunk", false);
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode forward = indexesOf(migrated, "c1").get("forward");
    assertEquals(forward.size(), 1);
    assertFalse(forward.get("deriveNumDocsPerChunk").asBoolean());
  }

  @Test
  public void newFormatExplicitDisabledTruePreserved() {
    // User opts out of forward by setting disabled=true. Migration must keep that exact shape,
    // not overwrite with the typed POJO's disabled=false default — silent re-enable would be a
    // correctness bug. Non-default typed POJO → gap-fill branch fires.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("disabled", true);
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode forward = indexesOf(migrated, "c1").get("forward");
    assertEquals(forward.size(), 1);
    assertTrue(forward.get("disabled").asBoolean(), "Explicit disabled=true preserved");
  }

  @Test
  public void newFormatExplicitNullValueRejectedByIndexTypeValidator() {
    // Pin the actual handling of {"forward": null}: per-index-type validators (here
    // ForwardIndexType.createDeserializer's lambda) reject NullNode as an invalid forward index
    // config and raise IllegalStateException. The gap-fill loop is never reached. Users wanting
    // "enabled with defaults" must use {} (empty object), not null.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.set("forward", NullNode.getInstance());
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    try {
      TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));
      fail("Expected IllegalStateException from ForwardIndexType validator");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains("column: c1"),
          "Expected validation error naming column c1; got: " + expected.getMessage());
    }
  }

  @Test
  public void newFormatPrimitiveAtPrettyNameRejected() {
    // Adversarial input: {"forward": 42}. ForwardIndexType's createDeserializer rejects
    // non-object values; pin the loud failure path.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.put("forward", 42);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    try {
      TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));
      fail("Expected IllegalStateException for primitive at prettyName");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains("column: c1"),
          "Expected validation error naming column c1; got: " + expected.getMessage());
    }
  }

  @Test
  public void newFormatArrayAtPrettyNameRejected() {
    // Adversarial input: {"forward": [1,2,3]}. Same validator path as above.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putArray("forward").add(1).add(2).add(3);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    try {
      TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));
      fail("Expected IllegalStateException for array at prettyName");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains("column: c1"),
          "Expected validation error naming column c1; got: " + expected.getMessage());
    }
  }

  @Test
  public void newFormatEmptyObjectPreserved() {
    // {"inverted": {}} means "enabled with no settings" — must remain an empty object, not be
    // replaced with the typed POJO default.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("inverted");
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode inverted = indexesOf(migrated, "c1").get("inverted");
    assertNotNull(inverted);
    assertTrue(inverted.isObject());
    assertEquals(inverted.size(), 0);
  }

  // ---- Pure legacy input still translates to new format (back-compat) ----

  @Test
  public void legacyBloomFilterColumnsTranslatedToNewFormat() {
    TableConfig tc = baseTc();
    tc.getIndexingConfig().setBloomFilterColumns(new ArrayList<>(Arrays.asList("c1")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode indexes = indexesOf(migrated, "c1");
    assertNotNull(indexes.get("bloom"), "Legacy bloomFilterColumns translates to new format");
  }

  @Test
  public void legacyNoDictionaryColumnsTranslatedToRawEncoding() {
    TableConfig tc = baseTc();
    tc.getIndexingConfig().setNoDictionaryColumns(new ArrayList<>(Arrays.asList("c1")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    FieldConfig fc = findFieldConfig(migrated, "c1");
    assertEquals(fc.getEncodingType(), EncodingType.RAW);
  }

  // ---- Both-formats coexistence ----

  @Test
  public void bothFormatsDifferentIndexTypesCoexist() {
    // User: slim new-format bloom on c1. Legacy: noDictionaryColumns=[c1] (i.e. RAW encoding).
    // Different index types in different surfaces — both translate independently, each obeying the
    // gap-fill rule for its own type.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("bloom").put("fpp", 0.1);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);
    tc.getIndexingConfig().setNoDictionaryColumns(new ArrayList<>(Arrays.asList("c1")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    // bloom: user slim shape preserved verbatim.
    JsonNode bloom = indexesOf(migrated, "c1").get("bloom");
    assertEquals(bloom.size(), 1, "User slim shape preserved. Got: " + bloom);
    assertEquals(bloom.get("fpp").asDouble(), 0.1);
    // encoding flipped to RAW by legacy translation.
    assertEquals(findFieldConfig(migrated, "c1").getEncodingType(), EncodingType.RAW);
  }

  @Test
  public void bothFormatsSameIndexTypeThrowsConfigDeclaredTwice() {
    // User: new-format bloom on c1. Legacy: bloomFilterColumns=[c1]. Same column + same index type
    // declared in both legacy and new format → existing deserializer raises ConfigDeclaredTwice.
    // Pin this behavior — gap-fill must not paper over the conflict.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("bloom").put("fpp", 0.1);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);
    tc.getIndexingConfig().setBloomFilterColumns(new ArrayList<>(Arrays.asList("c1")));

    expectThrows(ConfigDeclaredTwiceException.class,
        () -> TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1")));
  }

  // ---- Idempotency: running migration twice == running it once ----

  @Test
  public void migrationIsIdempotentOnNewFormatInput() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("compressionCodec", "SNAPPY");
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig once = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));
    TableConfig twice = TableConfigUtils.createTableConfigFromOldFormat(once, schemaWith("c1"));

    assertEquals(
        indexesOf(twice, "c1").toString(),
        indexesOf(once, "c1").toString(),
        "Running migration twice produces the same FieldConfig.indexes as once");
  }

  @Test
  public void migrationIsIdempotentOnLegacyInput() {
    TableConfig tc = baseTc();
    tc.getIndexingConfig().setBloomFilterColumns(new ArrayList<>(Arrays.asList("c1")));

    TableConfig once = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));
    TableConfig twice = TableConfigUtils.createTableConfigFromOldFormat(once, schemaWith("c1"));

    assertEquals(
        indexesOf(twice, "c1").toString(),
        indexesOf(once, "c1").toString());
  }

  // ---- Multi-column independence ----

  @Test
  public void mixedColumnsEachShapeIndependent() {
    // c1: slim new format. c2: legacy only. c3: column not in field config (added by legacy).
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("compressionCodec", "SNAPPY");
    TableConfig tc = baseTc();
    List<FieldConfig> fcl = new ArrayList<>();
    fcl.add(new FieldConfig.Builder("c1").withEncodingType(EncodingType.RAW).withIndexes(slim).build());
    tc.setFieldConfigList(fcl);
    tc.getIndexingConfig().setBloomFilterColumns(new ArrayList<>(Arrays.asList("c2", "c3")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1", "c2", "c3"));

    // c1: slim survives.
    JsonNode c1Forward = indexesOf(migrated, "c1").get("forward");
    assertEquals(c1Forward.size(), 1);
    assertEquals(c1Forward.get("compressionCodec").asText(), "SNAPPY");

    // c2 + c3: legacy translated to new format.
    assertNotNull(indexesOf(migrated, "c2").get("bloom"));
    assertNotNull(indexesOf(migrated, "c3").get("bloom"));
  }

  // ---- Multiple index types on one column ----

  @Test
  public void multipleIndexTypesOneColumnEachSlimShapePreserved() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("compressionCodec", "LZ4");
    slim.putObject("bloom").put("fpp", 0.05);
    slim.putObject("inverted");
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode indexes = indexesOf(migrated, "c1");
    assertEquals(indexes.get("forward").size(), 1);
    assertEquals(indexes.get("forward").get("compressionCodec").asText(), "LZ4");
    assertEquals(indexes.get("bloom").size(), 1);
    assertEquals(indexes.get("bloom").get("fpp").asDouble(), 0.05);
    assertEquals(indexes.get("inverted").size(), 0);
  }

  // ---- Cross-type inference still fires (dictionary auto-create for inverted, etc.) ----

  @Test
  public void invertedIndexLegacyOnlyTranslatedWithoutFattening() {
    // Pure legacy invertedIndexColumns. No FieldConfig.indexes yet → migration writes the inverted
    // entry. Verbose form is acceptable here because the user did not supply a slim shape.
    TableConfig tc = baseTc();
    tc.getIndexingConfig().setInvertedIndexColumns(new ArrayList<>(Arrays.asList("c1")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode inverted = indexesOf(migrated, "c1").get("inverted");
    assertNotNull(inverted, "Legacy invertedIndexColumns translates to new format");
  }

  // ---- Legacy cleanup ----

  @Test
  public void legacyFieldsClearedAfterMigration() {
    TableConfig tc = baseTc();
    tc.getIndexingConfig().setBloomFilterColumns(new ArrayList<>(Arrays.asList("c1")));
    tc.getIndexingConfig().setNoDictionaryColumns(new ArrayList<>(Arrays.asList("c2")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1", "c2"));
    IndexingConfig ic = migrated.getIndexingConfig();

    assertTrue(ic.getBloomFilterColumns() == null || ic.getBloomFilterColumns().isEmpty(),
        "Legacy bloomFilterColumns cleared post-migration");
    assertTrue(ic.getNoDictionaryColumns() == null || ic.getNoDictionaryColumns().isEmpty(),
        "Legacy noDictionaryColumns cleared post-migration");
  }

  @Test
  public void legacyCleanupRunsEvenWhenUserSlimShapePreserved() {
    // User has slim new-format bloom on c1 (new format) AND legacy invertedIndexColumns=[c1]
    // (different index type, so no ConfigDeclaredTwice). Migration preserves user slim bloom and
    // also translates legacy inverted → new format. Legacy cleanup must still drop
    // invertedIndexColumns from indexingConfig so ZK doesn't carry both representations.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("bloom").put("fpp", 0.1);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);
    tc.getIndexingConfig().setInvertedIndexColumns(new ArrayList<>(Arrays.asList("c1")));

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    assertTrue(
        migrated.getIndexingConfig().getInvertedIndexColumns() == null
            || migrated.getIndexingConfig().getInvertedIndexColumns().isEmpty(),
        "Legacy invertedIndexColumns dropped post-migration");
    assertEquals(indexesOf(migrated, "c1").get("bloom").size(), 1,
        "User slim bloom shape preserved untouched");
    assertNotNull(indexesOf(migrated, "c1").get("inverted"),
        "Legacy inverted translated into new format");
  }

  // ---- Default-config skip path ----

  @Test
  public void defaultConfigInNewFormatKeptVerbatim() {
    // User supplies a config that equals the type's default. Today's code: skipped (continue).
    // Behavior preserved with the gap-fill change.
    ObjectNode slim = MAPPER.createObjectNode();
    // disabled=false matches IndexConfig default → typed value equals default → skipped.
    slim.putObject("inverted").put("disabled", false);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    // The original JsonNode is preserved verbatim because we never wrote (skipped).
    JsonNode inverted = indexesOf(migrated, "c1").get("inverted");
    assertNotNull(inverted);
    assertTrue(inverted.has("disabled"));
    assertFalse(inverted.get("disabled").asBoolean(), "User's explicit disabled=false preserved");
  }

  // ---- Cross-type coverage: gap-fill applies uniformly to every AbstractIndexType subclass ----

  @Test
  public void newFormatRangeSlimPreservedVerbatim() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("range").put("version", 1);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode range = indexesOf(migrated, "c1").get("range");
    assertEquals(range.size(), 1, "Range slim preserved. Got: " + range);
    assertEquals(range.get("version").asInt(), 1);
  }

  @Test
  public void newFormatTextSlimPreservedVerbatim() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("text").put("luceneAnalyzerClass", "org.example.MyAnalyzer");
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode text = indexesOf(migrated, "c1").get("text");
    assertEquals(text.size(), 1, "Text slim preserved. Got: " + text);
    assertEquals(text.get("luceneAnalyzerClass").asText(), "org.example.MyAnalyzer");
  }

  @Test
  public void newFormatJsonSlimPreservedVerbatim() {
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("json").put("maxLevels", 5);
    TableConfig tc = withFieldConfig("c1", EncodingType.DICTIONARY, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));

    JsonNode json = indexesOf(migrated, "c1").get("json");
    assertEquals(json.size(), 1, "Json slim preserved. Got: " + json);
    assertEquals(json.get("maxLevels").asInt(), 5);
  }

  // ---- End-to-end Jackson round-trip: slim shape survives serialize → deserialize ----

  @Test
  public void slimShapeSurvivesJacksonRoundTrip() throws Exception {
    // Production failure mode: user POSTs slim JSON → service deserializes → migration → service
    // re-serializes the result → response gets fattened with bean-serializer defaults. Pin the
    // full round-trip so a regression in FieldConfig serialization or IndexConfig.toJsonNode
    // would break this test, not just the in-memory checks above.
    ObjectNode slim = MAPPER.createObjectNode();
    slim.putObject("forward").put("compressionCodec", "SNAPPY");
    TableConfig tc = withFieldConfig("c1", EncodingType.RAW, slim);

    TableConfig migrated = TableConfigUtils.createTableConfigFromOldFormat(tc, schemaWith("c1"));
    String serialized = MAPPER.writeValueAsString(migrated);
    TableConfig deserialized = MAPPER.readValue(serialized, TableConfig.class);

    JsonNode forward = indexesOf(deserialized, "c1").get("forward");
    assertNotNull(forward, "forward survives Jackson round-trip");
    assertEquals(forward.size(), 1,
        "Slim shape survives serialize → deserialize. Got: " + forward);
    assertEquals(forward.get("compressionCodec").asText(), "SNAPPY");
  }

  // ---- Helpers ----

  private static TableConfig baseTc() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
  }

  private static TableConfig withFieldConfig(String name, EncodingType encoding, JsonNode indexes) {
    TableConfig tc = baseTc();
    List<FieldConfig> fcl = new ArrayList<>();
    fcl.add(new FieldConfig.Builder(name).withEncodingType(encoding).withIndexes(indexes).build());
    tc.setFieldConfigList(fcl);
    return tc;
  }

  private static Schema schemaWith(String... cols) {
    Schema.SchemaBuilder b = new Schema.SchemaBuilder().setSchemaName("t");
    for (String c : cols) {
      b.addSingleValueDimension(c, DataType.STRING);
    }
    return b.build();
  }

  private static FieldConfig findFieldConfig(TableConfig tc, String col) {
    return tc.getFieldConfigList().stream()
        .filter(fc -> fc.getName().equals(col))
        .findFirst()
        .orElseThrow();
  }

  private static JsonNode indexesOf(TableConfig tc, String col) {
    return findFieldConfig(tc, col).getIndexes();
  }
}
