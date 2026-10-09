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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class OpenStructIndexTypeTest {

  @DataProvider(name = "codecSpecOpenStructConfigs")
  public Object[][] codecSpecOpenStructConfigs() {
    OpenStructIndexConfig valueConfig = new OpenStructIndexConfig(false, null, -1, null, 0.5,
        List.of(rawCodecSpecFieldConfig("clicks")), null, null, null);
    OpenStructIndexConfig defaultConfig = new OpenStructIndexConfig(false, rawCodecSpecFieldConfig("default"), -1,
        null, 0.5, null, null, null, null);
    return new Object[][]{{valueConfig, "key 'clicks'"}, {defaultConfig, "defaultValueFieldConfig"}};
  }

  @Test
  public void testServiceLoaderResolves() {
    assertNotNull(StandardIndexes.openStruct(),
        "StandardIndexes.openStruct() should resolve via OpenStructIndexPlugin");
  }

  @Test
  public void testIndexIdMatches() {
    assertEquals(StandardIndexes.openStruct().getId(), StandardIndexes.OPEN_STRUCT_ID);
  }

  @Test
  public void testSingletonInstance() {
    assertSame(StandardIndexes.openStruct(), OpenStructIndexPlugin.INSTANCE);
  }

  @Test
  public void testValidateRejectsUnsupportedPerKeyIndex()
      throws Exception {
    // 'h3' is not in the OPEN_STRUCT vetted subset; declaring it on a key must fail validation.
    JsonNode indexes = JsonUtils.stringToJsonNode("{\"h3\": {}}");
    FieldConfig keyConfig = new FieldConfig.Builder("loc").withIndexes(indexes).build();
    // First constructor arg is `disabled` — pass false so the config is enabled and validation runs.
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(keyConfig), null);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true, Map.of());

    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null));
  }

  @Test
  public void testValidateAllowsVettedPerKeyIndexes()
      throws Exception {
    JsonNode indexes = JsonUtils.stringToJsonNode("{\"range\": {}, \"bloom\": {}, \"inverted\": {}}");
    FieldConfig keyConfig = new FieldConfig.Builder("clicks").withIndexes(indexes).build();
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(keyConfig), null);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true, Map.of());

    // Must not throw.
    StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null);
  }

  @Test
  public void testValidateRejectsIgnoredKeyAlsoDense()
      throws Exception {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"denseKeys\": [\"clicks\"], \"ignoredKeys\": [\"clicks\"]}", OpenStructIndexConfig.class);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true, Map.of());

    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null));
  }

  @Test
  public void testValidateRejectsIgnoredKeyAlsoHasValueFieldConfig()
      throws Exception {
    FieldConfig keyConfig = new FieldConfig.Builder("clicks").build();
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(keyConfig), null,
        null, Set.of("clicks"));
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true, Map.of());

    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null));
  }

  @Test
  public void testValidateRejectsIgnoredKeyAlsoDeclaredInSchema()
      throws Exception {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"ignoredKeys\": [\"clicks\"]}", OpenStructIndexConfig.class);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true,
        Map.of("clicks", new DimensionFieldSpec("clicks", FieldSpec.DataType.LONG, true)));

    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null));
  }

  @Test
  public void testValidateAllowsNonConflictingIgnoredKeys()
      throws Exception {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"denseKeys\": [\"clicks\"], \"ignoredKeys\": [\"debug\"]}", OpenStructIndexConfig.class);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true,
        Map.of("clicks", new DimensionFieldSpec("clicks", FieldSpec.DataType.LONG, true)));

    // Must not throw.
    StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null);
  }

  /// A declared MAP child key passes ColumnDataType conversion (used to be the only check) but has no DIMENSION
  /// case in FieldSpec#getDefaultNullValue, the call allocateKeyColumn() actually makes — must be rejected here
  /// instead of throwing uncaught on the first row ingested for the key.
  @Test
  public void testValidateRejectsMapChildKeyType()
      throws Exception {
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true,
        Map.of("nested", new ComplexFieldSpec("nested", FieldSpec.DataType.MAP, true, Map.of())));

    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null));
  }

  /// Same gap as MAP, for a nested OPEN_STRUCT child key.
  @Test
  public void testValidateRejectsOpenStructChildKeyType()
      throws Exception {
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true,
        Map.of("nested", new ComplexFieldSpec("nested", FieldSpec.DataType.OPEN_STRUCT, true, Map.of())));

    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null));
  }

  @Test
  public void testValidateSkipsIgnoredKeyChecksWhenIndexDisabled()
      throws Exception {
    OpenStructIndexConfig config = JsonUtils.stringToObject(
        "{\"disabled\": true, \"denseKeys\": [\"clicks\"], \"ignoredKeys\": [\"clicks\"]}",
        OpenStructIndexConfig.class);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    FieldSpec openStructSpec = new ComplexFieldSpec("payload", FieldSpec.DataType.OPEN_STRUCT, true, Map.of());

    // Must not throw - validation is skipped entirely when the index is disabled.
    StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSpec, null);
  }

  @Test(dataProvider = "codecSpecOpenStructConfigs")
  public void testTableValidationRejectsCodecSpecForMaterializedChildren(OpenStructIndexConfig openStructConfig,
      String expectedTarget) {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> validateTableWithOpenStruct(openStructConfig));
    assertEquals(exception.getMessage(), "OPEN_STRUCT column 'payload': codecSpec is not supported for "
        + expectedTarget + "; materialized keys always use a dictionary-encoded or LZ4 raw forward index");
  }

  /// Per-key settings that a materialized key would silently ignore. JSON uses single quotes for readability.
  @DataProvider(name = "ignoredPerKeySettings")
  public Object[][] ignoredPerKeySettings() {
    String unsupported = "OPEN_STRUCT column 'payload': %s is not supported for key 'clicks'; ";
    return new Object[][]{
        {"{'name': 'clicks', 'encodingType': 'RAW', 'compressionCodec': 'ZSTANDARD'}",
            String.format(unsupported, "compressionCodec")},
        {"{'name': 'clicks', 'indexTypes': ['INVERTED']}", String.format(unsupported, "indexTypes")},
        {"{'name': 'clicks', 'timestampConfig': {'granularities': ['DAY']}}",
            String.format(unsupported, "timestampConfig")},
        {"{'name': 'clicks', 'properties': {'forwardIndexDisabled': 'true'}}",
            String.format(unsupported, "properties")},
        {"{'name': 'clicks', 'tierOverwrites': {'hotTier': {'encodingType': 'RAW'}}}",
            String.format(unsupported, "tierOverwrites")},
        {rawKeyWithForward("'compressionCodec': 'ZSTANDARD'"),
            String.format(unsupported, "indexes.forward.compressionCodec")},
        {rawKeyWithForward("'chunkCompressionType': 'ZSTANDARD'"),
            String.format(unsupported, "indexes.forward.chunkCompressionType")},
        {"{'name': 'clicks', 'indexes': {'forward': {'dictIdCompressionType': 'MV_ENTRY_DICT'}}}",
            String.format(unsupported, "indexes.forward.dictIdCompressionType")},
        {rawKeyWithForward("'targetDocsPerChunk': 2048"),
            String.format(unsupported, "indexes.forward.targetDocsPerChunk")},
        {rawKeyWithForward("'targetMaxChunkSize': '512K'"),
            String.format(unsupported, "indexes.forward.targetMaxChunkSize")},
        {rawKeyWithForward("'rawIndexWriterVersion': 4"),
            String.format(unsupported, "indexes.forward.rawIndexWriterVersion")},
        {rawKeyWithForward("'deriveNumDocsPerChunk': true"),
            String.format(unsupported, "indexes.forward.deriveNumDocsPerChunk")},
        {rawKeyWithForward("'configs': {'key': 'value'}"), String.format(unsupported, "indexes.forward.configs")},
        {rawKeyWithForward("'disabled': true"), String.format(unsupported, "indexes.forward.disabled")},
        {"{'name': 'clicks', 'indexes': {'dictionary': {'onHeap': true}}}",
            String.format(unsupported, "indexes.dictionary.onHeap")},
        {"{'name': 'clicks', 'indexes': {'dictionary': {'useVarLengthDictionary': true}}}",
            String.format(unsupported, "indexes.dictionary.useVarLengthDictionary")},
        // A key is dictionary-encoded when its encodingType is DICTIONARY or its inverted index is enabled; the
        // per-key forward and dictionary configs may only restate that outcome.
        {"{'name': 'clicks', 'indexes': {'forward': {'encodingType': 'RAW'}}}",
            "OPEN_STRUCT column 'payload': indexes.forward.encodingType RAW of key 'clicks' does not match the "
                + "dictionary-encoded forward index it is built with; "},
        {"{'name': 'clicks', 'encodingType': 'RAW', 'indexes': {'inverted': {}, 'forward': {'encodingType': 'RAW'}}}",
            "OPEN_STRUCT column 'payload': indexes.forward.encodingType RAW of key 'clicks' does not match the "
                + "dictionary-encoded forward index it is built with; "},
        {"{'name': 'clicks', 'indexes': {'dictionary': {'disabled': true}}}",
            "OPEN_STRUCT column 'payload': indexes.dictionary of key 'clicks' disables the dictionary, but the key is "
                + "built with one; "},
        // An enabled inverted index requires a dictionary, so a RAW key with one is still built with a dictionary.
        {"{'name': 'clicks', 'encodingType': 'RAW', 'indexes': {'inverted': {}, 'dictionary': {'disabled': true}}}",
            "OPEN_STRUCT column 'payload': indexes.dictionary of key 'clicks' disables the dictionary, but the key is "
                + "built with one; "},
        {"{'name': 'clicks', 'indexes': {'forward': 'LZ4'}}",
            "OPEN_STRUCT column 'payload': indexes.forward of key 'clicks' must be a JSON object"},
        {"{'name': 'clicks', 'indexes': {'dictionary': true}}",
            "OPEN_STRUCT column 'payload': indexes.dictionary of key 'clicks' must be a JSON object"}
    };
  }

  @Test(dataProvider = "ignoredPerKeySettings")
  public void testValidateRejectsIgnoredPerKeySettings(String keyConfigJson, String expectedMessagePrefix)
      throws Exception {
    FieldConfig keyConfig = parseFieldConfig(keyConfigJson);
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> validatePerKeyConfigs(null, List.of(keyConfig)));
    assertTrue(exception.getMessage().startsWith(expectedMessagePrefix), exception.getMessage());
  }

  @Test
  public void testValidateNamesDefaultValueFieldConfigInPerKeyErrors()
      throws Exception {
    FieldConfig defaultConfig = parseFieldConfig("{'name': 'default', 'properties': {'forwardIndexDisabled': 'true'}}");
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> validatePerKeyConfigs(defaultConfig, null));
    assertEquals(exception.getMessage(), "OPEN_STRUCT column 'payload': properties is not supported for "
        + "defaultValueFieldConfig; configure the key through encodingType and 'indexes'");
  }

  /// Settings a materialized key honors, and forward/dictionary entries that only restate how it is built, pass.
  @Test
  public void testValidateAllowsHonoredAndRestatedPerKeySettings()
      throws Exception {
    FieldConfig rawKey = parseFieldConfig("{'name': 'clicks', 'encodingType': 'RAW', 'indexes': {"
        + "'forward': {'encodingType': 'RAW', 'disabled': false}, 'dictionary': {'disabled': true}, "
        + "'range': {'version': 2}, 'bloom': {'fpp': 0.01}, 'inverted': {'disabled': true}}}");
    // Empty and JSON-null values count as unset.
    FieldConfig dictionaryKey = parseFieldConfig("{'name': 'views', 'properties': {}, 'tierOverwrites': {}, "
        + "'indexes': {'forward': {'compressionCodec': null}, 'dictionary': {'disabled': false, 'onHeap': null}, "
        + "'inverted': {}}}");
    // The inverted index makes a RAW key dictionary-encoded, which its forward and dictionary configs may restate.
    FieldConfig rawInvertedKey = parseFieldConfig("{'name': 'tags', 'encodingType': 'RAW', 'indexes': {"
        + "'inverted': {}, 'forward': {'encodingType': 'DICTIONARY'}, 'dictionary': {}}}");
    FieldConfig defaultConfig = parseFieldConfig("{'name': 'default', 'encodingType': 'RAW', "
        + "'indexes': {'forward': null, 'dictionary': null, 'bloom': {}}}");
    validatePerKeyConfigs(defaultConfig, List.of(rawKey, dictionaryKey, rawInvertedKey));
  }

  /// `validatePerKeyFieldConfig` treats an enabled inverted index as the only per-key index that forces a
  /// dictionary; fail here if another vetted per-key index starts requiring one.
  @Test
  public void testInvertedIsTheOnlyVettedPerKeyIndexRequiringDictionary() {
    FieldSpec keySpec = new DimensionFieldSpec("clicks", FieldSpec.DataType.INT, true);
    for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
      String name = indexType.getPrettyName();
      if (OpenStructSupportedIndexes.ALLOWED_PRETTY_NAMES.contains(name)
          && !name.equals(StandardIndexes.dictionary().getPrettyName())) {
        assertEquals(requiresDictionary(indexType, keySpec), name.equals(StandardIndexes.inverted().getPrettyName()),
            name);
      }
    }
  }

  private static <C extends IndexConfig> boolean requiresDictionary(IndexType<C, ?, ?> indexType, FieldSpec spec) {
    return indexType.requiresDictionary(spec, indexType.getDefaultConfig());
  }

  /// The table-config path deserializes per-key configs from JSON; a round trip must neither lose a rejected setting
  /// nor turn an unset field into a rejected one.
  @Test
  public void testTableValidationChecksIgnoredPerKeySettings()
      throws Exception {
    FieldConfig honoredKey = parseFieldConfig("{'name': 'clicks', 'encodingType': 'RAW', "
        + "'indexes': {'forward': {'encodingType': 'RAW'}, 'range': {}}}");
    validateTableWithOpenStruct(
        new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(honoredKey), null, null, null, null));

    FieldConfig ignoredKey = parseFieldConfig("{'name': 'clicks', 'encodingType': 'RAW', "
        + "'indexes': {'forward': {'targetDocsPerChunk': 2048}}}");
    IllegalStateException exception = expectThrows(IllegalStateException.class, () -> validateTableWithOpenStruct(
        new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(ignoredKey), null, null, null, null)));
    assertEquals(exception.getMessage(), "OPEN_STRUCT column 'payload': indexes.forward.targetDocsPerChunk is not "
        + "supported for key 'clicks'; materialized keys always use a dictionary-encoded or LZ4 raw forward index");
  }

  private static FieldConfig parseFieldConfig(String singleQuotedJson)
      throws Exception {
    return JsonUtils.stringToObject(singleQuotedJson.replace('\'', '"'), FieldConfig.class);
  }

  private static String rawKeyWithForward(String forwardFields) {
    return "{'name': 'clicks', 'encodingType': 'RAW', 'indexes': {'forward': {" + forwardFields + "}}}";
  }

  /// Validates an OPEN_STRUCT column `payload` whose index config has the given per-key configs.
  private static void validatePerKeyConfigs(@Nullable FieldConfig defaultValueFieldConfig,
      @Nullable List<FieldConfig> valueFieldConfigs) {
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, defaultValueFieldConfig, -1, null, 0.5,
        valueFieldConfigs, null, null, null, null);
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.openStruct(), config).build();
    StandardIndexes.openStruct().validate(fieldIndexConfigs, openStructSchema().getFieldSpecFor("payload"), null);
  }

  /// Runs full table-config validation, which reads the OPEN_STRUCT config back from JSON.
  private static void validateTableWithOpenStruct(OpenStructIndexConfig openStructConfig) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set(StandardIndexes.openStruct().getPrettyName(), JsonUtils.objectToJsonNode(openStructConfig));
    FieldConfig parentFieldConfig = new FieldConfig.Builder("payload").withIndexes(indexes).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("openStructPerKeyTest")
        .setFieldConfigList(List.of(parentFieldConfig))
        .build();
    TableConfigUtils.validate(tableConfig, openStructSchema());
  }

  private static Schema openStructSchema() {
    return new Schema.SchemaBuilder().setSchemaName("openStructPerKeyTest").addOpenStruct("payload", Map.of()).build();
  }

  private static FieldConfig rawCodecSpecFieldConfig(String name) {
    ObjectNode forward = JsonUtils.newObjectNode();
    forward.put("encodingType", FieldConfig.EncodingType.RAW.name());
    forward.put("codecSpec", "LZ4");
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set(StandardIndexes.forward().getPrettyName(), forward);
    return new FieldConfig.Builder(name)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
  }
}
