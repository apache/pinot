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
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
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
import static org.testng.Assert.expectThrows;


public class OpenStructIndexTypeTest {

  @DataProvider(name = "codecSpecOpenStructConfigs")
  public Object[][] codecSpecOpenStructConfigs() {
    FieldConfig valueFieldConfig = rawCodecSpecFieldConfig("clicks");
    FieldConfig defaultValueFieldConfig = rawCodecSpecFieldConfig("default");
    return new Object[][]{
        {new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(valueFieldConfig), null), "clicks"},
        {new OpenStructIndexConfig(false, defaultValueFieldConfig, -1, null, 0.5, null, null), "default"}
    };
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

  @Test(dataProvider = "codecSpecOpenStructConfigs")
  public void testTableValidationRejectsCodecSpecForMaterializedChildren(OpenStructIndexConfig openStructConfig,
      String configuredKey) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set(StandardIndexes.openStruct().getPrettyName(), JsonUtils.objectToJsonNode(openStructConfig));
    FieldConfig parentFieldConfig = new FieldConfig.Builder("payload").withIndexes(indexes).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("openStructCodecSpecTest")
        .setFieldConfigList(List.of(parentFieldConfig))
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("openStructCodecSpecTest")
        .addOpenStruct("payload", Map.of())
        .build();

    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> TableConfigUtils.validate(tableConfig, schema));
    assertEquals(exception.getMessage(),
        "codecSpec is not supported for OPEN_STRUCT key: " + configuredKey);
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
