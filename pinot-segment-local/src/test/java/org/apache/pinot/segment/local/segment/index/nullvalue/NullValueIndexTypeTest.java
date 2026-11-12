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
package org.apache.pinot.segment.local.segment.index.nullvalue;

import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.NullValueVectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class NullValueIndexTypeTest {
  private static final NullValueVectorConfig DISABLED = new NullValueVectorConfig(true, false);
  private static final NullValueVectorConfig ENABLED = new NullValueVectorConfig(false, false);

  @DataProvider(name = "provideCases")
  public Object[][] provideCases() {
    return new Object[][]{
        // This is the semantic table, assuming a null bitmap buffer exists in the segment
        // enableColumnBasedNullHandling | table nullable | column nullable | expected index config
        new Object[]{false, false, false, DISABLED},
        new Object[]{false, false, true, DISABLED},
        new Object[]{false, true, false, ENABLED},
        new Object[]{false, true, true, ENABLED},
        new Object[]{true, false, false, DISABLED},
        new Object[]{true, false, true, ENABLED},
        new Object[]{true, true, false, DISABLED},
        new Object[]{true, true, true, ENABLED}
    };
  }

  @Test
  public void testConfigSerde()
      throws Exception {
    // The config itself round-trips (it is persisted as part of the table config).
    NullValueVectorConfig config = new NullValueVectorConfig(false, true);
    NullValueVectorConfig deserialized =
        JsonUtils.stringToObject(JsonUtils.objectToString(config), NullValueVectorConfig.class);
    assertEquals(deserialized, config);
    assertTrue(deserialized.isBackfill());
    assertTrue(deserialized.isEnabled());

    // The backfill opt-in survives a full table config round-trip and still resolves per column.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("intCol", DataType.INT)
        .addSingleValueDimension("otherCol", DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(new FieldConfig.Builder("intCol")
            .withIndexes(JsonUtils.stringToJsonNode("{\"null\": {\"backfill\": true}}"))
            .build()))
        .build();
    TableConfig reloaded = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);

    Map<String, FieldIndexConfigs> configsByCol =
        FieldIndexConfigsUtil.createIndexConfigsByColName(reloaded, schema);
    NullValueVectorConfig intColConfig = configsByCol.get("intCol").getConfig(StandardIndexes.nullValueVector());
    assertTrue(intColConfig.isEnabled());
    assertTrue(intColConfig.isBackfill());
    // A column without the opt-in is still enabled by table-level null handling, but not backfilled.
    NullValueVectorConfig otherColConfig = configsByCol.get("otherCol").getConfig(StandardIndexes.nullValueVector());
    assertTrue(otherColConfig.isEnabled());
    assertFalse(otherColConfig.isBackfill());
  }

  @Test
  public void testBackfillValidation() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    FieldIndexConfigs backfillOn = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.nullValueVector(), new NullValueVectorConfig(false, true))
        .build();

    // Scalar column opted into backfill is allowed.
    StandardIndexes.nullValueVector()
        .validate(backfillOn, new DimensionFieldSpec("scalar", DataType.INT, true), tableConfig);

    // MAP column opted into backfill is rejected at config time.
    FieldSpec mapFieldSpec = new ComplexFieldSpec("map", DataType.MAP, true, Map.of());
    assertThrows(IllegalStateException.class,
        () -> StandardIndexes.nullValueVector().validate(backfillOn, mapFieldSpec, tableConfig));

    // MAP column without the backfill opt-in is fine (validation is a no-op).
    FieldIndexConfigs backfillOff = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.nullValueVector(), new NullValueVectorConfig(false, false))
        .build();
    StandardIndexes.nullValueVector().validate(backfillOff, mapFieldSpec, tableConfig);
  }

  public static class ConfTest extends AbstractSerdeIndexContract {

    @Test(dataProvider = "provideCases", dataProviderClass = NullValueIndexTypeTest.class)
    public void isEnabledWhenNullable(boolean enableColumnBasedNullHandling, boolean tableNullable,
        boolean fieldNullable, NullValueVectorConfig expected) {
      _schema.setEnableColumnBasedNullHandling(enableColumnBasedNullHandling);
      _tableConfig.getIndexingConfig().setNullHandlingEnabled(tableNullable);

      FieldSpec fieldSpec = _schema.getFieldSpecFor("dimStr");
      fieldSpec.setNullable(fieldNullable);

      assertEquals(getActualConfig("dimStr", StandardIndexes.nullValueVector()), expected);
    }
  }
}
