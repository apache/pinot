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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class MapFieldTypeTest extends CustomDataQueryClusterIntegrationTest {

  // Default settings
  private static final int V1_DEFAULT_SELECTION_COUNT = 10;
  protected static final String DEFAULT_TABLE_NAME = "MapFieldTypeTest";
  private static final int NUM_DOCS = 1000;
  private static final String STRING_MAP_FIELD_NAME = "stringMap";
  private static final String INT_MAP_FIELD_NAME = "intMap";
  private int _setSelectionDefaultDocCount = 10;

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    ComplexFieldSpec stringMapFieldSpec = new ComplexFieldSpec(STRING_MAP_FIELD_NAME, FieldSpec.DataType.MAP, true,
        Map.of(
            ComplexFieldSpec.KEY_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.KEY_FIELD, FieldSpec.DataType.STRING, true),
            ComplexFieldSpec.VALUE_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.VALUE_FIELD, FieldSpec.DataType.STRING, true)
        ));
    ComplexFieldSpec intMapFieldSpec = new ComplexFieldSpec(INT_MAP_FIELD_NAME, FieldSpec.DataType.MAP, true,
        Map.of(
            ComplexFieldSpec.KEY_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.KEY_FIELD, FieldSpec.DataType.STRING, true),
            ComplexFieldSpec.VALUE_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.VALUE_FIELD, FieldSpec.DataType.INT, true)
        ));
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addField(stringMapFieldSpec)
        .addField(intMapFieldSpec)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    // Create table config with field configs
    TableConfig config =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setFieldConfigList(createFieldConfigs())
            .build();
    return config;
  }

  private List<FieldConfig> createFieldConfigs() {
    ObjectMapper objectMapper = new ObjectMapper();

    // Create the combined indexes map for STRING_MAP_FIELD_NAME
    Map<String, Object> stringIndexes = new HashMap<>();

    // Add JSON index with configs at root level
    Map<String, Object> stringJson = new HashMap<>();
    stringJson.put("maxLevels", 1);
    stringJson.put("excludeArray", false);
    stringJson.put("disableCrossArrayUnnest", true);
    stringIndexes.put("json", stringJson);

    JsonNode stringIndexesJson = objectMapper.valueToTree(stringIndexes);

    // Create the combined indexes map for INT_MAP_FIELD_NAME
    Map<String, Object> intIndexes = new HashMap<>();

    // Add JSON index for INT_MAP_FIELD_NAME
    Map<String, Object> intJson = new HashMap<>();
    intJson.put("maxLevels", 1);
    intJson.put("excludeArray", false);
    intJson.put("disableCrossArrayUnnest", true);
    intIndexes.put("json", intJson);

    JsonNode intIndexesJson = objectMapper.valueToTree(intIndexes);

    FieldConfig stringMapFieldConfig =
        new FieldConfig.Builder(STRING_MAP_FIELD_NAME).withIndexes(stringIndexesJson).build();

    FieldConfig intMapFieldConfig = new FieldConfig.Builder(INT_MAP_FIELD_NAME).withIndexes(intIndexesJson).build();
    return Arrays.asList(stringMapFieldConfig, intMapFieldConfig);
  }

  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema stringMapAvroSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    org.apache.avro.Schema intMapAvroSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
    List<org.apache.avro.Schema.Field> fields =
        Arrays.asList(
            new org.apache.avro.Schema.Field(STRING_MAP_FIELD_NAME, stringMapAvroSchema, null, null),
            new org.apache.avro.Schema.Field(INT_MAP_FIELD_NAME, intMapAvroSchema, null, null));
    avroSchema.setFields(fields);

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_DOCS; i++) {
        Map<String, String> stringMap = new HashMap<>();
        Map<String, Integer> intMap = new HashMap<>();
        for (int j = 0; j < i; j++) {
          String key = "k" + j;
          stringMap.put(key, "v" + i);
          intMap.put(key, i);
        }
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(STRING_MAP_FIELD_NAME, stringMap);
        record.put(INT_MAP_FIELD_NAME, intMap);
        fileWriter.append(record);
      }
    }
    return List.of(avroFile);
  }

  protected int getSelectionDefaultDocCount() {
    return _setSelectionDefaultDocCount;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Selection only
    String query = "SELECT * FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      JsonNode intMap = rows.get(i).get(0);
      JsonNode stringMap = rows.get(i).get(1);
      for (int j = 0; j < i; j++) {
        assertEquals(intMap.get("k" + j).intValue(), i);
        assertEquals(stringMap.get("k" + j).textValue(), "v" + i);
      }
    }
    // Selection only
    query = "SELECT stringMap['k0'], intMap['k0'] FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());

    assertEquals(rows.get(0).get(0).textValue(), "null");
    assertEquals(rows.get(0).get(1).intValue(), -2147483648);
    for (int i = 1; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), "v" + i);
      assertEquals(rows.get(i).get(1).intValue(), i);
    }

    // Selection order-by
    query = "SELECT intMap['k0'], intMap['k1'], stringMap['k0'], stringMap['k1'] FROM " + getTableName()
        + " ORDER BY intMap['k0']";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());

    assertEquals(rows.get(0).get(0).intValue(), -2147483648);
    assertEquals(rows.get(0).get(1).intValue(), -2147483648);
    assertEquals(rows.get(0).get(2).textValue(), "null");
    assertEquals(rows.get(1).get(0).intValue(), 1);
    assertEquals(rows.get(1).get(1).intValue(), -2147483648);
    assertEquals(rows.get(1).get(2).textValue(), "v1");
    for (int i = 2; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
      assertEquals(rows.get(i).get(1).intValue(), i);
      assertEquals(rows.get(i).get(2).textValue(), "v" + i);
      assertEquals(rows.get(i).get(3).textValue(), "v" + i);
    }

    // Aggregation only
    query = "SELECT MAX(intMap['k0']), MAX(intMap['k1']) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    assertEquals(pinotResponse.get("resultTable").get("rows").get(0).get(0).intValue(), NUM_DOCS - 1);
    assertEquals(pinotResponse.get("resultTable").get("rows").get(0).get(1).intValue(), NUM_DOCS - 1);

    // Aggregation group-by
    query = "SELECT stringMap['k0'] AS key, MIN(intMap['k0']) AS value FROM " + getTableName()
        + " GROUP BY key ORDER BY value";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    assertEquals(rows.get(0).get(0).textValue(), "null");
    assertEquals(rows.get(0).get(1).intValue(), Integer.MIN_VALUE);
    for (int i = 1; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), "v" + i);
      assertEquals(rows.get(i).get(1).intValue(), i);
    }

    // Filter
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] = 'v25'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).textValue(), "v25");

    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['k1'] = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).intValue(), 25);

    // Filter on non-existing key
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['kk'] = 'v25'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['kk'] = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Select non-existing key
    query = "SELECT stringMap['kkk'], intMap['kkk'] FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), "null");
      assertEquals(rows.get(i).get(1).intValue(), Integer.MIN_VALUE);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNotEqPredicate(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test NOT_EQ predicate with string map
    String query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] != 'v25'";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    // All records except the one with k1 = 'v25' should be returned
    // Verify that none of the returned rows have k1 = 'v25'
    for (int i = 0; i < rows.size(); i++) {
      assertNotEquals(rows.get(i).get(0).textValue(), "v25");
    }

    // Test NOT_EQ predicate with int map
    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['k1'] != 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    // All records except the one with k1 = 25 should be returned
    // Verify that none of the returned rows have k1 = 25
    for (int i = 0; i < rows.size(); i++) {
      assertNotEquals(rows.get(i).get(0).textValue(), "v25");
    }

    // Test NOT_EQ predicate with non-existing key
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['kk'] != 'v25'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    // All records should be returned since the key doesn't exist
    // assertEquals(rows.size(), 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInPredicate(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test IN predicate with string map
    String query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] IN ('v25', 'v26')";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    // Only records with k1 = 'v25' or 'v26' should be returned
    assertEquals(rows.size(), 2);

    // Verify the returned values
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assert (value.equals("v25") || value.equals("v26"));
    }

    // Test IN predicate with int map
    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['k1'] IN (25, 26)";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    // Only records with k1 = 25 or 26 should be returned
    assertEquals(rows.size(), 2);

    // Verify the returned values
    for (int i = 0; i < rows.size(); i++) {
      int value = rows.get(i).get(0).intValue();
      assert (value == 25 || value == 26);
    }

    // Test IN predicate with non-existing key
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['kk'] IN ('v25', 'v26')";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    // No records should be returned since the key doesn't exist
    assertEquals(rows.size(), 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNotInPredicate(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test NOT IN predicate with string map
    String query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] NOT IN ('v25', 'v26')";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");

    // Verify the returned values
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assert (!value.equals("v25") && !value.equals("v26"));
    }

    // Test NOT IN predicate with int map
    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['k1'] NOT IN (25, 26)";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");

    // Verify the returned values
    for (int i = 0; i < rows.size(); i++) {
      int value = rows.get(i).get(0).intValue();
      assert (value != 25 && value != 26);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testIsNullPredicate(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test IS_NULL predicate with string map
    String query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] IS NULL";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Test IS_NULL predicate with non-existing key
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['kk'] IS NULL";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Test IS_NOT_NULL predicate with string map
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] IS NOT NULL";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    // All records should be returned since all records have k1 defined
    if (useMultiStageQueryEngine) {
      assertEquals(rows.size(), getSelectionDefaultDocCount());
    } else {
      //First Two rows are null for k1
      assertEquals(rows.size(), getSelectionDefaultDocCount());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStringWithQuotes(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test string with single quote in map value
    String query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] = 'v25''s value'";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);

    // Test string with multiple single quotes
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1'] = 'v25''s ''quoted'' value'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);

    // Test IN predicate with quoted strings
    query = "SELECT stringMap['k2'] FROM " + getTableName()
        + " WHERE stringMap['k1'] IN ('v25''s value', 'v26''s value')";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
  }

  @Override
  protected void setUseMultiStageQueryEngine(boolean useMultiStageQueryEngine) {
    super.setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    _setSelectionDefaultDocCount = useMultiStageQueryEngine ? NUM_DOCS : V1_DEFAULT_SELECTION_COUNT;
  }
}
