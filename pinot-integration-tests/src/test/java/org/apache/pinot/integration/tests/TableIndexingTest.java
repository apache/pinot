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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


// Try to create various index types for all data type/cardinality/encoding combinations and report outcome.
// NOTES: There is no multi-value type for BigDecimal, JSON or MAP.
// see PinotDataType.getPinotDataTypeForIngestion()
@Test(enabled = false)
public class TableIndexingTest extends BaseClusterIntegrationTestSet {

  private final ArrayList<String> _tableNames = new ArrayList<>();
  private final int _allDocs = 3000;
  private final SimpleDateFormat _format = new SimpleDateFormat("HH:mm:ss.SSS");
  private final static List<TestCase> _allResults = new ArrayList<>();

  static class TestCase {
    String _tableName;
    String _indexType;
    Throwable error;

    public TestCase(String tableName, String indexType) {
      this._tableName = tableName;
      this._indexType = indexType;
    }

    @Override
    public String toString() {
      return _tableName + "," + _indexType;
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    List<Schema> schemas = createSchemas();
    addSchemas(schemas);
    List<TableConfig> tableConfigs = createOfflineTableConfigs(schemas);
    addTableConfigs(tableConfigs);

    List<List<File>> avroFiles = createAvroFile(schemas);

    for (int i = 0; i < schemas.size(); i++) {
      // we've to use separate directories because segment tar files must exist for the duration of test
      File schemaSegmentDir = new File(_segmentDir, "schema_" + i);
      File schemaTarDir = new File(_tarDir, "schema_" + i);
      TestUtils.ensureDirectoriesExistAndEmpty(schemaSegmentDir, schemaTarDir);
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles.get(i), tableConfigs.get(i), schemas.get(i), 0,
          schemaSegmentDir, schemaTarDir);
      uploadSegments(schemas.get(i).getSchemaName(), schemaTarDir);
    }

    waitForAllDocsLoaded(schemas);
  }

  private void addTableConfigs(List<TableConfig> tableConfigs)
      throws IOException {
    for (TableConfig config : tableConfigs) {
      super.addTableConfig(config);
    }
  }

  private List<TableConfig> createOfflineTableConfigs(List<Schema> schemas) {
    return
        schemas.stream().map(s -> new TableConfigBuilder(TableType.OFFLINE)
                .setTableName(s.getSchemaName())
                .build())
            .collect(Collectors.toList());
  }

  private void waitForAllDocsLoaded(final List<Schema> schemas) {
    HashSet<String> incompleteTables = new HashSet<>();
    for (Schema schema : schemas) {
      incompleteTables.add(schema.getSchemaName());
    }
    List<String> toRemove = new ArrayList<>();

    TestUtils.waitForCondition(() -> {
          toRemove.clear();
          for (String table : incompleteTables) {
            if (getCurrentCountStarResult(table) == _allDocs) {
              toRemove.add(table);
            }
          }
          incompleteTables.removeAll(toRemove);
          return incompleteTables.isEmpty();
        }, 100L, 60_000L,
        "Failed to load " + _allDocs + " documents", true, Duration.ofMillis(60_000L / 10));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Ignore
  @Test(dataProvider = "fieldsAndIndexTypes")
  public void testAddIndex(TestCase testCase)
      throws Throwable {
    try {
      String schemaName = testCase._tableName;
      String indexType = testCase._indexType;

      System.out.println(
          _format.format(new Date()) + " Starting check for column: " + schemaName + " index type: " + indexType);
      Schema schema = getSchema(schemaName);
      FieldSpec field = schema.getFieldSpecFor("col");

      // These exceptions are thrown during segment reload (and not table config update) and appear in logs only
      // We're throwing them here to make test faster and improve output.
      if ("geo".equals(indexType) && field.getDataType() != DataType.BYTES) {
        throw new RuntimeException("Geo/H3 index can only be applied to column of BYTES data type!");
      }

      if ("json".equals(indexType) && ((field.getDataType() != DataType.STRING && field.getDataType() != DataType.JSON)
          || !field.isSingleValueField())) {
        throw new RuntimeException(
            "JSON index can only be applied to single value column of STRING or JSON data type!");
      }

      if ("vector".equals(indexType) && (field.getDataType() != DataType.FLOAT || field.isSingleValueField())) {
        throw new RuntimeException("VECTOR index can only be applied to Float Array columns");
      }

      if (("text".equals(indexType) || "native_text".equals(indexType)) && field.getDataType() != DataType.STRING) {
        throw new RuntimeException("Text index is currently only supported on STRING columns");
      }

      TableConfig tableConfig = getOfflineTableConfig(schemaName);
      IndexingConfig idxCfg = tableConfig.getIndexingConfig();

      FieldConfig.EncodingType encoding =
          field.getName().startsWith("raw") ? FieldConfig.EncodingType.RAW : FieldConfig.EncodingType.DICTIONARY;

      List<FieldConfig.IndexType> indexTypes = new ArrayList<>();
      Map<String, String> properties = new HashMap<>();
      ObjectNode indexes = new ObjectNode(JsonNodeFactory.instance);
      TimestampConfig tstmpConfig = null;
      FieldConfig config = getByName(tableConfig, field.getName());
      // ignore existing config and overwrite it, otherwise it will fail on earlier errors
      boolean isNew = config == null;

      switch (indexType) {
        case "bloom":
            /* bloom filter. Maybe we should call it bloom filter index to be consistent ?
            {
              "tableName": "somePinotTable",
              "fieldConfigList": [
                {
                  "name": "playerID",
                  "indexes": {
                    "bloom": {}
                  }
                },
                ...
              ],
              ...
            } */
          // no params
          indexes.put("bloom", new ObjectNode(JsonNodeFactory.instance));

          break;
        case "fst":
            /* fst index / text index
              "fieldConfigList":[
              {
                "name":"text_col_1",
                "encodingType":"DICTIONARY",
                "indexType":"FST"
                }
                ]
             */
          indexTypes.add(FieldConfig.IndexType.FST);
          break;
        case "geo":
            /* geospatial - requires dictionary be disabled
              {
              "fieldConfigList": [
                {
                  "name": "location_st_point",
                  "encodingType":"RAW", // this actually disables the dictionary
                  "indexes": {
                    "h3": {
                      "resolutions": [13, 5, 6]
                    }
                  }
                }
              ],
              ...
            }
             */
          ObjectNode resolutions = new ObjectNode(JsonNodeFactory.instance);
          ArrayNode res = new ArrayNode(JsonNodeFactory.instance);
          res.add(13).add(5).add(6);
          resolutions.put("resolutions", res);
          indexes.put("h3", resolutions);
          break;
        case "inverted":
            /* inverted index (bitmap or sorted). requires dictionary
               -> new:
                       {
                      "fieldConfigList": [
                        {
                          "name": "theColumnName",
                          "indexes": {
                            "inverted": {}
                          }
                        }
                      ],
                    }
                 old:
               -> "tableIndexConfig": {  "invertedIndexColumns": ["uuid"], */
          // no params, has to be dictionary
          indexes.put("inverted", new ObjectNode(JsonNodeFactory.instance));
          break;
        case "json":
            /* json index (string or json column), should be no-dictionary
               {
              "fieldConfigList": [
                {
                  "name": "person",
                  "indexes": {
                    "json": {}
                  }
                }
              ],
              ...
              } */
          // no params, should be no dictionary, only string or json
          indexes.put("json", new ObjectNode(JsonNodeFactory.instance));
          break;
        case "native_text":
            /* native text index
            "fieldConfigList":[
              {
                 "name":"text_col_1",
                 "encodingType":"RAW",
                 "indexTypes": ["TEXT"],
                 "properties":{"fstType":"native"}
              }
            ] */
          indexTypes.add(FieldConfig.IndexType.TEXT);
          properties.put("fstType", "native");
          break;
        case "text":
            /* text index
            "fieldConfigList":[
              {
                 "name":"text_col_1",
                 "encodingType":"RAW",
                 "indexTypes":["TEXT"]
              }
            ] */
          indexTypes.add(FieldConfig.IndexType.TEXT);
          break;
        case "range":
            /* range index (supported for dictionary encoded columns of any type as well as raw encoded columns
            of a numeric type)
                {
                  "tableIndexConfig": {
                      "rangeIndexColumns": [
                          "column_name",
                          ...
                      ],
                      ...
                  }
              }
             */
          if (idxCfg.getRangeIndexColumns() == null) {
            idxCfg.setRangeIndexColumns(new ArrayList<>());
          }
          idxCfg.getRangeIndexColumns().add(field.getName());
          break;
        case "startree":
             /* star tree
              "tableIndexConfig": {
                "starTreeIndexConfigs": [{
                  "dimensionsSplitOrder": [
                    "Country",
                    "Browser",
                    "Locale"
                  ],
                  "skipStarNodeCreationForDimensions": [
                  ],
                  "functionColumnPairs": [
                    "SUM__Impressions"
                  ],
                  "maxLeafRecords": 1
                }],
                ...
              }
             */
          if (idxCfg.getStarTreeIndexConfigs() == null) {
            idxCfg.setStarTreeIndexConfigs(new ArrayList<>());
          }
          StarTreeIndexConfig stIdxCfg =
              new StarTreeIndexConfig(List.of("col"), Collections.emptyList(), List.of("SUM__col"),
                  Collections.emptyList(), 1);
          idxCfg.getStarTreeIndexConfigs().add(stIdxCfg);

          break;
        case "timestamp":
            /* timestamp index
            {
            "fieldConfigList": [
              {
                "name": "ts",
                "timestampConfig": {
                  "granularities": [
                    "DAY",
                    "WEEK",
                    "MONTH"
                  ]
                }
              } */
          tstmpConfig = new TimestampConfig(
              List.of(TimestampIndexGranularity.DAY, TimestampIndexGranularity.WEEK,
                  TimestampIndexGranularity.MONTH));
          break;
        case "vector":
            /* vector
            "fieldConfigList": [
            {
              "encodingType": "RAW",
              "indexType": "VECTOR",
              "name": "embedding",
              "properties": {
                "vectorIndexType": "HNSW",
                "vectorDimension": 1536,
                "vectorDistanceFunction": "COSINE",
                "version": 1
              }
            } */
          indexTypes.add(FieldConfig.IndexType.VECTOR);
          properties.put("vectorIndexType", "HNSW");
          properties.put("vectorDimension", "1536");
          properties.put("vectorDistanceFunction", "COSINE");
          properties.put("version", "1");
          break;
        default:
          throw new IllegalArgumentException("Unexpected index type " + indexType);
      }

      config = new FieldConfig(field.getName(), encoding, null, indexTypes, null, tstmpConfig,
          indexes, properties, null);

      if (isNew) {
        tableConfig.getFieldConfigList().add(config);
      } else {
        tableConfig.getFieldConfigList().set(0, config);
      }

      updateConfig(tableConfig);
      reloadAllSegments(schemaName);

      TableConfig updateConfig = getOfflineTableConfig(schemaName);

      Assert.assertEquals(updateConfig, tableConfig);
    } catch (Throwable t) {
      testCase.error = t;
      throw t;
    } finally {
      _allResults.add(testCase);
    }
  }

  @AfterClass
  public void showSummary() {
    System.out.println("data_type;cardinality;encoding;index_type;success;error");
    for (TestCase test : _allResults) {
      int fst = test._tableName.indexOf('_');
      int sec = test._tableName.lastIndexOf('_');
      String encoding = test._tableName.substring(0, fst);
      String cardinality = test._tableName.substring(fst + 1, sec);
      String type = test._tableName.substring(sec + 1);

      System.out.print(type);
      System.out.print(';');
      System.out.print(cardinality);
      System.out.print(';');
      System.out.print(encoding);
      System.out.print(';');
      System.out.print(test._indexType);
      System.out.print(';');
      System.out.print(test.error == null);
      System.out.print(';');
      if (test.error != null) {
        System.out.print(test.error.getMessage());
      }
      System.out.println();
    }
  }

  private void updateConfig(TableConfig tableConfig)
      throws Throwable {
    try {
      updateTableConfig(tableConfig);
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        int beginIndex = cause.getMessage().indexOf("with reason:");
        if (beginIndex > -1) {
          beginIndex += "with reason:".length() + 1;
        } else {
          beginIndex = 0;
        }
        throw new RuntimeException(cause.getMessage().substring(beginIndex));
      } else {
        throw e;
      }
    }
  }

  @DataProvider(name = "fieldsAndIndexTypes")
  public TestCase[] getFieldsAndIndexTypes() {
    String[] indexTypes =
        {
            "timestamp", "bloom", "fst", "geo", "inverted", "json", "native_text", "text", "range", "startree",
            "vector"
        };

    TestCase[] result = new TestCase[_tableNames.size() * indexTypes.length];

    for (int i = 0; i < _tableNames.size(); i++) {
      for (int j = 0; j < indexTypes.length; j++) {
        result[i * indexTypes.length + j] = new TestCase(_tableNames.get(i), indexTypes[j]);
      }
    }

    return result;
  }

  private void reloadAllSegments(String tableName)
      throws IOException {
    // Try to refresh all the segments again with force download from the controller URI.
    String reloadJob = reloadTableAndValidateResponse(tableName, TableType.OFFLINE, true);
    PinotMetricName metricName =
        PinotMetricUtils.makePinotMetricName(ServerMetrics.class,
            "pinot.server." + tableName + "_OFFLINE.reloadFailures");
    final long startTime = System.currentTimeMillis();
    TestUtils.waitForCondition(aVoid -> {
      try {
        // check for failure via server metrics
        if (ServerMetrics.get().getMetricsRegistry().allMetrics().get(metricName) != null) {
          // add some delay to avoid previous reload error messages appearing in current method's log
          if ((System.currentTimeMillis() - startTime) > 5000) {
            throw new RuntimeException("Reload failed!");
          }
        }

        return isReloadJobCompleted(reloadJob);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 10_000L, "Failed to reload table with force download");
  }

  protected List<Schema> createSchemas() {
    List<Schema> schemas = new ArrayList<>();

    for (DataType type : DataType.values()) {
      if (type == DataType.UNKNOWN || type == DataType.LIST || type == DataType.MAP
          || type == DataType.STRUCT) {
        continue;
      }

      for (String encoding : List.of("raw", "dict")) {
        if (type == DataType.BOOLEAN && "dict".equals(encoding)) {
          // pinot doesn't support dictionary encoding for boolean type
          continue;
        }

        if (type == DataType.TIMESTAMP) {
          //create separate tables for all data types
          schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_sv_" + type.name())
              .addDateTime("col", type, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
              .build());

          schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_mv_" + type.name())
              .addDateTime("col", type, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
              .build());
        } else {
          schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_sv_" + type.name())
              .addSingleValueDimension("col", type).build());
          //pinot doesn't support multi-values for big decimals, json and map
          if (type != DataType.BIG_DECIMAL && type != DataType.JSON) {
            schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_mv_" + type.name())
                .addMultiValueDimension("col", type)
                .build());
          }
        }
      }
    }

    // add maps with all possible value data types
    for (DataType type : List.of(DataType.STRING, DataType.INT, DataType.LONG,
        DataType.FLOAT, DataType.DOUBLE)) {
      for (String encoding : List.of("raw", "dict")) {
        Map<String, FieldSpec> children = new HashMap<>();
        children.put("key", new DimensionFieldSpec("key", DataType.STRING, true));
        children.put("value", type == DataType.STRING ?
            new DimensionFieldSpec("value", type, true) :
            new MetricFieldSpec("value", type)
        );

        schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_map_" + type.name())
            .addComplex("col", DataType.MAP, children)
            .build());
      }
    }

    for (Schema schema : schemas) {
      _tableNames.add(schema.getSchemaName());
    }

    return schemas;
  }

  public void addSchemas(List<Schema> schemas)
      throws IOException {
    for (Schema schema : schemas) {
      super.addSchema(schema);
    }
  }

  private FieldConfig getByName(TableConfig config, String name) {
    if (config.getFieldConfigList() == null) {
      config.setFieldConfigList(new ArrayList<>());
      return null;
    }

    for (FieldConfig fc : config.getFieldConfigList()) {
      if (name.equals(fc.getName())) {
        return fc;
      }
    }
    return null;
  }

  @Override
  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .build();
  }

  private List<List<File>> createAvroFile(List<Schema> schemas)
      throws IOException {
    List<List<File>> result = new ArrayList<>();

    for (Schema schema : schemas) {
      // create avro schema
      org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
      List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        avroFields.add(new org.apache.avro.Schema.Field(fieldSpec.getName(), mapToAvroType(fieldSpec), null, null));
      }
      avroSchema.setFields(avroFields);

      ArrayList<File> files = new ArrayList<>();

      for (int file = 0; file < 3; file++) {
        // create avro file
        File avroFile = new File(_tempDir, schema.getSchemaName() + "_data_" + file + ".avro");
        try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(
            new GenericDatumWriter<>(avroSchema))) {
          fileWriter.create(avroSchema, avroFile);

          int numDocs = 1_000;
          Random random = new Random(0);
          for (int docId = 0; docId < numDocs; docId++) {
            GenericData.Record record = new GenericData.Record(avroSchema);

            for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
              record.put(fieldSpec.getName(), getValue(fieldSpec, random));
            }

            fileWriter.append(record);
          }
          files.add(avroFile);
        }
      }

      result.add(files);
    }
    return result;
  }

  private Object getValue(FieldSpec fieldSpec, Random random) {

    if (fieldSpec.isSingleValueField()) {
      switch (fieldSpec.getDataType()) {
        case INT:
          return random.nextInt();
        case LONG:
        case TIMESTAMP:
          return random.nextLong();
        case FLOAT:
          return random.nextFloat();
        case DOUBLE:
        case BIG_DECIMAL:
          return random.nextDouble();
        case BOOLEAN:
          return random.nextBoolean();
        case STRING:
          return "str" + random.nextInt();
        case BYTES:
          return ByteBuffer.wrap(("bytes" + random.nextInt()).getBytes());
        case JSON:
          return "{ \"field\": \"" + random.nextLong() + "\" }";
        case MAP:
          DataType valueType = ((ComplexFieldSpec) fieldSpec).getChildFieldSpecs().get("value").getDataType();
          Object value;
          switch (valueType) {
            case STRING:
              value = "str" + random.nextInt();
              break;
            case INT:
              value = random.nextInt();
              break;
            case LONG:
              value = random.nextLong();
              break;
            case FLOAT:
              value = random.nextFloat();
              break;
            case DOUBLE:
              value = random.nextDouble();
              break;
            default:
              throw new IllegalArgumentException("Unexpected map value type: " + valueType);
          }
          return Map.of("key", value);
        default:
          throw new IllegalArgumentException("Unexpected data type " + fieldSpec.getDataType());
      }
    } else {
      switch (fieldSpec.getDataType()) {
        case INT:
          return List.of(random.nextInt());
        case LONG:
        case TIMESTAMP:
          return List.of(random.nextLong());
        case FLOAT:
          return List.of(random.nextFloat());
        case DOUBLE:
          return List.of(random.nextDouble());
        case BOOLEAN:
          return List.of(random.nextBoolean());
        case STRING:
          return List.of("str" + random.nextInt());
        case BYTES:
          return List.of(ByteBuffer.wrap(("bytes" + random.nextInt()).getBytes()));
        default:
          throw new IllegalArgumentException("Unexpected data type " + fieldSpec.getDataType());
      }
    }
  }

  private org.apache.avro.Schema mapToAvroType(FieldSpec field) {
    if (field.getDataType() == DataType.MAP) {
      FieldSpec value = ((ComplexFieldSpec) field).getChildFieldSpec("value");
      return org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(mapToAvroType(value.getDataType())));
    }

    org.apache.avro.Schema.Type avroType = mapToAvroType(field.getDataType());
    if (field.isSingleValueField()) {
      return org.apache.avro.Schema.create(avroType);
    } else {
      return org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(avroType));
    }
  }

  private org.apache.avro.Schema.Type mapToAvroType(DataType type) {
    switch (type) {
      case INT:
        return org.apache.avro.Schema.Type.INT;
      case LONG:
        return org.apache.avro.Schema.Type.LONG;
      case FLOAT:
        return org.apache.avro.Schema.Type.FLOAT;
      case DOUBLE:
        return org.apache.avro.Schema.Type.DOUBLE;
      case BIG_DECIMAL:
        return org.apache.avro.Schema.Type.DOUBLE;
      case BOOLEAN:
        return org.apache.avro.Schema.Type.BOOLEAN;
      case TIMESTAMP:
        return org.apache.avro.Schema.Type.LONG;
      case STRING:
        return org.apache.avro.Schema.Type.STRING;
      case JSON:
        return org.apache.avro.Schema.Type.STRING;
      case BYTES:
        return org.apache.avro.Schema.Type.BYTES;
      default:
        throw new IllegalArgumentException("Unexpected type passed: " + type);
    }
  }
}
