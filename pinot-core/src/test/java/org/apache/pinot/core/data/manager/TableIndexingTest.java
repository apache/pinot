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
package org.apache.pinot.core.data.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentAllIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentDownloadThrottler;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
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
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


// Tests various combinations of field type, encoding, single/multi-value and index type
// and compares test case results with TableIndexingTest.csv
public class TableIndexingTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "TableIndexingTest");
  private static final String TABLE_NAME = "mytable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final SegmentOperationsThrottler SEGMENT_PREPROCESS_THROTTLER = new SegmentOperationsThrottler(
      new SegmentAllIndexPreprocessThrottler(2, 4, true), new SegmentStarTreePreprocessThrottler(1, 2, true),
      new SegmentDownloadThrottler(2, 4, true));
  public static final String COLUMN_NAME = "col";
  public static final String COLUMN_DAY_NAME = "$col$DAY";
  public static final String COLUMN_MONTH_NAME = "$col$MONTH";
  public static final String COLUMN_WEEK_NAME = "$col$WEEK";

  private final ArrayList<Schema> _schemas = new ArrayList<>();
  private TestCase[] _testCases;
  private Map<TestCase, TestCase> _testCaseMap;
  private final List<TestCase> _allResults = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    createSchemas();
    createTestCases();
    readExpectedResults();
  }

  private void createTestCases() {
    String[] indexTypes = {
        "timestamp_index", "bloom_filter", "fst_index", "h3_index", "inverted_index", "json_index",
        "native_text_index", "text_index", "range_index", "startree_index", "vector_index"
    };

    _testCases = new TestCase[_schemas.size() * indexTypes.length];
    _testCaseMap = new HashMap<>();

    for (int i = 0; i < _schemas.size(); i++) {
      for (int j = 0; j < indexTypes.length; j++) {
        TestCase testCase = new TestCase(_schemas.get(i).getSchemaName(), i, indexTypes[j]);
        _testCases[i * indexTypes.length + j] = testCase;
        _testCaseMap.put(testCase, testCase);
      }
    }
  }

  private void readExpectedResults()
      throws IOException {
    List<String> expected = readExpectedFromFile();
    // parse csv lines, e.g. INT;sv;raw;timestamp_index;true;
    for (int i = 1; i < expected.size(); i++) {
      String line = expected.get(i);
      if (line.isEmpty()) {
        continue;
      }

      int idx = line.indexOf(';');
      String dataType = line.substring(0, idx);
      int cardIdx = line.indexOf(';', idx + 1);
      String cardType = line.substring(idx + 1, cardIdx);
      int encIdx = line.indexOf(';', cardIdx + 1);
      String enc = line.substring(cardIdx + 1, encIdx);
      int indexIdx = line.indexOf(';', encIdx + 1);
      String indexType = line.substring(encIdx + 1, indexIdx);
      int resIdx = line.indexOf(';', indexIdx + 1);
      String result = line.substring(indexIdx + 1, resIdx);
      String error = line.substring(resIdx + 1);

      String schemaName = enc + "_" + cardType + "_" + dataType;
      TestCase testCase = _testCaseMap.get(new TestCase(schemaName, -1, indexType));
      if (testCase == null) {
        throw new AssertionError("Expected testCase not found: " + testCase);
      } else {
        testCase._expectedSuccess = Boolean.valueOf(result);
        testCase._expectedMessage = error;
      }
    }
  }

  protected void createSchemas() {
    for (DataType type : DataType.values()) {
      if (type == DataType.UNKNOWN || type == DataType.LIST || type == DataType.MAP || type == DataType.STRUCT) {
        continue;
      }

      for (String encoding : List.of("raw", "dict")) {
        if (type == DataType.BOOLEAN && "dict".equals(encoding)) {
          // pinot doesn't support dictionary encoding for boolean type
          continue;
        }

        if (type == DataType.TIMESTAMP) {
          //create separate tables for all data types
          _schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_sv_" + type.name())
              .addDateTime(COLUMN_NAME, type, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
              .build());

          _schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_mv_" + type.name())
              .addDateTime(COLUMN_NAME, type, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
              .build());
        } else {
          _schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_sv_" + type.name())
              .addSingleValueDimension(COLUMN_NAME, type)
              .build());
          //pinot doesn't support multi-values for big decimals, json and map
          if (type != DataType.BIG_DECIMAL && type != DataType.JSON) {
            _schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_mv_" + type.name())
                .addMultiValueDimension(COLUMN_NAME, type)
                .build());
          }
        }
      }
    }

    // add maps with all possible value data types
    for (DataType type : List.of(DataType.STRING, DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE)) {
      for (String encoding : List.of("raw", "dict")) {
        Map<String, FieldSpec> children = new HashMap<>();
        children.put("key", new DimensionFieldSpec("key", DataType.STRING, true));
        children.put("value",
            type == DataType.STRING ? new DimensionFieldSpec("value", type, true) : new MetricFieldSpec("value", type));

        _schemas.add(new Schema.SchemaBuilder().setSchemaName(encoding + "_map_" + type.name())
            .addComplex(COLUMN_NAME, DataType.MAP, children)
            .build());
      }
    }
  }

  static class TestCase {
    String _schemaName;
    String _indexType;

    int _schemaIndex;
    Throwable _error;

    String _expectedMessage;
    Boolean _expectedSuccess;

    public TestCase(String schemaName, int schemaIndex, String indexType) {
      _schemaName = schemaName;
      _indexType = indexType;
      _schemaIndex = schemaIndex;
    }

    @Override
    public String toString() {
      return _schemaName + "," + _indexType;
    }

    @Override
    public int hashCode() {
      return 31 * _schemaName.hashCode() + _indexType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TestCase)) {
        return false;
      }

      TestCase other = (TestCase) obj;
      return _schemaName.equals(other._schemaName) && _indexType.equals(other._indexType);
    }

    String getErrorMessage() {
      if (_error == null) {
        return null;
      } else {
        return _error.getMessage().replaceAll("\n", " ");
      }
    }
  }

  @Test(dataProvider = "fieldsAndIndexTypes")
  public void testAddIndex(TestCase testCase) {
    try {
      // create schema copy to avoid side effects between test cases
      // e.g. timestamp index creates additional virtual columns
      Schema schema = Schema.fromString(_schemas.get(testCase._schemaIndex).toPrettyJsonString());
      String indexType = testCase._indexType;
      String schemaName = schema.getSchemaName();

      FieldSpec field = schema.getFieldSpecFor(COLUMN_NAME);

      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(schema.getSchemaName())
          .setFieldConfigList(new ArrayList<>())
          .build();
      IndexingConfig idxCfg = tableConfig.getIndexingConfig();

      FieldConfig.EncodingType encoding =
          schemaName.startsWith("raw") ? FieldConfig.EncodingType.RAW : FieldConfig.EncodingType.DICTIONARY;

      List<FieldConfig.IndexType> indexTypes = new ArrayList<>();
      Map<String, String> properties = new HashMap<>();
      ObjectNode indexes = new ObjectNode(JsonNodeFactory.instance);
      TimestampConfig tstmpConfig = null;
      FieldConfig config = null; // table will be created from scratch for each run;

      switch (indexType) {
        case "bloom_filter":
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
          indexes.put("bloom", JsonUtils.newObjectNode());

          break;
        case "fst_index":
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
        case "h3_index":
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
          JsonNode resolutions = JsonUtils.stringToJsonNode("{\"resolutions\": [13, 5, 6]}");
          indexes.put("h3", resolutions);
          break;
        case "inverted_index":
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
        case "json_index":
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
        case "native_text_index":
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
        case "text_index":
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
        case "range_index":
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
        case "startree_index":
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
              new StarTreeIndexConfig(List.of(COLUMN_NAME), Collections.emptyList(), List.of("SUM__col"),
                  Collections.emptyList(), 1);
          idxCfg.getStarTreeIndexConfigs().add(stIdxCfg);

          break;
        case "timestamp_index":
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
              List.of(TimestampIndexGranularity.DAY, TimestampIndexGranularity.WEEK, TimestampIndexGranularity.MONTH));
          break;
        case "vector_index":
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

      config =
          new FieldConfig(field.getName(), encoding, null, indexTypes, null, tstmpConfig, indexes, properties, null);

      tableConfig.getFieldConfigList().add(config);

      //ImmutableSegmentDataManager segmentDataManager =
      Map<String, Map<String, Integer>> indexStats =
          createImmutableSegment(tableConfig, schema, indexType, generateRows(schema));

      if ("timestamp_index".equals(indexType)) {
        // this index is built on virtual columns, not on the timestamp one
        Assert.assertEquals(indexStats.get(COLUMN_DAY_NAME).get("range_index"), 1);
        Assert.assertEquals(indexStats.get(COLUMN_WEEK_NAME).get("range_index"), 1);
        Assert.assertEquals(indexStats.get(COLUMN_MONTH_NAME).get("range_index"), 1);
      } else {
        String expectedType;
        if ("native_text_index".equals(indexType)) {
          expectedType = "text_index";
        } else {
          expectedType = indexType;
        }

        Assert.assertEquals(indexStats.get(COLUMN_NAME).get(expectedType), 1);
      }
    } catch (Throwable t) {
      testCase._error = t;
      //throw t;
    } finally {
      _allResults.add(testCase);
    }

    if (testCase._expectedSuccess == null) {
      throw new AssertionError("No expected status found for test case: " + testCase);
    } else if (testCase._expectedSuccess && testCase._error != null) {
      throw new AssertionError("Expected success for test case: " + testCase + " but got error: " + testCase._error);
    } else if (!testCase._expectedSuccess && !testCase.getErrorMessage().equals(testCase._expectedMessage)) {
      throw new AssertionError(
          "Expected error: \"" + testCase._expectedMessage + "\" for test case: " + testCase + " but got: \""
              + testCase.getErrorMessage() + " \"");
    }
  }

  @AfterClass
  public void printSummary() {
    StringBuilder summary = generateSummary();
    if (Boolean.parseBoolean(System.getProperty("PRINT_SUMMARY"))) {
      System.out.println(summary);
    }
  }

  private @NotNull StringBuilder generateSummary() {
    StringBuilder summary = new StringBuilder();
    summary.append("data_type;cardinality;encoding;index_type;success;error\n");
    for (TestCase test : _allResults) {
      String tableName = test._schemaName;

      int fst = tableName.indexOf('_');
      int sec = tableName.lastIndexOf('_');
      String encoding = tableName.substring(0, fst);
      String cardinality = tableName.substring(fst + 1, sec);
      String type = tableName.substring(sec + 1);

      //@formatter:off
      summary.append(type).append(';')
          .append(cardinality).append(';')
          .append(encoding).append(';')
          .append(test._indexType).append(';')
          .append(test._error == null).append(';');
      //@formatter:on
      if (test._error != null) {
        summary.append(test._error.getMessage().replaceAll("\n", " "));
      }
      summary.append('\n');
    }
    return summary;
  }

  private @NotNull List<String> readExpectedFromFile()
      throws IOException {
    URL resource = getClass().getClassLoader().getResource("TableIndexingTest.csv");
    File expectedFile = new File(TestUtils.getFileFromResourceUrl(resource));
    return Files.readAllLines(expectedFile.toPath());
  }

  @DataProvider(name = "fieldsAndIndexTypes")
  public TestCase[] getFieldsAndIndexTypes() {
    return _testCases;
  }

  protected static List<GenericRow> generateRows(Schema schema) {
    ArrayList<GenericRow> rows = new ArrayList<>();
    Random random = new Random(0);
    FieldSpec col = schema.getFieldSpecFor(COLUMN_NAME);

    for (int i = 0; i < 10; i++) {
      GenericRow row = new GenericRow();
      row.putValue(COLUMN_NAME, getValue(col, random));
      rows.add(row);
    }

    return rows;
  }

  private static Object getValue(FieldSpec fieldSpec, Random r) {
    if (fieldSpec.isSingleValueField()) {
      switch (fieldSpec.getDataType()) {
        case INT:
          return r.nextInt();
        case LONG:
        case TIMESTAMP:
          return r.nextLong();
        case FLOAT:
          return r.nextFloat();
        case DOUBLE:
        case BIG_DECIMAL:
          return r.nextDouble();
        case BOOLEAN:
          return r.nextBoolean();
        case STRING:
          return "str" + r.nextInt();
        case BYTES:
          return ByteBuffer.wrap(("bytes" + r.nextInt()).getBytes());
        case JSON:
          return "{ \"field\": \"" + r.nextLong() + "\" }";
        case MAP:
          DataType valueType = ((ComplexFieldSpec) fieldSpec).getChildFieldSpecs().get("value").getDataType();
          Object value;
          switch (valueType) {
            case STRING:
              value = "str" + r.nextInt();
              break;
            case INT:
              value = r.nextInt();
              break;
            case LONG:
              value = r.nextLong();
              break;
            case FLOAT:
              value = r.nextFloat();
              break;
            case DOUBLE:
              value = r.nextDouble();
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
          return List.of(r.nextInt(), r.nextInt());
        case LONG:
        case TIMESTAMP:
          return List.of(r.nextLong(), r.nextLong());
        case FLOAT:
          return List.of(r.nextFloat(), r.nextFloat());
        case DOUBLE:
          return List.of(r.nextDouble(), r.nextDouble());
        case BOOLEAN:
          return List.of(r.nextBoolean(), r.nextBoolean());
        case STRING:
          return List.of("str" + r.nextInt(), "str" + r.nextInt());
        case BYTES:
          return List.of(ByteBuffer.wrap(("bytes" + r.nextInt()).getBytes()),
              ByteBuffer.wrap(("bytes" + r.nextInt()).getBytes()));
        default:
          throw new IllegalArgumentException("Unexpected data type " + fieldSpec.getDataType());
      }
    }
  }

  private static File createSegment(TableConfig tableConfig, Schema schema, String segmentName, List<GenericRow> rows)
      throws Exception {
    // load each segment in separate directory
    File dataDir = new File(TEMP_DIR, OFFLINE_TABLE_NAME + "_" + schema.getSchemaName());
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(dataDir.getAbsolutePath());
    config.setSegmentName(segmentName);
    config.setSegmentVersion(SegmentVersion.v3);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(dataDir, segmentName);
  }

  private static Map<String, Map<String, Integer>> createImmutableSegment(TableConfig tableConfig, Schema schema,
      String segmentName, List<GenericRow> rows)
      throws Exception {
    // validate here to get better error messages (segment creation doesn't check everything  )
    TableConfigUtils.validate(tableConfig, schema);
    ImmutableSegmentDataManager segmentDataManager = mock(ImmutableSegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    File indexDir = createSegment(tableConfig, schema, segmentName, rows);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, SEGMENT_PREPROCESS_THROTTLER);

    Map<String, Map<String, Integer>> map = new HashMap<>();
    addColumnIndexStats(segment, COLUMN_NAME, map);

    if (tableConfig.getFieldConfigList().get(0).getTimestampConfig() != null) {
      addColumnIndexStats(segment, COLUMN_DAY_NAME, map);
      addColumnIndexStats(segment, COLUMN_MONTH_NAME, map);
      addColumnIndexStats(segment, COLUMN_WEEK_NAME, map);
    }

    segment.destroy();

    return map;
  }

  private static void addColumnIndexStats(ImmutableSegment immutableSegment, String columnName,
      Map<String, Map<String, Integer>> map) {
    map.put(columnName, getColumnIndexStats(immutableSegment, columnName));
  }

  private static Map<String, Integer> getColumnIndexStats(ImmutableSegment segment, String columnName) {
    DataSource colDataSource = segment.getDataSource(columnName);
    HashMap<String, Integer> stats = new HashMap<>();
    IndexService.getInstance().getAllIndexes().forEach(idxType -> {
      int count = colDataSource.getIndex(idxType) != null ? 1 : 0;
      stats.merge(idxType.getId(), count, Integer::sum);
    });
    int starTrees = 0;
    if (segment.getStarTrees() != null) {
      for (StarTreeV2 stree : segment.getStarTrees()) {
        if (stree.getMetadata().getDimensionsSplitOrder().contains(columnName)) {
          starTrees++;
        }
      }
    }
    stats.put("startree_index", starTrees);
    return stats;
  }
}
