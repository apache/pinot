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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.apache.avro.Schema.createUnion;
import static org.apache.pinot.integration.tests.GroupByOptionsIntegrationTest.toResultStr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@Test(suiteName = "CustomClusterIntegrationTest")
public class MultiColumnTextIndicesTest extends CustomDataQueryClusterIntegrationTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TABLE_NAME = "MultiColTextIndicesTest";

  private static final String DICT_TEXT_COL = "dict_skills";
  private static final String DICT_TEXT_COL_CASE_SENSITIVE = "dict_skills_case_sensitive";
  private static final String DICT_TEXT_COL_MV = "dict_skills_mv";
  private static final String DICT_TEXT_COL_CASE_SENSITIVE_MV = "dict_skills_case_sensitive_mv";

  private static final String NULLABLE_TEXT_COL = "nullable_skills";
  private static final String NULLABLE_TEXT_COL_MV = "nullable_skills_mv";

  private static final String TEXT_COL = "skills";
  private static final String TEXT_COL_CASE_SENSITIVE = "skills_case_sensitive";
  private static final String TEXT_COL_MV = "skills_mv";
  private static final String TEXT_COL_CASE_SENSITIVE_MV = "skills_case_sensitive_mv";
  private static final String TEXT_COL_NATIVE = "skills_native";
  private static final String TIME_COL = "millisSinceEpoch";

  private static final int NUM_SKILLS = 28;
  private static final int NUM_MATCHING_SKILLS = 4;
  protected static final int NUM_RECORDS = NUM_SKILLS * 1000;
  private static final int NUM_MATCHING_RECORDS = NUM_MATCHING_SKILLS * 1000;
  private static final int NUM_MATCHING_RECORDS_NATIVE = 7000;

  private static final String TEST_TEXT_COLUMN_QUERY_NATIVE =
      "SELECT COUNT(*) FROM %s WHERE TEXT_CONTAINS(skills_native, 'm.*') AND TEXT_CONTAINS(skills_native, "
          + "'spark')";

  private static final List<String> TEXT_COLUMNS =
      List.of(NULLABLE_TEXT_COL, NULLABLE_TEXT_COL_MV, TEXT_COL, TEXT_COL_CASE_SENSITIVE, TEXT_COL_MV,
          TEXT_COL_CASE_SENSITIVE_MV, DICT_TEXT_COL, DICT_TEXT_COL_CASE_SENSITIVE, DICT_TEXT_COL_MV,
          DICT_TEXT_COL_CASE_SENSITIVE_MV);

  @Override
  public String getTimeColumnName() {
    return TIME_COL;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return Collections.singletonList(TEXT_COL_NATIVE);
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return List.of(NULLABLE_TEXT_COL, NULLABLE_TEXT_COL_MV, TEXT_COL, TEXT_COL_CASE_SENSITIVE, TEXT_COL_MV,
        TEXT_COL_CASE_SENSITIVE_MV);
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  protected static MultiColumnTextIndexConfig getMultiColumnTextIndexConfig() {
    return new MultiColumnTextIndexConfig(TEXT_COLUMNS, null,
        Map.of(
            TEXT_COL_CASE_SENSITIVE, Map.of(FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY, "true"),
            TEXT_COL_CASE_SENSITIVE_MV, Map.of(FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY, "true"),
            DICT_TEXT_COL_CASE_SENSITIVE, Map.of(FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY, "true"),
            DICT_TEXT_COL_CASE_SENSITIVE_MV, Map.of(FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY, "true")
        )
    );
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    ObjectNode nativeIndex;
    try {
      nativeIndex = (ObjectNode) OBJECT_MAPPER.readTree("{ \"text\": { \"fst\": \"NATIVE\" } }");
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    FieldConfig nativeCol =
        new FieldConfig.Builder(TEXT_COL_NATIVE).withEncodingType(FieldConfig.EncodingType.DICTIONARY)
            .withIndexes(nativeIndex)
            .build();

    ForwardIndexConfig fwdCfg = ForwardIndexConfig.getDisabled();
    ObjectNode indexes = JsonUtils.newObjectNode();
    if (!isRealtimeTable()) { // we can't disable forward index for realtime table
      indexes.set("forward", fwdCfg.toJsonNode());
      indexes.set("inverted", JsonUtils.newObjectNode());
    }

    return Arrays.asList(
        new FieldConfig.Builder(NULLABLE_TEXT_COL).withEncodingType(FieldConfig.EncodingType.RAW).build(),
        new FieldConfig.Builder(NULLABLE_TEXT_COL_MV).withEncodingType(FieldConfig.EncodingType.RAW).build(),
        new FieldConfig.Builder(TEXT_COL).withEncodingType(FieldConfig.EncodingType.RAW).build(),
        new FieldConfig.Builder(TEXT_COL_CASE_SENSITIVE).withEncodingType(FieldConfig.EncodingType.RAW).build(),
        new FieldConfig.Builder(TEXT_COL_MV).withEncodingType(FieldConfig.EncodingType.RAW).build(),
        new FieldConfig.Builder(TEXT_COL_CASE_SENSITIVE_MV).withEncodingType(FieldConfig.EncodingType.RAW).build(),
        new FieldConfig.Builder(DICT_TEXT_COL).withEncodingType(FieldConfig.EncodingType.DICTIONARY)
            .withIndexes(indexes)
            .build(), // column missing forward index can still be indexed if there's dictionary and inverted index
        new FieldConfig.Builder(DICT_TEXT_COL_CASE_SENSITIVE).withEncodingType(FieldConfig.EncodingType.DICTIONARY)
            .build(),
        new FieldConfig.Builder(DICT_TEXT_COL_MV).withEncodingType(FieldConfig.EncodingType.DICTIONARY).build(),
        new FieldConfig.Builder(DICT_TEXT_COL_CASE_SENSITIVE_MV).withEncodingType(FieldConfig.EncodingType.DICTIONARY)
            .build(),
        nativeCol);
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField(NULLABLE_TEXT_COL, FieldSpec.DataType.STRING, field -> {
          field.setNullable(true);
          field.setDefaultNullValue(null);
        })
        .addDimensionField(NULLABLE_TEXT_COL_MV, FieldSpec.DataType.STRING, field -> {
          field.setNullable(true);
          field.setDefaultNullValue(null);
          field.setSingleValueField(false);
        })
        .addSingleValueDimension(TEXT_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TEXT_COL_CASE_SENSITIVE, FieldSpec.DataType.STRING)
        .addMultiValueDimension(TEXT_COL_MV, FieldSpec.DataType.STRING)
        .addMultiValueDimension(TEXT_COL_CASE_SENSITIVE_MV, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DICT_TEXT_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DICT_TEXT_COL_CASE_SENSITIVE, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DICT_TEXT_COL_MV, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DICT_TEXT_COL_CASE_SENSITIVE_MV, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TEXT_COL_NATIVE, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  @Override
  protected long getCountStarResult() {
    return NUM_RECORDS;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    // Read all skills from the skill file
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("data/text_search_data/skills.txt");
    assertNotNull(inputStream);
    List<String> skills = new ArrayList<>(NUM_SKILLS);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        skills.add(line);
      }
    }
    assertEquals(skills.size(), NUM_SKILLS);

    File avroFile = new File(_tempDir, "data.avro");
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(Arrays.asList(
        new Field(NULLABLE_TEXT_COL, createUnion(create(Type.NULL), create(Type.STRING)), null, null),
        new Field(NULLABLE_TEXT_COL_MV, createUnion(create(Type.NULL), createArray(create(Type.STRING))), null, null),
        new Field(TEXT_COL, create(Type.STRING), null, null),
        new Field(TEXT_COL_CASE_SENSITIVE, create(Type.STRING), null, null),
        new Field(TEXT_COL_MV, createArray(create(Type.STRING)), null, null),
        new Field(TEXT_COL_CASE_SENSITIVE_MV, createArray(create(Type.STRING)), null, null),
        new Field(DICT_TEXT_COL, create(Type.STRING), null, null),
        new Field(DICT_TEXT_COL_CASE_SENSITIVE, create(Type.STRING), null, null),
        new Field(DICT_TEXT_COL_MV, createArray(create(Type.STRING)), null, null),
        new Field(DICT_TEXT_COL_CASE_SENSITIVE_MV, createArray(create(Type.STRING)), null, null),
        new Field(TEXT_COL_NATIVE, create(Type.STRING), null, null),
        new Field(TIME_COL, create(Type.LONG), null, null)));

    List<String> valueList = List.of("value");

    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_RECORDS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(NULLABLE_TEXT_COL, (i & 1) == 1 ? null : "value");
        record.put(NULLABLE_TEXT_COL_MV, (i & 1) == 1 ? null : valueList);
        record.put(TEXT_COL, skills.get(i % NUM_SKILLS));
        record.put(TEXT_COL_CASE_SENSITIVE, skills.get(i % NUM_SKILLS));
        record.put(TEXT_COL_MV, List.of(skills.get(i % NUM_SKILLS), "" + i));
        record.put(TEXT_COL_CASE_SENSITIVE_MV, List.of(skills.get(i % NUM_SKILLS), "" + i));
        record.put(DICT_TEXT_COL, skills.get(i % NUM_SKILLS));
        record.put(DICT_TEXT_COL_CASE_SENSITIVE, skills.get(i % NUM_SKILLS));
        record.put(DICT_TEXT_COL_MV, List.of(skills.get(i % NUM_SKILLS), "" + i));
        record.put(DICT_TEXT_COL_CASE_SENSITIVE_MV, List.of(skills.get(i % NUM_SKILLS), "" + i));
        record.put(TEXT_COL_NATIVE, skills.get(i % NUM_SKILLS));
        record.put(TIME_COL, System.currentTimeMillis());
        fileWriter.append(record);
      }
    }
    return List.of(avroFile);
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setMultiColumnTextIndexConfig(getMultiColumnTextIndexConfig())
        .build();
  }

  @Test
  public void testRebuildIndex()
      throws IOException {
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();
    MultiColumnTextIndexConfig oldConfig = tableConfig.getIndexingConfig()
        .getMultiColumnTextIndexConfig();
    MultiColumnTextIndexConfig newConfig = new MultiColumnTextIndexConfig(oldConfig.getColumns(),
        Map.of(FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY, "yikes"), oldConfig.getPerColumnProperties());
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);
    updateTableConfig(tableConfig);

    reloadTable();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTextMatchTransformFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryTemplate =
        "SELECT TEXT_MATCH(skills, 'machine learning') as test, count(*) as cnt "
            + " FROM %s "
            + " GROUP BY TEXT_MATCH(skills, 'machine learning') "
            + " ORDER BY 1 ";
    String query = String.format(queryTemplate, getTableName());

    assertEquals(toResultStr(postQuery(query)),
        "\"test\"[\"BOOLEAN\"],\t\"cnt\"[\"LONG\"]\n"
            + "false,\t18000\n"
            + "true,\t10000");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTextSearchCountQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryTemplate = "SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, '\"machine learning\" AND spark')";

    String[] queries;

    if (useMultiStageQueryEngine) { //MSQE doesn't handle TEXT_MATCH against mv string column
      queries = new String[]{
          String.format(queryTemplate, getTableName(), TEXT_COL),
          String.format(queryTemplate, getTableName(), DICT_TEXT_COL),
      };
    } else {
      queries = new String[]{
          String.format(queryTemplate, getTableName(), TEXT_COL),
          String.format(queryTemplate, getTableName(), TEXT_COL_MV),
          String.format(queryTemplate, getTableName(), DICT_TEXT_COL),
          String.format(queryTemplate, getTableName(), DICT_TEXT_COL_MV)
      };
    }

    while (getCurrentCountStarResult() < NUM_RECORDS) {
      Thread.sleep(100);
    }

    //Lucene index on consuming segments to update the latest records
    TestUtils.waitForCondition(aVoid -> {
      try {
        boolean test = true;
        for (String query : queries) {
          test &= getQueryResult(query) == NUM_MATCHING_RECORDS;
        }
        return test;
      } catch (Exception e) {
        fail("Caught exception while getting text column query result: " + e.getMessage());
        return false;
      }
    }, 10_000L, "Failed to reach expected number of matching records");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTextSearchCountQueryNative(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Keep posting queries until all records are consumed
    long previousResult = 0;
    while (getCurrentCountStarResult() < NUM_RECORDS) {
      long result = getQueryResult(String.format(TEST_TEXT_COLUMN_QUERY_NATIVE, getTableName()));
      org.testng.Assert.assertTrue(result >= previousResult);
      previousResult = result;
      Thread.sleep(100);
    }

    assertEquals(getQueryResult(String.format(TEST_TEXT_COLUMN_QUERY_NATIVE, getTableName())),
        NUM_MATCHING_RECORDS_NATIVE);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTextSearchCountQueryCaseSensitive(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // wait until all rows are available in realtime index
    String query = "SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'Java')";
    while (getCurrentCountStarResult() < NUM_RECORDS) {
      if (getQueryResult(String.format(query, getTableName(), TEXT_COL_CASE_SENSITIVE)) == 12000) {
        break;
      } else {
        Thread.sleep(100);
      }
    }

    assertEquals(getQueryResult(String.format(query, getTableName(), TEXT_COL_CASE_SENSITIVE)), 12000);
    assertEquals(getQueryResult(String.format(query, getTableName(), DICT_TEXT_COL_CASE_SENSITIVE)), 12000);

    if (!useMultiStageQueryEngine) { // MSQE doesn't handle text_match against MV string
      assertEquals(getQueryResult(String.format(query, getTableName(), TEXT_COL_CASE_SENSITIVE_MV)), 12000);
      assertEquals(getQueryResult(String.format(query, getTableName(), DICT_TEXT_COL_CASE_SENSITIVE_MV)), 12000);
    }

    // Test case-sensitive match, all skills are 'Java' not 'java'
    String noResultsQuery = "SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'java')";
    assertEquals(getQueryResult(String.format(noResultsQuery, getTableName(), TEXT_COL_CASE_SENSITIVE)), 0);
    assertEquals(getQueryResult(String.format(noResultsQuery, getTableName(), DICT_TEXT_COL_CASE_SENSITIVE)), 0);

    if (!useMultiStageQueryEngine) {
      assertEquals(getQueryResult(String.format(noResultsQuery, getTableName(), TEXT_COL_CASE_SENSITIVE_MV)), 0);
      assertEquals(getQueryResult(String.format(noResultsQuery, getTableName(), DICT_TEXT_COL_CASE_SENSITIVE_MV)), 0);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryNullableColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // null avro columns are, by default, transformed into 'null' strings.
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE TEXT_MATCH(%s, '%s')";

    // wait until all rows are available in realtime index
    long start = System.currentTimeMillis();
    int rows = NUM_RECORDS / 2;
    while ((System.currentTimeMillis() - start) < 60000) {
      if (getQueryResult(String.format(query, NULLABLE_TEXT_COL, "null")) == rows) {
        break;
      } else {
        Thread.sleep(100);
      }
    }

    assertEquals(getQueryResult(String.format(query, NULLABLE_TEXT_COL, "null")), rows);
    assertEquals(getQueryResult(String.format(query, NULLABLE_TEXT_COL, "value")), rows);

    if (!useMultiStageQueryEngine) {
      assertEquals(getQueryResult(String.format(query, NULLABLE_TEXT_COL_MV, "null")), rows);
      assertEquals(getQueryResult(String.format(query, NULLABLE_TEXT_COL_MV, "value")), rows);
    }
  }

  @Test
  public void testExplainAskingServersShowsMultiColumnIndexBeingUsed()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    JsonNode jsonNode = postQuery("set explainAskingServers=true; "
        + "explain plan for "
        + "SELECT nullable_skills FROM " + getTableName()
        + " WHERE TEXT_MATCH(nullable_skills, 'test')");
    String plan = jsonNode.get("resultTable").get("rows").get(0).get(1).asText();

    String suffix = "              FilterTextIndex(predicate=[text_match(nullable_skills,'test')], "
        + "indexLookUp=[text_index], multiColumnIndex=[true], operator=[TEXT_MATCH])\n";
    assertTrue(plan.endsWith(suffix), plan + " doesn't end with: " + suffix);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTextMatchWithThirdParameter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test TEXT_MATCH with third parameter (options) for parser configuration
    String queryWithOptions =
        "SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'Java', 'parser=CLASSIC,DefaultOperator=AND')";

    // Wait until all rows are available in realtime index
    while (getCurrentCountStarResult() < NUM_RECORDS) {
      Thread.sleep(100);
    }

    // Test that the query with third parameter works
    long resultWithOptions = getQueryResult(String.format(queryWithOptions, getTableName(), TEXT_COL));
    assertTrue(resultWithOptions > 0, "TEXT_MATCH with third parameter should return results");

    // Test that the same query without third parameter returns the same result
    String queryWithoutOptions = "SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'Java')";
    long resultWithoutOptions = getQueryResult(String.format(queryWithoutOptions, getTableName(), TEXT_COL));
    assertEquals(resultWithOptions, resultWithoutOptions,
        "TEXT_MATCH with and without third parameter should return same results for basic queries");

    // Test with different parser options
    String queryWithStandardParser = "SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'Java', 'parser=STANDARD')";
    long resultWithStandardParser = getQueryResult(String.format(queryWithStandardParser, getTableName(), TEXT_COL));
    assertTrue(resultWithStandardParser > 0, "TEXT_MATCH with STANDARD parser should return results");
  }

  @Test(priority = 1)
  public void testRemoveColumnFromIndex()
      throws Exception {
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();
    MultiColumnTextIndexConfig oldConfig = tableConfig.getIndexingConfig()
        .getMultiColumnTextIndexConfig();

    ArrayList<String> newColumns = new ArrayList<>(TEXT_COLUMNS);
    newColumns.remove(TEXT_COL);

    MultiColumnTextIndexConfig newConfig =
        new MultiColumnTextIndexConfig(newColumns, oldConfig.getProperties(), oldConfig.getPerColumnProperties());
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);
    updateTableConfig(tableConfig);
    reloadTable();

    boolean columnDisabled = false;

    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < 60000) {
      JsonNode node =
          postQuery(String.format("SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'x')", getTableName(), TEXT_COL));
      if (node.get("exceptions") != null && !node.get("exceptions").isEmpty()) {
        columnDisabled = true;
        break;
      }

      Thread.sleep(100);
    }

    assertTrue(columnDisabled, "Column: " + TEXT_COL + " wasn't removed from multi column text index!");

    // check that index is present on other columns
    assertEquals(getQueryResult(String.format("SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'x')", getTableName(),
        TEXT_COL_CASE_SENSITIVE)), 0);
  }

  @Test(priority = 2)
  public void testRemoveIndex()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(null);
    updateTableConfig(tableConfig);
    reloadTable();

    boolean[] columnDisabled = new boolean[TEXT_COLUMNS.size()];
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < 60000) {
      boolean isComplete = false;
      for (int i = 0; i < columnDisabled.length; i++) {
        if (!columnDisabled[i]) {
          JsonNode node = postQuery(String.format("SELECT COUNT(*) FROM %s WHERE TEXT_MATCH(%s, 'x')", getTableName(),
              TEXT_COLUMNS.get(i)));
          if (node.get("exceptions") != null
              && node.get("exceptions").size() > 0) {
            columnDisabled[i] = true;
          } else {
            isComplete = true;
          }
        }
      }
      if (!isComplete) {
        break;
      }
      Thread.sleep(100);
    }

    for (int i = 0; i < columnDisabled.length; i++) {
      if (!columnDisabled[i]) {
        fail("Not all columns were removed from multi-column text index, flags:" + Arrays.toString(columnDisabled));
      }
    }
  }

  @Test(priority = Integer.MAX_VALUE)
  public void testCreateWithDuplicateColumnsReturnsError() {
    setUseMultiStageQueryEngine(false);
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();

    ArrayList<String> newColumns = new ArrayList<>();
    newColumns.add(TEXT_COL);
    newColumns.add(TEXT_COL);

    MultiColumnTextIndexConfig newConfig = new MultiColumnTextIndexConfig(newColumns, null, null);
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);

    try {
      updateTableConfig(tableConfig);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Cannot create TEXT index on duplicate columns: [skills]"), e.getMessage());
    }
  }

  @Test(priority = Integer.MAX_VALUE)
  public void testCreateWithNoColumnsReturnsError() {
    setUseMultiStageQueryEngine(false);
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();

    MultiColumnTextIndexConfig newConfig = new MultiColumnTextIndexConfig(Collections.emptyList(), null, null);
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);

    try {
      updateTableConfig(tableConfig);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Multi-column text index's list of columns can't be empty"), e.getMessage());
    }
  }

  @Test(priority = Integer.MAX_VALUE)
  public void testCreateWithWrongSharedPropertyKeyReturnsError() {
    setUseMultiStageQueryEngine(false);
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();

    ArrayList<String> newColumns = new ArrayList<>();
    newColumns.add(TEXT_COL);

    MultiColumnTextIndexConfig newConfig = new MultiColumnTextIndexConfig(newColumns, Map.of("XXX", "YYY"), null);
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);

    try {
      updateTableConfig(tableConfig);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Multi-column text index doesn't allow: XXX as shared property"),
          e.getMessage());
    }
  }

  @Test(priority = Integer.MAX_VALUE)
  public void testCreateWithWrongColumnPropertyKeyReturnsError() {
    setUseMultiStageQueryEngine(false);
    TableConfig tableConfig = isRealtimeTable() ? createRealtimeTableConfig(null) : createOfflineTableConfig();

    ArrayList<String> newColumns = new ArrayList<>();
    newColumns.add(TEXT_COL);

    MultiColumnTextIndexConfig newConfig =
        new MultiColumnTextIndexConfig(newColumns, null, Map.of(TEXT_COL, Map.of("XXX", "YYY")));
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);

    try {
      updateTableConfig(tableConfig);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Multi-column text index doesn't allow: XXX as property for column: skills"),
          e.getMessage());
    }

    newConfig = new MultiColumnTextIndexConfig(newColumns, null, Map.of("bogus", Map.of("XXX", "YYY")));
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(newConfig);

    try {
      updateTableConfig(tableConfig);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Multi-column text index per-column property refers to unknown column: bogus"),
          e.getMessage());
    }
  }

  private String reloadTable()
      throws IOException {
    if (isRealtimeTable()) {
      return reloadRealtimeTable(getTableName());
    } else {
      return reloadOfflineTable(getTableName());
    }
  }

  private long getQueryResult(String query)
      throws Exception {
    return postQuery(query).get("resultTable")
        .get("rows")
        .get(0)
        .get(0)
        .asLong();
  }
}
