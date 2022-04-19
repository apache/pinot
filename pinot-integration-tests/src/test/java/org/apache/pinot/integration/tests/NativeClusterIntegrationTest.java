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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class NativeClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final String TEXT_COLUMN_NAME = "skills";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private static final int NUM_SKILLS = 24;
  private static final int NUM_RECORDS = NUM_SKILLS * 1000;

  private static final String TEST_TEXT_COLUMN_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE skills CONTAINS 'lea.*ng'";

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN_NAME;
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return Collections.singletonList(TEXT_COLUMN_NAME);
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

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    return Collections.singletonList(
        new FieldConfig(TEXT_COLUMN_NAME, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null,
            propertiesMap));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Create the Avro file
    File avroFile = createAvroFile();

    // Create and upload the schema and table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(TEXT_COLUMN_NAME, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    addSchema(schema);
    addTableConfig(createRealtimeTableConfig(avroFile));

    // Push data into Kafka
    pushAvroIntoKafka(Collections.singletonList(avroFile));

    // Wait until the table is queryable
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResult() >= 0;
      } catch (Exception e) {
        return null;
      }
    }, 10_000L, "Failed to get COUNT(*) result");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private File createAvroFile()
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
    avroSchema.setFields(Arrays.asList(new org.apache.avro.Schema.Field(TEXT_COLUMN_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(TIME_COLUMN_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)));
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_RECORDS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(TEXT_COLUMN_NAME, skills.get(i % NUM_SKILLS));
        record.put(TIME_COLUMN_NAME, System.currentTimeMillis());
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  @Test
  public void testTextSearchCountQuery()
      throws Exception {
    // Keep posting queries until all records are consumed
    long previousResult = 0;
    while (getCurrentCountStarResult() < NUM_RECORDS) {
      long result = getTextColumnQueryResult();
      assertTrue(result >= previousResult);
      previousResult = result;
      Thread.sleep(100);
    }

    assertTrue(getTextColumnQueryResult() > 0);
  }

  private long getTextColumnQueryResult()
      throws Exception {
    return postQuery(TEST_TEXT_COLUMN_QUERY).get("aggregationResults").get(0).get("value").asLong();
  }
}
