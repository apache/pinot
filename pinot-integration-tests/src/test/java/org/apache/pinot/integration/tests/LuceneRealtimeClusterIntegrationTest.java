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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.FieldConfig;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Cluster integration test for near realtime text search
 */
public class LuceneRealtimeClusterIntegrationTest extends BaseClusterIntegrationTestSet {

  private static final String TABLE_NAME = "mytable";
  private static final String SKILLS_TEXT_COL_NAME = "SKILLS_TEXT_COL";
  private static final String TIME_COL_NAME = "TIME_COL";
  private static final String INT_COL_NAME = "INT_COL";
  private static final int INT_BASE_VALUE = 10000;
  Schema _schema;

  @BeforeClass
  public void setUp()
      throws Exception {
    Pql2Compiler.ENABLE_TEXT_MATCH = true;
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SKILLS_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME_COL_NAME))
        .build();

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    File avroFile = createAvroFile();

    ExecutorService executor = Executors.newCachedThreadPool();

    // Push data into the Kafka topic
    pushAvroIntoKafka(Lists.newArrayList(avroFile), getKafkaTopic(), executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create Pinot table
    addSchema(_schema);
    List<FieldConfig> textIndexColumns = new ArrayList<>();
    FieldConfig fieldConfig = new FieldConfig(SKILLS_TEXT_COL_NAME, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null);
    textIndexColumns.add(fieldConfig);

    addRealtimeTable(TABLE_NAME, true, KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KafkaStarterUtils.DEFAULT_ZK_STR,
        getKafkaTopic(), getRealtimeSegmentFlushSize(), avroFile, null, null, TABLE_NAME,
        getBrokerTenant(), getServerTenant(), getLoadMode(), null, null,
        null, null, getTaskConfig(), getStreamConsumerFactoryClassName(),
        1, textIndexColumns);

    // just wait for 1sec for few docs to be loaded
    waitForDocsLoaded(2000L, false );
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(TABLE_NAME);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private File createAvroFile()
      throws Exception {
    // read the skills file
    URL resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/skills.txt");
    File skillFile = new File(resourceUrl.getFile());
    String[] skills = new String[100];
    int skillCount = 0;
    try (InputStream inputStream = new FileInputStream(skillFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        skills[skillCount++] = line;
      }
    }

    org.apache.avro.Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(_schema);
    DataFileWriter avroRecordWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
    String pathToAvroFile = _tempDir.getAbsolutePath() + "/" + "skills.avro";
    File outputAvroFile = new File(pathToAvroFile);
    avroRecordWriter.create(avroSchema, outputAvroFile);

    int counter = 0;
    Random random = new Random();
    // create Avro file with 100k documents
    // it will be later pushed to Kafka
    while (counter < 100000) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put(INT_COL_NAME, INT_BASE_VALUE + counter);
      if (counter >= skillCount) {
        int index = random.nextInt(skillCount);
        record.put(SKILLS_TEXT_COL_NAME, skills[index]);
      } else {
        record.put(SKILLS_TEXT_COL_NAME, skills[counter]);
      }
      record.put(TIME_COL_NAME, TimeUtils.getValidMinTimeMillis());
      avroRecordWriter.append(record);
      counter++;
    }

    avroRecordWriter.close();
    return outputAvroFile;
  }

  @Test
  public void testTextSearchCountQuery() throws Exception {
    String pqlQuery =
        "SELECT count(*) FROM " + TABLE_NAME + " WHERE text_match(SKILLS_TEXT_COL, '\"machine learning\" AND spark') LIMIT 1000000";
    int prevResult = 0;
    // run the same query 2000 times and see an increasing number of hits in the index and count(*) result
    for (int i = 0; i < 2000; i++) {
      JsonNode pinotResponse = postQuery(pqlQuery);
      Assert.assertTrue(pinotResponse.has("aggregationResults"));
      TextNode textNode = (TextNode)pinotResponse.get("aggregationResults").get(0).get("value");
      int result = Integer.valueOf(textNode.textValue());
      // TODO: see if this can be made more deterministic
      if (i >= 300) {
        Assert.assertTrue(result > 0);
        Assert.assertTrue(result >= prevResult);
      }
      prevResult = result;
    }
  }
}
