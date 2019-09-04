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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MapTypeClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final long NUM_DOCS = 1_000L;

  private Schema _schema;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    _schema =
        new Schema.SchemaBuilder().setSchemaName(getTableName()).addMultiValueDimension("myMap__KEYS", DataType.STRING)
            .addMultiValueDimension("myMap__VALUES", DataType.STRING).build();

    // Create the tables
    ArrayList<String> invertedIndexColumns = Lists.newArrayList();
    addOfflineTable(getTableName(), null, null, null, null, null, SegmentVersion.v1, invertedIndexColumns, null, null,
        null, null);

    setUpSegmentsAndQueryGenerator();

    // Wait for all documents loaded
    waitForAllDocsLoaded(60_000);
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  protected void setUpSegmentsAndQueryGenerator()
      throws Exception {

    org.apache.avro.Schema mapAvroSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(Type.STRING));
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("myMap", mapAvroSchema, "", null));
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", "some desc", null, false);
    avroSchema.setFields(fields);

    DataFileWriter<GenericData.Record> recordWriter =
        new DataFileWriter<>(new GenericDatumWriter<GenericData.Record>(avroSchema));
    String parent = "/tmp/mapTest";
    File avroFile = new File(parent, "part-" + 0 + ".avro");
    avroFile.getParentFile().mkdirs();
    recordWriter.create(avroSchema, avroFile);
    for (int i = 0; i < NUM_DOCS; i++) {
      Map<String, String> map = new HashMap<>();
      map.put("k1", "value-k1-" + i);
      map.put("k2", "value-k2-" + i);
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("myMap", map);
      recordWriter.append(record);
    }
    recordWriter.close();

    // Unpack the Avro files
    List<File> avroFiles = Lists.newArrayList(avroFile);

    // Create and upload segments without star tree indexes from Avro data
    createAndUploadSegments(avroFiles, getTableName(), false);
  }

  private void createAndUploadSegments(List<File> avroFiles, String tableName, boolean createStarTreeIndex)
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName, createStarTreeIndex, null, null, _schema,
            executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(getTableName(), _tarDir);
  }

  @Test
  public void testQueries()
      throws Exception {

    //Selection Query
    String pqlQuery = "Select map_value(myMap__KEYS, 'k1', myMap__VALUES) from " + getTableName();
    JsonNode pinotResponse = postQuery(pqlQuery);
    ArrayNode selectionResults = (ArrayNode) pinotResponse.get("selectionResults").get("results");
    Assert.assertNotNull(selectionResults);
    Assert.assertTrue(selectionResults.size() > 0);
    for (int i = 0; i < selectionResults.size(); i++) {
      String value = selectionResults.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
    }

    //Filter Query
    pqlQuery = "Select map_value(myMap__KEYS, 'k1', myMap__VALUES) from " + getTableName()
        + "  where map_value(myMap__KEYS, 'k1', myMap__VALUES) = 'value-k1-0'";
    pinotResponse = postQuery(pqlQuery);
    selectionResults = (ArrayNode) pinotResponse.get("selectionResults").get("results");
    Assert.assertNotNull(selectionResults);
    Assert.assertTrue(selectionResults.size() > 0);
    for (int i = 0; i < selectionResults.size(); i++) {
      String value = selectionResults.get(i).get(0).textValue();
      Assert.assertEquals(value, "value-k1-0");
    }

    //selection order by
    pqlQuery = "Select map_value(myMap__KEYS, 'k1', myMap__VALUES) from " + getTableName()
        + " order by map_value(myMap__KEYS, 'k1', myMap__VALUES)";
    pinotResponse = postQuery(pqlQuery);
    selectionResults = (ArrayNode) pinotResponse.get("selectionResults").get("results");
    Assert.assertNotNull(selectionResults);
    Assert.assertTrue(selectionResults.size() > 0);
    for (int i = 0; i < selectionResults.size(); i++) {
      String value = selectionResults.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
    }

    //Group By Query
    pqlQuery = "Select count(*) from " + getTableName() + " group by map_value(myMap__KEYS, 'k1', myMap__VALUES)";
    pinotResponse = postQuery(pqlQuery);
    Assert.assertNotNull(pinotResponse.get("aggregationResults"));
    JsonNode groupByResult = pinotResponse.get("aggregationResults").get(0).get("groupByResult");
    Assert.assertNotNull(groupByResult);
    Assert.assertTrue(groupByResult.isArray());
    Assert.assertTrue(groupByResult.size() > 0);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
