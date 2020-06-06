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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ThetaSketchIntegrationTest extends BaseClusterIntegrationTest {

  private static final String DIM_NAME = "dimName";
  private static final String DIM_VALUE = "dimValue";
  private static final String SHARD_ID = "shardId";
  private static final String THETA_SKETCH = "thetaSketchCol";

  @BeforeClass
  public void setup()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // create & upload schema AND table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(DIM_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_VALUE, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SHARD_ID, FieldSpec.DataType.INT)
        .addSingleValueDimension(THETA_SKETCH, FieldSpec.DataType.BYTES).build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    // create & upload segments
    File avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    waitForAllDocsLoaded(60_000);
  }

  @Override
  protected long getCountStarResult() {
    /*
    Uploaded table content:

    row#  dimName  dimValue  shardId  thetaSketchCol
    ----  =======  ========  =======  ==============
    1     country  US        1        ...
    2     country  CA        1        ...
    3     country  MX        1        ...
    4     title    Engineer  1        ...
    5     title    Manager   1        ...
    6     country  US        2        ...
    7     country  CA        2        ...
    8     country  MX        2        ...
    9     title    Engineer  2        ...
    10    title    Manager   2        ...
     */
    return 10;
  }

  @Test
  public void testThetaSketchQuery()
      throws Exception {
    /*
    Original data:

    Title     Country  Shard#1  Shard#2
    --------  -------  -------  -------
    Engineer  US       50       110
    Engineer  CA       60       120
    Engineer  MX       70       130
    Manager   US       80       140
    Manager   CA       90       150
    Manager   MX       100      160
     */

    // title = engineer
    String query = "select distinctCountThetaSketch(thetaSketchCol, '', "
        + "\"dimName = 'title'\", \"dimValue = 'Engineer'\", \"dimName = 'title' AND dimValue = 'Engineer'\") from "
        + DEFAULT_TABLE_NAME + " where dimName = 'title' AND dimValue = 'Engineer'";
    runAndAssert(query, 50 + 60 + 70 + 110 + 120 + 130);

    // title = manager
    query = "select distinctCountThetaSketch(thetaSketchCol, '', "
        + "\"dimName = 'title'\", \"dimValue='Manager'\", \"dimName = 'title' AND dimValue = 'Manager'\") from "
        + DEFAULT_TABLE_NAME + " where dimName = 'title' AND dimValue = 'Manager'";
    runAndAssert(query, 80 + 90 + 100 + 140 + 150 + 160);

    // country = US
    query = "select distinctCountThetaSketch(thetaSketchCol, '', "
        + "\"dimName = 'country'\", \"dimValue = 'US'\", \"dimName = 'country' AND dimValue = 'US'\") from "
        + DEFAULT_TABLE_NAME + " where dimName = 'country' AND dimValue = 'US'";
    runAndAssert(query, 50 + 80 + 110 + 140);

    // title = engineer AND country = US
    query = "select distinctCountThetaSketch(thetaSketchCol, '', "
        + "\"dimName = 'title'\", \"dimValue = 'Engineer'\", \"dimName = 'country'\", \"dimValue = 'US'\", "
        + "\"(dimName = 'title' AND dimValue = 'Engineer') AND (dimName = 'country' AND dimValue = 'US')\") from "
        + DEFAULT_TABLE_NAME
        + " where (dimName = 'title' AND dimValue = 'Engineer') OR (dimName = 'country' AND dimValue = 'US')";
    runAndAssert(query, 50 + 110);

    // title = manager OR country = MX
    query = "select distinctCountThetaSketch(thetaSketchCol, '', "
        + "\"dimName = 'title'\", \"dimValue='Manager'\", \"dimName = 'country'\", \"dimValue='MX'\", "
        + "\"(dimName = 'title' AND dimValue = 'Manager') OR (dimName = 'country' AND dimValue = 'MX')\") from "
        + DEFAULT_TABLE_NAME
        + " where (dimName = 'title' AND dimValue = 'Manager') OR (dimName = 'country' AND dimValue = 'MX')";
    runAndAssert(query, 70 + 80 + 90 + 100 + 130 + 140 + 150 + 160);
  }

  private void runAndAssert(String query, int expected)
      throws Exception {
    JsonNode jsonNode = postQuery(query);
    int actual = Integer.parseInt(jsonNode.get("aggregationResults").get(0).get("value").textValue());
    assertEquals(actual, expected);
  }

  private File createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(new Field(DIM_NAME, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(DIM_VALUE, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(SHARD_ID, org.apache.avro.Schema.create(Type.INT), null, null),
        new Field(THETA_SKETCH, org.apache.avro.Schema.create(Type.BYTES), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);

      int memberId = 0;
      int cardinality = 50;
      for (int shardId = 0; shardId < 2; shardId++) {

        // populate member data (memberId, title, country) for this shard id
        String[] allTitles = {"Engineer", "Manager"};
        String[] allCountries = {"US", "CA", "MX"};
        Map<Pair<String, String>, List<Integer>> titleCountryToMemberIds = new HashMap<>();
        for (String title : allTitles) {
          for (String country : allCountries) {
            List<Integer> memberIds =
                titleCountryToMemberIds.computeIfAbsent(ImmutablePair.of(title, country), key -> new ArrayList<>());
            for (int i = 0; i < cardinality; i++) {
              memberIds.add(memberId++);
            }
            cardinality += 10;
          }
        }

        // [title dimension] calculate theta sketches & add them to avro file
        for (String title : allTitles) {

          // calculate theta sketch
          UpdateSketch sketch = new UpdateSketchBuilder().build();
          titleCountryToMemberIds.forEach((titleCountry, memberIds) -> {
            if (title.equals(titleCountry.getLeft())) {
              memberIds.forEach(sketch::update);
            }
          });

          // create avro record
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(DIM_NAME, "title");
          record.put(DIM_VALUE, title);
          record.put(SHARD_ID, shardId);
          record.put(THETA_SKETCH, ByteBuffer.wrap(sketch.compact().toByteArray()));

          // add avro record to file
          fileWriter.append(record);
        }

        // [country dimension] calculate theta sketches & add them to avro file
        for (String country : allCountries) {

          // calculate theta sketch
          UpdateSketch sketch = new UpdateSketchBuilder().build();
          titleCountryToMemberIds.forEach((titleCountry, memberIds) -> {
            if (country.equals(titleCountry.getRight())) {
              memberIds.forEach(sketch::update);
            }
          });

          // create avro record
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(DIM_NAME, "country");
          record.put(DIM_VALUE, country);
          record.put(SHARD_ID, shardId);
          record.put(THETA_SKETCH, ByteBuffer.wrap(sketch.compact().toByteArray()));

          // add avro record to file
          fileWriter.append(record);
        }
      }
    }

    return avroFile;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
