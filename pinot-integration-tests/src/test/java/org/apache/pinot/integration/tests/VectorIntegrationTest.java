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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
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
import static org.testng.Assert.assertTrue;


public class VectorIntegrationTest extends BaseClusterIntegrationTest {
  private static final String VECTOR_1 = "vector1";
  private static final String VECTOR_2 = "vector2";
  private static final String ZERO_VECTOR = "zeroVector";
  private static final int VECTOR_DIM_SIZE = 512;

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
        .addMultiValueDimension(VECTOR_1, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(VECTOR_2, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(ZERO_VECTOR, FieldSpec.DataType.FLOAT)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    // create & upload segments
    File avroFile = createAvroFile(getCountStarResult());
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    waitForAllDocsLoaded(60_000);
  }

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "cosineDistance(vector1, vector2), "
            + "innerProduct(vector1, vector2), "
            + "l1Distance(vector1, vector2), "
            + "l2Distance(vector1, vector2), "
            + "vectorDims(vector1), vectorDims(vector2), "
            + "vectorNorm(vector1), vectorNorm(vector2), "
            + "cosineDistance(vector1, zeroVector), "
            + "cosineDistance(vector1, zeroVector, 0) "
            + "FROM %s", DEFAULT_TABLE_NAME);
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      double cosineDistance = jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble();
      assertTrue(cosineDistance > 0.1 && cosineDistance < 0.4);
      double innerProduce = jsonNode.get("resultTable").get("rows").get(0).get(1).asDouble();
      assertTrue(innerProduce > 100 && innerProduce < 160);
      double l1Distance = jsonNode.get("resultTable").get("rows").get(0).get(2).asDouble();
      assertTrue(l1Distance > 150 && l1Distance < 200);
      double l2Distance = jsonNode.get("resultTable").get("rows").get(0).get(3).asDouble();
      assertTrue(l2Distance > 8 && l2Distance < 11);
      int vectorDimsVector1 = jsonNode.get("resultTable").get("rows").get(0).get(4).asInt();
      assertEquals(vectorDimsVector1, VECTOR_DIM_SIZE);
      int vectorDimsVector2 = jsonNode.get("resultTable").get("rows").get(0).get(5).asInt();
      assertEquals(vectorDimsVector2, VECTOR_DIM_SIZE);
      double vectorNormVector1 = jsonNode.get("resultTable").get("rows").get(0).get(6).asInt();
      assertTrue(vectorNormVector1 > 10 && vectorNormVector1 < 16);
      double vectorNormVector2 = jsonNode.get("resultTable").get("rows").get(0).get(7).asInt();
      assertTrue(vectorNormVector2 > 10 && vectorNormVector2 < 16);
      cosineDistance = jsonNode.get("resultTable").get("rows").get(0).get(8).asDouble();
      assertEquals(cosineDistance, Double.NaN);
      cosineDistance = jsonNode.get("resultTable").get("rows").get(0).get(9).asDouble();
      assertEquals(cosineDistance, 0.0);
    }
  }

  private File createAvroFile(long totalNumRecords)
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new Field(VECTOR_1, org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.FLOAT)), null,
            null),
        new Field(VECTOR_2, org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.FLOAT)), null,
            null),
        new Field(ZERO_VECTOR, org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.FLOAT)), null,
            null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < totalNumRecords; i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);

        Collection<Float> vector1 = createRandomVector(VECTOR_DIM_SIZE);
        Collection<Float> vector2 = createRandomVector(VECTOR_DIM_SIZE);
        Collection<Float> zeroVector = createZeroVector(VECTOR_DIM_SIZE);
        record.put(VECTOR_1, vector1);
        record.put(VECTOR_2, vector2);
        record.put(ZERO_VECTOR, zeroVector);

        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  private Collection<Float> createZeroVector(int vectorDimSize) {
    List<Float> vector = new ArrayList<>();
    for (int i = 0; i < vectorDimSize; i++) {
      vector.add(i, 0.0f);
    }
    return vector;
  }

  private Collection<Float> createRandomVector(int vectorDimSize) {
    List<Float> vector = new ArrayList<>();
    for (int i = 0; i < vectorDimSize; i++) {
      vector.add(i, RandomUtils.nextFloat(0.0f, 1.0f));
    }
    return vector;
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
