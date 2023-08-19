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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class VectorTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "VectorTest";
  private static final String VECTOR_1 = "vector1";
  private static final String VECTOR_2 = "vector2";
  private static final String ZERO_VECTOR = "zeroVector";
  private static final int VECTOR_DIM_SIZE = 512;

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
            + "FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      double cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(0).asDouble();
      assertTrue(cosineDistance > 0.1 && cosineDistance < 0.4);
      double innerProduce = jsonNode.get("resultTable").get("rows").get(i).get(1).asDouble();
      assertTrue(innerProduce > 100 && innerProduce < 160);
      double l1Distance = jsonNode.get("resultTable").get("rows").get(i).get(2).asDouble();
      assertTrue(l1Distance > 140 && l1Distance < 210);
      double l2Distance = jsonNode.get("resultTable").get("rows").get(i).get(3).asDouble();
      assertTrue(l2Distance > 8 && l2Distance < 11);
      int vectorDimsVector1 = jsonNode.get("resultTable").get("rows").get(i).get(4).asInt();
      assertEquals(vectorDimsVector1, VECTOR_DIM_SIZE);
      int vectorDimsVector2 = jsonNode.get("resultTable").get("rows").get(i).get(5).asInt();
      assertEquals(vectorDimsVector2, VECTOR_DIM_SIZE);
      double vectorNormVector1 = jsonNode.get("resultTable").get("rows").get(i).get(6).asInt();
      assertTrue(vectorNormVector1 > 10 && vectorNormVector1 < 16);
      double vectorNormVector2 = jsonNode.get("resultTable").get("rows").get(i).get(7).asInt();
      assertTrue(vectorNormVector2 > 10 && vectorNormVector2 < 16);
      cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(8).asDouble();
      assertEquals(cosineDistance, Double.NaN);
      cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(9).asDouble();
      assertEquals(cosineDistance, 0.0);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueriesWithLiterals(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String zeroVectorStringLiteral = "ARRAY[0.0"
        + StringUtils.repeat(", 0.0", VECTOR_DIM_SIZE - 1)
        + "]";
    String oneVectorStringLiteral = "ARRAY[1.0"
        + StringUtils.repeat(", 1.0", VECTOR_DIM_SIZE - 1)
        + "]";
    String query =
        String.format("SELECT "
                + "cosineDistance(vector1, %s), "
                + "innerProduct(vector1, %s), "
                + "l1Distance(vector1, %s), "
                + "l2Distance(vector1, %s), "
                + "vectorDims(%s), "
                + "vectorNorm(%s) "
                + "FROM %s LIMIT %d",
            zeroVectorStringLiteral, zeroVectorStringLiteral, zeroVectorStringLiteral, zeroVectorStringLiteral,
            zeroVectorStringLiteral, zeroVectorStringLiteral, getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      double cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(0).asDouble();
      assertEquals(cosineDistance, Double.NaN);
      double innerProduce = jsonNode.get("resultTable").get("rows").get(i).get(1).asDouble();
      assertEquals(innerProduce, 0.0);
      double l1Distance = jsonNode.get("resultTable").get("rows").get(i).get(2).asDouble();
      assertTrue(l1Distance > 100 && l1Distance < 300);
      double l2Distance = jsonNode.get("resultTable").get("rows").get(i).get(3).asDouble();
      assertTrue(l2Distance > 10 && l2Distance < 16);
      int vectorDimsVector = jsonNode.get("resultTable").get("rows").get(i).get(4).asInt();
      assertEquals(vectorDimsVector, VECTOR_DIM_SIZE);
      double vectorNormVector = jsonNode.get("resultTable").get("rows").get(i).get(5).asInt();
      assertEquals(vectorNormVector, 0.0);
    }

    query =
        String.format("SELECT "
                + "cosineDistance(%s, %s), "
                + "cosineDistance(%s, %s, 0.0), "
                + "innerProduct(%s, %s), "
                + "l1Distance(%s, %s), "
                + "l2Distance(%s, %s)"
                + "FROM %s LIMIT 1",
            zeroVectorStringLiteral, oneVectorStringLiteral,
            zeroVectorStringLiteral, oneVectorStringLiteral,
            zeroVectorStringLiteral, oneVectorStringLiteral,
            zeroVectorStringLiteral, oneVectorStringLiteral,
            zeroVectorStringLiteral, oneVectorStringLiteral,
            getTableName());
    jsonNode = postQuery(query);
    double cosineDistance = jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble();
    assertEquals(cosineDistance, Double.NaN);
    cosineDistance = jsonNode.get("resultTable").get("rows").get(0).get(1).asDouble();
    assertEquals(cosineDistance, 0.0);
    double innerProduce = jsonNode.get("resultTable").get("rows").get(0).get(2).asDouble();
    assertEquals(innerProduce, 0.0);
    double l1Distance = jsonNode.get("resultTable").get("rows").get(0).get(3).asDouble();
    assertEquals(l1Distance, 512.0);
    double l2Distance = jsonNode.get("resultTable").get("rows").get(0).get(4).asDouble();
    assertEquals(l2Distance, 22.627416997969522);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(VECTOR_1, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(VECTOR_2, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(ZERO_VECTOR, FieldSpec.DataType.FLOAT)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(VECTOR_1, org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.FLOAT)), null,
            null),
        new org.apache.avro.Schema.Field(VECTOR_2, org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.FLOAT)), null,
            null),
        new org.apache.avro.Schema.Field(ZERO_VECTOR, org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.FLOAT)), null,
            null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
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
}
