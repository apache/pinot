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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.IntStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.Vector;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class VectorTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "VectorTest";
  private static final String VECTOR_1 = "vector1";
  private static final String VECTOR_2 = "vector2";
  private static final String ZERO_VECTOR = "zeroVector";
  private static final String VECTOR_1_NEW = "vector1New";
  private static final String VECTOR_2_NEW = "vector2New";
  private static final String ZERO_VECTOR_NEW = "zeroVectorNew";
  private static final String VECTOR_1_NORM = "vector1Norm";
  private static final String VECTOR_2_NORM = "vector2Norm";
  private static final String VECTORS_COSINE_DIST = "vectorsCosineDist";
  private static final String VECTORS_INNER_PRODUCT = "vectorsInnerProduct";
  private static final String VECTORS_L1_DIST = "vectorsL1Dist";
  private static final String VECTORS_L2_DIST = "vectorsL2Dist";
  private static final String VECTOR_ZERO_L1_DIST = "vectorZeroL1Dist";
  private static final String VECTOR_ZERO_L2_DIST = "vectorZeroL2Dist";
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
            + VECTORS_COSINE_DIST + ", "
            + "innerProduct(vector1, vector2), "
            + VECTORS_INNER_PRODUCT + ", "
            + "l1Distance(vector1, vector2), "
            + VECTORS_L1_DIST + ", "
            + "l2Distance(vector1, vector2), "
            + VECTORS_L2_DIST + ", "
            + "vectorDims(vector1), vectorDims(vector2), "
            + "vectorNorm(vector1), "
            + VECTOR_1_NORM + ", "
            + "vectorNorm(vector2), "
            + VECTOR_2_NORM + ", "
            + "cosineDistance(vector1, zeroVector), "
            + "cosineDistance(vector1, zeroVector, 0) "
            + "FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      double cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(0).asDouble();
      assertEquals(cosineDistance, jsonNode.get("resultTable").get("rows").get(i).get(1).asDouble());
      double innerProduce = jsonNode.get("resultTable").get("rows").get(i).get(2).asDouble();
      assertEquals(innerProduce, jsonNode.get("resultTable").get("rows").get(i).get(3).asDouble());
      double l1Distance = jsonNode.get("resultTable").get("rows").get(i).get(4).asDouble();
      assertEquals(l1Distance, jsonNode.get("resultTable").get("rows").get(i).get(5).asDouble());
      double l2Distance = jsonNode.get("resultTable").get("rows").get(i).get(6).asDouble();
      assertEquals(l2Distance, jsonNode.get("resultTable").get("rows").get(i).get(7).asDouble());
      int vectorDimsVector1 = jsonNode.get("resultTable").get("rows").get(i).get(8).asInt();
      assertEquals(vectorDimsVector1, VECTOR_DIM_SIZE);
      int vectorDimsVector2 = jsonNode.get("resultTable").get("rows").get(i).get(9).asInt();
      assertEquals(vectorDimsVector2, VECTOR_DIM_SIZE);
      double vectorNormVector1 = jsonNode.get("resultTable").get("rows").get(i).get(10).asDouble();
      assertEquals(vectorNormVector1, jsonNode.get("resultTable").get("rows").get(i).get(11).asDouble());
      double vectorNormVector2 = jsonNode.get("resultTable").get("rows").get(i).get(12).asDouble();
      assertEquals(vectorNormVector2, jsonNode.get("resultTable").get("rows").get(i).get(13).asDouble());
      cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(14).asDouble();
      assertEquals(cosineDistance, Double.NaN);
      cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(15).asDouble();
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
                + VECTOR_ZERO_L1_DIST + ", "
                + "l2Distance(vector1, %s), "
                + VECTOR_ZERO_L2_DIST + ", "
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
      assertEquals(l1Distance, jsonNode.get("resultTable").get("rows").get(i).get(3).asDouble());
      double l2Distance = jsonNode.get("resultTable").get("rows").get(i).get(4).asDouble();
      assertEquals(l2Distance, jsonNode.get("resultTable").get("rows").get(i).get(5).asDouble());
      int vectorDimsVector = jsonNode.get("resultTable").get("rows").get(i).get(6).asInt();
      assertEquals(vectorDimsVector, VECTOR_DIM_SIZE);
      double vectorNormVector = jsonNode.get("resultTable").get("rows").get(i).get(7).asDouble();
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

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueriesNew(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "cosineDistance(vector1New, vector2New), "
            + VECTORS_COSINE_DIST + ", "
            + "innerProduct(vector1New, vector2New), "
            + VECTORS_INNER_PRODUCT + ", "
            + "l1Distance(vector1New, vector2New), "
            + VECTORS_L1_DIST + ", "
            + "l2Distance(vector1New, vector2New), "
            + VECTORS_L2_DIST + ", "
            + "vectorDims(vector1New), vectorDims(vector2New), "
            + "vectorNorm(vector1New), "
            + VECTOR_1_NORM + ", "
            + "vectorNorm(vector2New), "
            + VECTOR_2_NORM + ", "
            + "cosineDistance(vector1New, zeroVectorNew), "
            + "cosineDistance(vector1New, zeroVectorNew, 0) "
            + "FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      double cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(0).asDouble();
      assertEquals(cosineDistance, jsonNode.get("resultTable").get("rows").get(i).get(1).asDouble());
      double innerProduce = jsonNode.get("resultTable").get("rows").get(i).get(2).asDouble();
      assertEquals(innerProduce, jsonNode.get("resultTable").get("rows").get(i).get(3).asDouble());
      double l1Distance = jsonNode.get("resultTable").get("rows").get(i).get(4).asDouble();
      assertEquals(l1Distance, jsonNode.get("resultTable").get("rows").get(i).get(5).asDouble());
      double l2Distance = jsonNode.get("resultTable").get("rows").get(i).get(6).asDouble();
      assertEquals(l2Distance, jsonNode.get("resultTable").get("rows").get(i).get(7).asDouble());
      int vectorDimsVector1 = jsonNode.get("resultTable").get("rows").get(i).get(8).asInt();
      assertEquals(vectorDimsVector1, VECTOR_DIM_SIZE);
      int vectorDimsVector2 = jsonNode.get("resultTable").get("rows").get(i).get(9).asInt();
      assertEquals(vectorDimsVector2, VECTOR_DIM_SIZE);
      double vectorNormVector1 = jsonNode.get("resultTable").get("rows").get(i).get(10).asDouble();
      assertEquals(vectorNormVector1, jsonNode.get("resultTable").get("rows").get(i).get(11).asDouble());
      double vectorNormVector2 = jsonNode.get("resultTable").get("rows").get(i).get(12).asDouble();
      assertEquals(vectorNormVector2, jsonNode.get("resultTable").get("rows").get(i).get(13).asDouble());
      cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(14).asDouble();
      assertEquals(cosineDistance, Double.NaN);
      cosineDistance = jsonNode.get("resultTable").get("rows").get(i).get(15).asDouble();
      assertEquals(cosineDistance, 0.0);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueriesWithLiteralsNew(boolean useMultiStageQueryEngine)
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
                + "cosineDistance(vector1New, %s), "
                + "innerProduct(vector1New, %s), "
                + "l1Distance(vector1New, %s), "
                + VECTOR_ZERO_L1_DIST + ", "
                + "l2Distance(vector1New, %s), "
                + VECTOR_ZERO_L2_DIST + ", "
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
      assertEquals(l1Distance, jsonNode.get("resultTable").get("rows").get(i).get(3).asDouble());
      double l2Distance = jsonNode.get("resultTable").get("rows").get(i).get(4).asDouble();
      assertEquals(l2Distance, jsonNode.get("resultTable").get("rows").get(i).get(5).asDouble());
      int vectorDimsVector = jsonNode.get("resultTable").get("rows").get(i).get(6).asInt();
      assertEquals(vectorDimsVector, VECTOR_DIM_SIZE);
      double vectorNormVector = jsonNode.get("resultTable").get("rows").get(i).get(7).asDouble();
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
        .addDimension(VECTOR_1_NEW, FieldSpec.DataType.VECTOR, FieldSpec.DataType.FLOAT, VECTOR_DIM_SIZE, null)
        .addDimension(VECTOR_2_NEW, FieldSpec.DataType.VECTOR, FieldSpec.DataType.FLOAT, VECTOR_DIM_SIZE, null)
        // ZERO_VECTOR_NEW is defined in schema, but not in avro, so we can test default null vector.
        .addDimension(ZERO_VECTOR_NEW, FieldSpec.DataType.VECTOR, FieldSpec.DataType.FLOAT, VECTOR_DIM_SIZE, null)
        .addSingleValueDimension(VECTOR_1_NORM, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTOR_2_NORM, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTORS_COSINE_DIST, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTORS_INNER_PRODUCT, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTORS_L1_DIST, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTORS_L2_DIST, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTOR_ZERO_L1_DIST, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(VECTOR_ZERO_L2_DIST, FieldSpec.DataType.DOUBLE)
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
            null),
        new org.apache.avro.Schema.Field(VECTOR_1_NEW, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(VECTOR_2_NEW, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        // Skip field definition for ZERO_VECTOR_NEW, let it be null to test null vector
        new org.apache.avro.Schema.Field(VECTOR_1_NORM,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTOR_2_NORM,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTORS_COSINE_DIST,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTORS_INNER_PRODUCT,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTORS_L1_DIST,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTORS_L2_DIST,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTOR_ZERO_L1_DIST,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(VECTOR_ZERO_L2_DIST,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);

        float[] vector1 = createRandomVector(VECTOR_DIM_SIZE);
        float[] vector2 = createRandomVector(VECTOR_DIM_SIZE);
        float[] zeroVector = createZeroVector(VECTOR_DIM_SIZE);

        record.put(VECTOR_1, convertToFloatCollection(vector1));
        record.put(VECTOR_2, convertToFloatCollection(vector2));
        record.put(ZERO_VECTOR, convertToFloatCollection(zeroVector));
        record.put(VECTOR_1_NEW, ByteBuffer.wrap(new Vector(VECTOR_DIM_SIZE, vector1).toBytes()));
        record.put(VECTOR_2_NEW, ByteBuffer.wrap(new Vector(VECTOR_DIM_SIZE, vector2).toBytes()));
        // Skip field definition for ZERO_VECTOR_NEW, let it be null to test null vector
        record.put(VECTOR_1_NORM, VectorFunctions.vectorNorm(vector1));
        record.put(VECTOR_2_NORM, VectorFunctions.vectorNorm(vector2));
        record.put(VECTORS_COSINE_DIST, VectorFunctions.cosineDistance(vector1, vector2));
        record.put(VECTORS_INNER_PRODUCT, VectorFunctions.innerProduct(vector1, vector2));
        record.put(VECTORS_L1_DIST, VectorFunctions.l1Distance(vector1, vector2));
        record.put(VECTORS_L2_DIST, VectorFunctions.l2Distance(vector1, vector2));
        record.put(VECTOR_ZERO_L1_DIST, VectorFunctions.l1Distance(vector1, zeroVector));
        record.put(VECTOR_ZERO_L2_DIST, VectorFunctions.l2Distance(vector1, zeroVector));

        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  private float[] createZeroVector(int vectorDimSize) {
    float[] vector = new float[vectorDimSize];
    IntStream.range(0, vectorDimSize).forEach(i -> vector[i] = 0.0f);
    return vector;
  }

  private float[] createRandomVector(int vectorDimSize) {
    float[] vector = new float[vectorDimSize];
    IntStream.range(0, vectorDimSize).forEach(i -> vector[i] = RandomUtils.nextFloat(0.0f, 1.0f));
    return vector;
  }

  private Collection<Float> convertToFloatCollection(float[] vector) {
    Collection<Float> vectorCollection = new ArrayList<>();
    for (float v : vector) {
      vectorCollection.add(v);
    }
    return vectorCollection;
  }
}
