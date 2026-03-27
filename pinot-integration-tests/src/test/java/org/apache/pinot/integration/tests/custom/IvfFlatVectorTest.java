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
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for IVF_FLAT vector index backend with VECTOR_SIMILARITY queries.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class IvfFlatVectorTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "IvfFlatVectorTest";
  private static final String VECTOR_COL = "embedding";
  private static final String VECTORS_L2_DIST = "embeddingL2Dist";
  private static final int VECTOR_DIM_SIZE = 128;
  // Use small nlist so that nprobe=nlist gives exact recall for validation
  private static final int NLIST = 8;

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setFieldConfigList(List.of(
            new FieldConfig.Builder(VECTOR_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
                .withEncodingType(FieldConfig.EncodingType.RAW)
                .withProperties(Map.of(
                    "vectorIndexType", "IVF_FLAT",
                    "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
                    "vectorDistanceFunction", "EUCLIDEAN",
                    "nlist", String.valueOf(NLIST),
                    "version", "1"))
                .build()
        ))
        .build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(VECTOR_COL, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(VECTORS_L2_DIST, FieldSpec.DataType.DOUBLE)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema floatArraySchema =
        org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(VECTOR_COL, floatArraySchema, null, null),
        new org.apache.avro.Schema.Field(VECTORS_L2_DIST,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null)
    ));

    // Pre-compute the query vector for l2 distance storage
    float[] queryVector = new float[VECTOR_DIM_SIZE];
    for (int i = 0; i < VECTOR_DIM_SIZE; i++) {
      queryVector[i] = 1.1f;
    }

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < getCountStarResult(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        float[] vector = createRandomVector(VECTOR_DIM_SIZE);
        record.put(VECTOR_COL, convertToFloatCollection(vector));
        record.put(VECTORS_L2_DIST, l2Distance(vector, queryVector));
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /**
   * Tests that VECTOR_SIMILARITY with IVF_FLAT returns correct results when nprobe=nlist (exact scan equivalent).
   * With nprobe=nlist, all inverted lists are probed so recall should be perfect.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testVectorSimilarityWithFullProbe(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVector = "ARRAY[1.1" + StringUtils.repeat(", 1.1", VECTOR_DIM_SIZE - 1) + "]";

    // Query using IVF_FLAT index with nprobe=nlist (full probe = exact results)
    String annQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d "
            + "OPTION(vectorNprobe=%d)",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK * 10,
        topK, NLIST);

    // Brute-force query for ground truth
    String exactQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(), topK);

    JsonNode annResult = postQuery(annQuery);
    JsonNode exactResult = postQuery(exactQuery);

    for (int i = 0; i < topK; i++) {
      double annDist = annResult.get("resultTable").get("rows").get(i).get(0).asDouble();
      double exactDist = exactResult.get("resultTable").get("rows").get(i).get(0).asDouble();
      assertEquals(annDist, exactDist,
          "With nprobe=nlist, IVF_FLAT should return the same results as exact scan");
    }
  }

  /**
   * Tests that VECTOR_SIMILARITY with default nprobe returns results (may not be exact).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testVectorSimilarityDefaultNprobe(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVector = "ARRAY[1.1" + StringUtils.repeat(", 1.1", VECTOR_DIM_SIZE - 1) + "]";

    String query = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK * 10, topK);

    JsonNode result = postQuery(query);
    int numRows = result.get("resultTable").get("rows").size();
    assertTrue(numRows > 0 && numRows <= topK, "Should return between 1 and topK results");

    // Verify distances are non-negative and ordered ascending
    double prevDist = -1;
    for (int i = 0; i < numRows; i++) {
      double dist = result.get("resultTable").get("rows").get(i).get(0).asDouble();
      assertTrue(dist >= 0, "L2 distance should be non-negative");
      assertTrue(dist >= prevDist, "Results should be ordered by distance ascending");
      prevDist = dist;
    }
  }

  /**
   * Tests that the pre-computed L2 distance column matches the computed distance.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testL2DistanceComputation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryVector = "ARRAY[1.1" + StringUtils.repeat(", 1.1", VECTOR_DIM_SIZE - 1) + "]";

    String query = String.format(
        "SELECT l2Distance(%s, %s), %s FROM %s LIMIT %d",
        VECTOR_COL, queryVector, VECTORS_L2_DIST, getTableName(), getCountStarResult());

    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < getCountStarResult(); i++) {
      double computedDist = jsonNode.get("resultTable").get("rows").get(i).get(0).asDouble();
      double storedDist = jsonNode.get("resultTable").get("rows").get(i).get(1).asDouble();
      assertEquals(computedDist, storedDist, 1e-5,
          "Computed L2 distance should match pre-computed value");
    }
  }

  private static double l2Distance(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private float[] createRandomVector(int vectorDimSize) {
    float[] vector = new float[vectorDimSize];
    for (int i = 0; i < vectorDimSize; i++) {
      vector[i] = RANDOM.nextFloat();
    }
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
