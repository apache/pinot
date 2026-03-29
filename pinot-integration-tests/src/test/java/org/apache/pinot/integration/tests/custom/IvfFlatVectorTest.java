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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for IVF_FLAT vector index backend with VECTOR_SIMILARITY queries.
 *
 * <p>Follows the same pattern as {@link VectorTest} to ensure the IVF_FLAT backend works
 * end-to-end in a real Pinot cluster: schema creation, offline segment build with IVF_FLAT
 * vector index, segment upload, and query execution via VECTOR_SIMILARITY.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class IvfFlatVectorTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "IvfFlatVectorTest";
  private static final String VECTOR_COL = "embedding";
  private static final String VECTORS_L2_DIST = "embeddingL2Dist";
  private static final int VECTOR_DIM_SIZE = 128;
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
   * Tests that VECTOR_SIMILARITY with IVF_FLAT returns valid results.
   * Uses a large candidate count to maximize recall with the default nprobe.
   * Follows the same pattern as {@link VectorTest#testVectorSimilarity}.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testVectorSimilarity(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVector = "ARRAY[1.1" + StringUtils.repeat(", 1.1", VECTOR_DIM_SIZE - 1) + "]";

    // Use vectorSimilarity to get ANN candidates, then order by exact L2 distance
    String annQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK * 10, topK);

    // Brute-force query for ground truth
    String exactQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(), topK);

    JsonNode annResult = postQuery(annQuery);
    JsonNode exactResult = postQuery(exactQuery);

    // Verify ANN query returned results and has no exceptions
    assertNotNull(annResult.get("resultTable"), "ANN query should return a resultTable, got: " + annResult);
    assertNotNull(exactResult.get("resultTable"), "Exact query should return a resultTable, got: " + exactResult);

    JsonNode annRows = annResult.get("resultTable").get("rows");
    JsonNode exactRows = exactResult.get("resultTable").get("rows");
    assertTrue(annRows.size() > 0, "ANN query should return at least 1 result");
    assertTrue(exactRows.size() > 0, "Exact query should return at least 1 result");

    // Verify ANN results are ordered and have valid distances
    double prevDist = -1;
    for (int i = 0; i < annRows.size(); i++) {
      double dist = annRows.get(i).get(0).asDouble();
      assertTrue(dist >= 0, "L2 distance should be non-negative");
      assertTrue(dist >= prevDist, "Results should be ordered by distance ascending");
      prevDist = dist;
    }

    // Verify ANN top-1 distance is within a reasonable range of the exact top-1.
    // With default nprobe (4 out of nlist=8), recall is approximate — the ANN result
    // may not be the true nearest neighbor, but should be in the same ballpark.
    double annTopDist = annRows.get(0).get(0).asDouble();
    double exactTopDist = exactRows.get(0).get(0).asDouble();
    assertTrue(annTopDist <= exactTopDist * 1.5,
        "ANN top-1 distance (" + annTopDist + ") should be within 1.5x of exact top-1 ("
            + exactTopDist + ")");
  }

  /**
   * Tests that the pre-computed L2 distance column matches the computed l2Distance scalar function.
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
    assertNotNull(jsonNode.get("resultTable"), "Query should return a resultTable, got: " + jsonNode);

    JsonNode rows = jsonNode.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      double computedDist = rows.get(i).get(0).asDouble();
      double storedDist = rows.get(i).get(1).asDouble();
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
