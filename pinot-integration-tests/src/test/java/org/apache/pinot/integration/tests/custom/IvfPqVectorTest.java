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
 * Integration test for the IVF_PQ vector backend with query-option and fallback coverage.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class IvfPqVectorTest extends CustomDataQueryClusterIntegrationTest {

  protected static final String TABLE_NAME = "IvfPqVectorTest";
  protected static final String VECTOR_COL = "embedding";
  protected static final String VECTOR_L2_DIST_COL = "embeddingL2Dist";
  protected static final int VECTOR_DIMENSION = 128;
  protected static final int NLIST = 8;
  protected static final int PQ_M = 8;
  protected static final int PQ_NBITS = 8;
  protected static final int TRAIN_SAMPLE_SIZE = 1000;

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return List.of(new FieldConfig.Builder(VECTOR_COL)
        .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withProperties(Map.of(
            "vectorIndexType", "IVF_PQ",
            "vectorDimension", String.valueOf(VECTOR_DIMENSION),
            "vectorDistanceFunction", "EUCLIDEAN",
            "version", "1",
            "nlist", String.valueOf(NLIST),
            "pqM", String.valueOf(PQ_M),
            "pqNbits", String.valueOf(PQ_NBITS),
            "trainSampleSize", String.valueOf(TRAIN_SAMPLE_SIZE),
            "trainingSeed", "7"))
        .build());
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setFieldConfigList(getFieldConfigs())
        .build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(VECTOR_COL, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(VECTOR_L2_DIST_COL, FieldSpec.DataType.DOUBLE)
        .addDateTime(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
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
        new org.apache.avro.Schema.Field(VECTOR_L2_DIST_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)
    ));

    float[] queryVector = getQueryVector();
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < getCountStarResult(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        float[] vector = createRandomVector(VECTOR_DIMENSION);
        record.put(VECTOR_COL, convertToFloatCollection(vector));
        record.put(VECTOR_L2_DIST_COL, l2Distance(vector, queryVector));
        record.put(TIMESTAMP_FIELD_NAME, 1_700_000_000_000L + i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testVectorSimilarityWithDefaultExactRerank(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVectorLiteral = getQueryVectorLiteral();

    String annQuery = String.format(
        "set vectorNprobe=%d; set vectorMaxCandidates=%d; "
            + "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d",
        NLIST, getCountStarResult(),
        VECTOR_COL, queryVectorLiteral, getTableName(),
        VECTOR_COL, queryVectorLiteral, topK, topK);
    String exactQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVectorLiteral, getTableName(), topK);

    JsonNode annResult = postQuery(annQuery);
    JsonNode exactResult = postQuery(exactQuery);
    assertNotNull(annResult.get("resultTable"), "ANN query should return a result table: " + annResult);
    assertNotNull(exactResult.get("resultTable"), "Exact query should return a result table: " + exactResult);

    JsonNode annRows = annResult.get("resultTable").get("rows");
    JsonNode exactRows = exactResult.get("resultTable").get("rows");
    assertEquals(annRows.size(), topK, "ANN query should return the requested top-K rows");
    assertEquals(exactRows.size(), topK, "Exact query should return the requested top-K rows");

    for (int i = 0; i < topK; i++) {
      assertEquals(annRows.get(i).get(0).asDouble(), exactRows.get(i).get(0).asDouble(), 1e-5,
          "IVF_PQ with full probing and default exact rerank should match brute-force top-K");
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainShowsDefaultIvfPqRuntimeParams(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long perSegmentDocCount = getCountStarResult() / getNumAvroFiles();
    String explain = getExplainString(useMultiStageQueryEngine,
        String.format("set vectorNprobe=%d; set vectorMaxCandidates=%d;", NLIST, getCountStarResult()), 5);

    assertTrue(explain.contains("VECTOR_SIMILARITY_INDEX") || explain.contains("VectorSimilarityIndex"),
        "Explain should show ANN vector index path: " + explain);
    assertExplainContains(explain, "backend", "IVF_PQ");
    assertExplainContains(explain, "distanceFunction", "EUCLIDEAN");
    assertExplainContains(explain, "effectiveNprobe", NLIST);
    assertExplainContains(explain, "effectiveExactRerank", true);
    assertExplainContains(explain, "effectiveCandidateCount", perSegmentDocCount);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainShowsRerankOverride(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String explain = getExplainString(useMultiStageQueryEngine,
        "set vectorNprobe=2; set vectorExactRerank=false; set vectorMaxCandidates=17;", topK);

    assertTrue(explain.contains("VECTOR_SIMILARITY_INDEX") || explain.contains("VectorSimilarityIndex"),
        "Explain should show ANN vector index path: " + explain);
    assertExplainContains(explain, "backend", "IVF_PQ");
    assertExplainContains(explain, "distanceFunction", "EUCLIDEAN");
    assertExplainContains(explain, "effectiveNprobe", 2);
    assertExplainContains(explain, "effectiveExactRerank", false);
    assertExplainContains(explain, "effectiveCandidateCount", topK);
  }

  protected String getExplainString(boolean useMultiStageQueryEngine, String optionPrefix, int topK)
      throws Exception {
    String queryVectorLiteral = getQueryVectorLiteral();
    String similarityQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVectorLiteral, getTableName(),
        VECTOR_COL, queryVectorLiteral, topK, topK);
    JsonNode plan = postQuery(
        "set explainAskingServers=true; " + optionPrefix + " EXPLAIN PLAN FOR " + similarityQuery);
    return GroupByOptionsTest.toExplainStr(plan, useMultiStageQueryEngine);
  }

  protected String getQueryVectorLiteral() {
    return "ARRAY[1.1" + StringUtils.repeat(", 1.1", VECTOR_DIMENSION - 1) + "]";
  }

  protected float[] getQueryVector() {
    float[] queryVector = new float[VECTOR_DIMENSION];
    for (int i = 0; i < VECTOR_DIMENSION; i++) {
      queryVector[i] = 1.1f;
    }
    return queryVector;
  }

  protected float[] createRandomVector(int vectorDimension) {
    float[] vector = new float[vectorDimension];
    for (int i = 0; i < vectorDimension; i++) {
      vector[i] = RANDOM.nextFloat();
    }
    return vector;
  }

  protected Collection<Float> convertToFloatCollection(float[] vector) {
    Collection<Float> vectorCollection = new ArrayList<>();
    for (float value : vector) {
      vectorCollection.add(value);
    }
    return vectorCollection;
  }

  protected static double l2Distance(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  protected static void assertExplainContains(String explain, String key, Object value) {
    String legacyStyle = key + ":" + value;
    String msqStyle = key + "=[" + value + "]";
    assertTrue(explain.contains(legacyStyle) || explain.contains(msqStyle),
        "Explain should contain " + key + "=" + value + ": " + explain);
  }
}
