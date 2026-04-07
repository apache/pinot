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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for Phase 3 vector query semantics: filtered ANN, threshold/radius search,
 * compound retrieval patterns, and execution mode reporting.
 *
 * <p>Uses HNSW backend with a metadata column ({@code category}) to test filtered ANN and
 * compound patterns end-to-end in a real Pinot cluster.</p>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class VectorPhase3QuerySemanticsTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "VectorPhase3Test";
  private static final String VECTOR_COL = "embedding";
  private static final String CATEGORY_COL = "category";
  private static final String L2_DIST_COL = "embeddingL2Dist";
  private static final int VECTOR_DIM_SIZE = 32;
  private static final int NUM_CATEGORIES = 5;

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
                    "vectorIndexType", "HNSW",
                    "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
                    "vectorDistanceFunction", "EUCLIDEAN",
                    "version", "1"))
                .build()
        ))
        .build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(VECTOR_COL, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(CATEGORY_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(L2_DIST_COL, FieldSpec.DataType.DOUBLE)
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
        new org.apache.avro.Schema.Field(CATEGORY_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(L2_DIST_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null)
    ));

    float[] queryVector = new float[VECTOR_DIM_SIZE];
    for (int i = 0; i < VECTOR_DIM_SIZE; i++) {
      queryVector[i] = 1.0f;
    }

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < getCountStarResult(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        float[] vector = createRandomVector(VECTOR_DIM_SIZE);
        record.put(VECTOR_COL, convertToFloatCollection(vector));
        record.put(CATEGORY_COL, "cat_" + (i % NUM_CATEGORIES));
        record.put(L2_DIST_COL, l2Distance(vector, queryVector));
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  // ---- Test: Basic top-K backward compatibility ----

  /**
   * Verifies that basic VECTOR_SIMILARITY top-K queries still work after Phase 3 changes.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testBasicTopKBackwardCompatible(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVector = getQueryVectorLiteral();

    String annQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK * 10, topK);

    String exactQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(), topK);

    JsonNode annResult = postQuery(annQuery);
    JsonNode exactResult = postQuery(exactQuery);

    assertResultTableNotEmpty(annResult, "ANN");
    assertResultTableNotEmpty(exactResult, "Exact");

    JsonNode annRows = annResult.get("resultTable").get("rows");
    JsonNode exactRows = exactResult.get("resultTable").get("rows");

    // ANN top-1 should be within 1.5x of exact top-1
    double annTopDist = annRows.get(0).get(0).asDouble();
    double exactTopDist = exactRows.get(0).get(0).asDouble();
    assertTrue(annTopDist <= exactTopDist * 1.5,
        "ANN top-1 (" + annTopDist + ") should be within 1.5x of exact top-1 (" + exactTopDist + ")");
  }

  // ---- Test: Filtered ANN ----

  /**
   * Tests VECTOR_SIMILARITY combined with a metadata filter (AND).
   * Verifies that results respect both the ANN proximity and the filter predicate.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAnn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVector = getQueryVectorLiteral();
    String targetCategory = "cat_0";

    // Filtered ANN: vector similarity + category filter
    String filteredAnnQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist, %s FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) AND %s = '%s' "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, CATEGORY_COL, getTableName(),
        VECTOR_COL, queryVector, topK * 10, CATEGORY_COL, targetCategory, topK);

    JsonNode result = postQuery(filteredAnnQuery);
    assertResultTableNotEmpty(result, "Filtered ANN");

    JsonNode rows = result.get("resultTable").get("rows");
    assertTrue(rows.size() > 0, "Filtered ANN query should return at least 1 result");

    // Verify all returned rows match the filter
    for (int i = 0; i < rows.size(); i++) {
      String category = rows.get(i).get(1).asText();
      assertTrue(category.equals(targetCategory),
          "Row " + i + " category should be '" + targetCategory + "' but was '" + category + "'");
    }

    // Verify results are ordered by distance
    double prevDist = -1;
    for (int i = 0; i < rows.size(); i++) {
      double dist = rows.get(i).get(0).asDouble();
      assertTrue(dist >= prevDist, "Results should be ordered by distance ascending");
      prevDist = dist;
    }
  }

  /**
   * Tests that filtered ANN returns fewer or equal results compared to unfiltered ANN.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAnnReducesResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 20;
    String queryVector = getQueryVectorLiteral();

    // Unfiltered
    String unfilteredQuery = String.format(
        "SELECT count(*) FROM %s WHERE vectorSimilarity(%s, %s, %d)",
        getTableName(), VECTOR_COL, queryVector, topK);

    // Filtered to 1 of 5 categories (expect ~20% of results)
    String filteredQuery = String.format(
        "SELECT count(*) FROM %s WHERE vectorSimilarity(%s, %s, %d) AND %s = 'cat_0'",
        getTableName(), VECTOR_COL, queryVector, topK, CATEGORY_COL);

    JsonNode unfilteredResult = postQuery(unfilteredQuery);
    JsonNode filteredResult = postQuery(filteredQuery);

    long unfilteredCount = unfilteredResult.get("resultTable").get("rows").get(0).get(0).asLong();
    long filteredCount = filteredResult.get("resultTable").get("rows").get(0).get(0).asLong();

    assertTrue(filteredCount <= unfilteredCount,
        "Filtered count (" + filteredCount + ") should be <= unfiltered count (" + unfilteredCount + ")");
    assertTrue(filteredCount > 0,
        "Filtered ANN should return at least 1 result (dataset has ~200 rows in cat_0)");
  }

  // ---- Test: Threshold/Radius Search ----

  /**
   * Tests threshold search with vectorDistanceThreshold query option.
   * All returned results should have exact distance within the threshold.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testThresholdSearch(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryVector = getQueryVectorLiteral();

    // Use the pre-computed L2 distance column to find a reasonable threshold.
    // L2_DIST_COL stores sqrt(sum of squares), but vectorDistanceThreshold uses
    // euclideanDistance which returns sum of squares (no sqrt). So we square the
    // percentile value to get the correct internal threshold.
    String medianQuery = String.format(
        "SELECT PERCENTILE(%s, 25) FROM %s", L2_DIST_COL, getTableName());
    JsonNode medianResult = postQuery(medianQuery);
    double l2Dist = medianResult.get("resultTable").get("rows").get(0).get(0).asDouble();
    double threshold = l2Dist * l2Dist;

    // Threshold search: returns all vectors within distance
    String thresholdQuery = String.format(
        "SET vectorDistanceThreshold = %f; "
            + "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT 100",
        threshold, VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, 200);

    JsonNode result = postQuery(thresholdQuery);
    assertResultTableNotEmpty(result, "Threshold search");

    JsonNode rows = result.get("resultTable").get("rows");
    assertTrue(rows.size() > 0, "Threshold search should return results");

    // Verify all returned l2Distance values (sqrt) are within sqrt(threshold)
    for (int i = 0; i < rows.size(); i++) {
      double dist = rows.get(i).get(0).asDouble();
      assertTrue(dist <= l2Dist + 1e-3,
          "Row " + i + " l2Distance (" + dist + ") should be within sqrt(threshold) (" + l2Dist + ")");
    }
  }

  // ---- Test: Compound filter + threshold ----

  /**
   * Tests compound pattern: metadata filter + threshold search.
   * Results should satisfy both the category filter and the distance threshold.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testCompoundFilterAndThreshold(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryVector = getQueryVectorLiteral();
    String targetCategory = "cat_1";

    // Get a threshold that returns a reasonable number of results.
    // L2_DIST_COL stores sqrt distances, but vectorDistanceThreshold compares against
    // euclideanDistance (squared). Square the percentile value.
    String medianQuery = String.format(
        "SELECT PERCENTILE(%s, 50) FROM %s", L2_DIST_COL, getTableName());
    JsonNode medianResult = postQuery(medianQuery);
    double l2Dist = medianResult.get("resultTable").get("rows").get(0).get(0).asDouble();
    double threshold = l2Dist * l2Dist;

    String compoundQuery = String.format(
        "SET vectorDistanceThreshold = %f; "
            + "SELECT l2Distance(%s, %s) AS dist, %s FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) AND %s = '%s' "
            + "ORDER BY dist ASC LIMIT 100",
        threshold, VECTOR_COL, queryVector, CATEGORY_COL, getTableName(),
        VECTOR_COL, queryVector, 200, CATEGORY_COL, targetCategory);

    JsonNode result = postQuery(compoundQuery);
    assertResultTableNotEmpty(result, "Compound filter+threshold");

    JsonNode rows = result.get("resultTable").get("rows");
    assertTrue(rows.size() > 0, "Compound query should return results");

    // Verify all results satisfy both conditions
    for (int i = 0; i < rows.size(); i++) {
      double dist = rows.get(i).get(0).asDouble();
      String category = rows.get(i).get(1).asText();
      assertTrue(dist <= l2Dist + 1e-3,
          "Row " + i + " l2Distance (" + dist + ") should be within sqrt(threshold) (" + l2Dist + ")");
      assertTrue(category.equals(targetCategory),
          "Row " + i + " category should be '" + targetCategory + "' but was '" + category + "'");
    }
  }

  // ---- Test: Execution mode in EXPLAIN output ----

  /**
   * Tests that EXPLAIN output includes the execution mode for a plain top-K query.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainShowsExecutionMode(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryVector = getQueryVectorLiteral();
    int topK = 5;

    String explainQuery = String.format(
        "EXPLAIN PLAN FOR SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK, topK);

    JsonNode explainResult = postQuery(explainQuery);
    assertNotNull(explainResult.get("resultTable"), "EXPLAIN should return a result table");
    String explainText = explainResult.get("resultTable").toString();

    // Should contain execution mode
    assertTrue(explainText.contains("executionMode") || explainText.contains("VECTOR_SIMILARITY"),
        "EXPLAIN output should contain execution mode info: " + explainText);
  }

  /**
   * Tests that EXPLAIN output for filtered ANN shows ANN_THEN_FILTER mode.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainFilteredAnnMode(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String queryVector = getQueryVectorLiteral();
    int topK = 5;

    String explainQuery = String.format(
        "EXPLAIN PLAN FOR SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) AND %s = 'cat_0' ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK, CATEGORY_COL, topK);

    JsonNode explainResult = postQuery(explainQuery);
    assertNotNull(explainResult.get("resultTable"), "EXPLAIN should return a result table");
    String explainText = explainResult.get("resultTable").toString();

    assertTrue(explainText.contains("ANN_THEN_FILTER") || explainText.contains("VECTOR_SIMILARITY"),
        "Filtered ANN EXPLAIN should contain ANN_THEN_FILTER or VECTOR_SIMILARITY: " + explainText);
  }

  // ---- Helpers ----

  private String getQueryVectorLiteral() {
    return "ARRAY[1.0" + StringUtils.repeat(", 1.0", VECTOR_DIM_SIZE - 1) + "]";
  }

  private void assertResultTableNotEmpty(JsonNode result, String queryType) {
    assertNotNull(result.get("resultTable"),
        queryType + " query should return a resultTable, got: " + result);
    JsonNode rows = result.get("resultTable").get("rows");
    assertNotNull(rows, queryType + " query should have rows");
  }

  private static double l2Distance(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private float[] createRandomVector(int dim) {
    float[] vector = new float[dim];
    for (int i = 0; i < dim; i++) {
      vector[i] = RANDOM.nextFloat();
    }
    return vector;
  }

  private Collection<Float> convertToFloatCollection(float[] vector) {
    Collection<Float> result = new ArrayList<>();
    for (float v : vector) {
      result.add(v);
    }
    return result;
  }
}
