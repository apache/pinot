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
import java.util.Random;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Realtime integration coverage for filtered HNSW search on a **consuming** segment, where the vector index is
/// `MutableVectorIndex` rather than the offline reader.
///
/// This pins a behavior change that reaches plain realtime tables, not only upsert ones: because the consuming
/// segment now advertises `supportsPreFilter()`, a `vectorSimilarity` predicate combined with a metadata filter
/// plans as `FILTER_THEN_ANN` instead of running an unfiltered ANN and intersecting afterwards. Nothing else
/// covers this -- the other HNSW integration tests are offline-only, and [IvfPqVectorRealtimeTest] asserts the
/// IVF_PQ exact-scan fallback.
///
/// ## Why the fixture is sized the way it is
///
/// `VectorSearchStrategy.decide` only wires the optional pre-filter when the filter matches at least
/// `EXACT_SCAN_THRESHOLD` (1000) documents *and* selectivity falls below the mid-range cutoff of 0.105; anything
/// more selective is cheaper as an exact scan, anything less selective is left to post-filtering. A single
/// consuming segment of [#getCountStarResult] rows over [#NUM_CATEGORIES] categories yields 1250 matches at
/// selectivity 0.083, which clears both bounds with margin. One Kafka partition and a flush size above the row
/// count keep every row in one consuming segment, so the per-segment counts the planner sees are the ones
/// computed here. The category column carries an inverted index because the wiring additionally requires every
/// non-vector filter to produce a bitmap -- a scan-based predicate silently leaves the vector operator
/// post-filtering.
@Test(suiteName = "CustomClusterIntegrationTest")
public class HnswVectorRealtimeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "HnswVectorRealtimeTest";
  private static final String VECTOR_COL = "embedding";
  private static final String CATEGORY = "category";
  private static final int VECTOR_DIM_SIZE = 32;
  private static final int NUM_CATEGORIES = 12;
  private static final int NUM_ROWS = 15000;
  private static final String TARGET_CATEGORY = "cat_3";

  @Override
  protected long getCountStarResult() {
    return NUM_ROWS;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  /// Keep every row in a single consuming segment: the planner reasons about per-segment counts, so a mid-stream
  /// commit would shrink them below the pre-filter thresholds this test depends on.
  @Override
  protected int getRealtimeSegmentFlushSize() {
    return NUM_ROWS * 10;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(VECTOR_COL, DataType.FLOAT)
        .addSingleValueDimension(CATEGORY, DataType.STRING)
        .addDateTimeField(getTimeColumnName(), DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  /// The inherited realtime builder attaches no field configs, so the HNSW index is declared here -- without it the
  /// consuming segment would hold no vector index and these tests would silently exercise a scan instead.
  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return getTableConfigBuilder(TableType.REALTIME)
        // The pre-filter is only wired when every non-vector filter can produce a bitmap; without an index the
        // category predicate plans as a full scan and the vector operator never receives the bitmap.
        .setInvertedIndexColumns(List.of(CATEGORY))
        .setFieldConfigList(List.of(
            new FieldConfig.Builder(VECTOR_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
                .withEncodingType(FieldConfig.EncodingType.RAW)
                .withProperties(Map.of(
                    "vectorIndexType", "HNSW",
                    "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
                    "vectorDistanceFunction", "COSINE",
                    "version", "1"))
                .build()
        ))
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
        new org.apache.avro.Schema.Field(CATEGORY,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(getTimeColumnName(),
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      Random random = new Random(42);
      long baseTimestamp = System.currentTimeMillis();
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        Collection<Float> vector = new ArrayList<>(VECTOR_DIM_SIZE);
        for (int d = 0; d < VECTOR_DIM_SIZE; d++) {
          vector.add(random.nextFloat());
        }
        record.put(VECTOR_COL, vector);
        record.put(CATEGORY, "cat_" + (i % NUM_CATEGORIES));
        record.put(getTimeColumnName(), baseTimestamp + i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /// Pins that the planner actually hands the metadata bitmap to the consuming segment's vector operator, so a
  /// regression that stops wiring the pre-filter fails here rather than surfacing only as a recall difference.
  ///
  /// The operator's `searchMode` is deliberately not asserted: it is initialized to `POST_FILTER_ANN` and only
  /// reassigned to `FILTER_THEN_ANN` while the search executes, and `EXPLAIN` plans without executing, so it
  /// always reports the initial value here. `filterSelectivity` is derived from the bitmap the operator received,
  /// which makes it the field that actually distinguishes a wired pre-filter from an unwired one.
  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainShowsPreFilterReachesConsumingSegment(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String explainQuery = String.format(
        "set explainAskingServers=true; EXPLAIN PLAN FOR "
            + "SELECT cosineDistance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) AND %s = '%s' ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector(), getTableName(), VECTOR_COL, queryVector(), 10, CATEGORY, TARGET_CATEGORY, 10);

    String explain = postQuery(explainQuery).get("resultTable").toString();
    assertTrue(explain.contains("backend"), "Explain should describe the vector index: " + explain);
    // NUM_ROWS / NUM_CATEGORIES matches out of NUM_ROWS -- the bitmap the operator received, not an estimate.
    assertTrue(explain.contains("filterSelectivity"),
        "The metadata bitmap should be wired into the consuming segment's vector operator: " + explain);
    assertTrue(explain.contains("0.0833"),
        "Pre-filter selectivity should reflect the " + (NUM_ROWS / NUM_CATEGORIES) + " matching rows: " + explain);
  }

  /// Every row a filtered vector search returns must satisfy the predicate. A disallowed row here would mean
  /// candidate generation ignored the pre-filter bitmap.
  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAnnReturnsOnlyMatchingRows(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 10;
    String query = String.format(
        "SELECT cosineDistance(%s, %s) AS dist, %s FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) AND %s = '%s' "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector(), CATEGORY, getTableName(),
        VECTOR_COL, queryVector(), topK, CATEGORY, TARGET_CATEGORY, topK);

    JsonNode rows = postQuery(query).get("resultTable").get("rows");
    assertEquals(rows.size(), topK, "Filtered ANN on a consuming segment must return a full topK");
    double prevDist = -1;
    for (int i = 0; i < rows.size(); i++) {
      assertEquals(rows.get(i).get(1).asText(), TARGET_CATEGORY, "All results must match the filter");
      double dist = rows.get(i).get(0).asDouble();
      assertTrue(dist >= prevDist, "Results must be ordered by distance");
      prevDist = dist;
    }
  }

  /// Candidate generation constrained by the filter yields a full topK drawn from the matching rows. Under the
  /// previous unfiltered-ANN-then-intersect behavior only the matching share of an unfiltered top-K survived,
  /// which for this fixture would be roughly `topK / NUM_CATEGORIES` rows.
  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAnnDrawsTopKFromTheFilteredSet(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 50;
    String filteredQuery = String.format(
        "SELECT count(*) FROM %s WHERE vectorSimilarity(%s, %s, %d) AND %s = '%s'",
        getTableName(), VECTOR_COL, queryVector(), topK, CATEGORY, TARGET_CATEGORY);

    long filteredCount = postQuery(filteredQuery).get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(filteredCount >= topK,
        "Filtered ANN must draw a full topK from the matching rows, got " + filteredCount + " for topK " + topK
            + "; post-intersection would yield roughly " + (topK / NUM_CATEGORIES));
  }

  private static String queryVector() {
    return "ARRAY[0.5" + StringUtils.repeat(", 0.5", VECTOR_DIM_SIZE - 1) + "]";
  }
}
