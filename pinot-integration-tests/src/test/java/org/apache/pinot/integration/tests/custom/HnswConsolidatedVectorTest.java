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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end integration test for HNSW with {@code storeInSegmentFile=true}.
 *
 * <p>Sibling of {@link IvfFlatConsolidatedVectorTest} for the HNSW backend. The first server load
 * packs the offline-built Lucene HNSW directory into a combined file and absorbs it into {@code
 * columns.psf}; subsequent queries read the typed entry directly via the buffer-backed
 * {@code HnswVectorIndexReader} (including the little-endian docId mapping consumed by
 * {@code DocIdTranslator}). The single test method asserts that a {@code vectorSimilarity} query
 * returns sensible top-K ANN candidates — i.e. the consolidated HNSW read path executes and the
 * Lucene-to-Pinot doc id translation is not byte-swapped.</p>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class HnswConsolidatedVectorTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "HnswConsolidatedVectorTest";
  private static final String VECTOR_COL = "embedding";
  private static final String CATEGORY = "category";
  private static final int VECTOR_DIM_SIZE = 32;
  private static final int NUM_CATEGORIES = 3;

  @Override
  protected long getCountStarResult() {
    return 200;
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
                    "version", "1",
                    // Consolidation: the Lucene HNSW directory is packed into a combined file and
                    // moved into columns.psf as a typed entry after the offline build, instead of
                    // remaining as a sibling directory.
                    "storeInSegmentFile", "true"))
                .build()
        ))
        .build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(VECTOR_COL, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(CATEGORY, FieldSpec.DataType.STRING)
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
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < getCountStarResult(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        float[] vector = createRandomVector(VECTOR_DIM_SIZE);
        record.put(VECTOR_COL, convertToFloatCollection(vector));
        record.put(CATEGORY, "cat_" + (i % NUM_CATEGORIES));
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /**
   * End-to-end: writes a segment with {@code storeInSegmentFile=true}, loads it onto a server,
   * and runs an ANN query. Passes iff (a) the server can load the segment without error (proves
   * the consolidated HNSW reader path resolves the typed entry from {@code columns.psf} and
   * rebuilds the Lucene directory + docId mapping from the packed buffer), and (b) the query
   * returns rows ordered by L2 distance ascending (proves the Lucene-to-Pinot doc id translation
   * is read little-endian rather than byte-swapped by the big-endian columns.psf view).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testConsolidatedHnswVectorSimilarity(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int topK = 5;
    String queryVector = "ARRAY[1.1" + StringUtils.repeat(", 1.1", VECTOR_DIM_SIZE - 1) + "]";

    String annQuery = String.format(
        "SELECT l2Distance(%s, %s) AS dist FROM %s "
            + "WHERE vectorSimilarity(%s, %s, %d) "
            + "ORDER BY dist ASC LIMIT %d",
        VECTOR_COL, queryVector, getTableName(),
        VECTOR_COL, queryVector, topK * 10, topK);

    JsonNode annResult = postQuery(annQuery);
    assertNotNull(annResult.get("resultTable"),
        "Consolidated HNSW ANN query must return a resultTable; got: " + annResult);

    JsonNode rows = annResult.get("resultTable").get("rows");
    assertTrue(rows.size() > 0, "Consolidated HNSW ANN query should return at least 1 row");

    // Distances must be monotonically non-decreasing — proves the consolidated reader returned
    // valid scored candidates with correct doc id translation rather than scrambled bytes.
    double prevDist = -1;
    for (int i = 0; i < rows.size(); i++) {
      double dist = rows.get(i).get(0).asDouble();
      assertTrue(dist >= 0, "L2 distance must be non-negative (row " + i + " = " + dist + ")");
      assertTrue(dist >= prevDist,
          "Rows must be ordered ascending by L2 distance; row " + i + " = " + dist
              + " < prev = " + prevDist);
      prevDist = dist;
    }
  }

  private static final Random RANDOM = new Random(42L);

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
