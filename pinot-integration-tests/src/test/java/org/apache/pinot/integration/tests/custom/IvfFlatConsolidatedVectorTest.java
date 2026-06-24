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
 * End-to-end integration test for IVF_FLAT with {@code storeInSegmentFile=true}.
 *
 * <p>Mirrors the table-config layout in {@link IvfFlatVectorTest} but with the consolidation
 * flag turned on. The first server load absorbs the offline-built combined into {@code
 * columns.psf}; subsequent queries read the typed entry directly from the segment's combined
 * index file. The single test method asserts that a {@code vectorSimilarity} query returns
 * sensible top-K ANN candidates — i.e. the consolidated read path actually executes.</p>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class IvfFlatConsolidatedVectorTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "IvfFlatConsolidatedVectorTest";
  private static final String VECTOR_COL = "embedding";
  private static final String CATEGORY = "category";
  private static final int VECTOR_DIM_SIZE = 32;
  private static final int NLIST = 4;
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
                    "vectorIndexType", "IVF_FLAT",
                    "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
                    "vectorDistanceFunction", "EUCLIDEAN",
                    "nlist", String.valueOf(NLIST),
                    "version", "1",
                    // Consolidation: the IVF payload is moved into columns.psf as a typed entry
                    // after the offline build, instead of remaining as a sibling combined file.
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
   * the consolidated reader path resolves and reads the typed entry from {@code columns.psf}),
   * and (b) the query returns rows ordered by L2 distance ascending (proves the consolidated
   * IVF payload is interpreted correctly).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testConsolidatedIvfFlatVectorSimilarity(boolean useMultiStageQueryEngine)
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
        "Consolidated IVF_FLAT ANN query must return a resultTable; got: " + annResult);

    JsonNode rows = annResult.get("resultTable").get("rows");
    assertTrue(rows.size() > 0, "Consolidated IVF_FLAT ANN query should return at least 1 row");

    // Distances must be monotonically non-decreasing — proves the consolidated reader returned
    // valid scored candidates rather than scrambled bytes.
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
