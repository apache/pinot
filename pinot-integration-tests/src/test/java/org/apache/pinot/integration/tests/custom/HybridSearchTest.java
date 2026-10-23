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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.apache.avro.Schema.createRecord;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end tests for `TEXT_SCORE` (BM25) and for hybrid lexical + vector retrieval fused with Reciprocal Rank
/// Fusion (RRF) in SQL on the multi-stage engine.
@Test(suiteName = "CustomClusterIntegrationTest")
public class HybridSearchTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "HybridSearchTest";
  private static final String ID_COL = "id";
  private static final String TITLE_COL = "title";
  private static final String EMBEDDING_COL = "embedding";
  private static final int NUM_RECORDS = 400;
  private static final int DIM = 8;
  private static final String[] VOCABULARY = {
      "usb", "charger", "cable", "hdmi", "monitor", "wireless", "mouse", "keyboard", "laptop", "stand", "power",
      "bank", "adapter", "travel", "hub", "braided"
  };
  private static final String TEXT_QUERY = "usb charger";
  private static final int CANDIDATES = 50;
  private static final int RRF_K = 60;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_RECORDS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID_COL, DataType.INT)
        .addSingleValueDimension(TITLE_COL, DataType.STRING)
        .addMultiValueDimension(EMBEDDING_COL, DataType.FLOAT)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setNoDictionaryColumns(List.of(TITLE_COL))
        .setFieldConfigList(List.of(
            new FieldConfig.Builder(TITLE_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.TEXT))
                .withEncodingType(FieldConfig.EncodingType.RAW)
                .build(),
            new FieldConfig.Builder(EMBEDDING_COL)
                .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
                .withEncodingType(FieldConfig.EncodingType.RAW)
                .withProperties(Map.of(
                    "vectorIndexType", "HNSW",
                    "vectorDimension", String.valueOf(DIM),
                    "vectorDistanceFunction", "COSINE",
                    "version", "1"))
                .build()))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    var avroSchema = createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new Field(ID_COL, create(Type.INT), null, null),
        new Field(TITLE_COL, create(Type.STRING), null, null),
        new Field(EMBEDDING_COL, createArray(create(Type.FLOAT)), null, null)));
    Random random = new Random(42);
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_RECORDS; i++) {
        int numWords = 2 + random.nextInt(7);
        StringBuilder title = new StringBuilder();
        for (int w = 0; w < numWords; w++) {
          if (w > 0) {
            title.append(' ');
          }
          title.append(VOCABULARY[random.nextInt(VOCABULARY.length)]);
        }
        List<Float> embedding = new ArrayList<>(DIM);
        for (int d = 0; d < DIM; d++) {
          embedding.add(random.nextFloat());
        }
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COL, i);
        record.put(TITLE_COL, title.toString());
        record.put(EMBEDDING_COL, embedding);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBm25TopK(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode rows = rows(postQuery(textTopKQuery(10)));
    assertEquals(rows.size(), 10);
    double previous = Double.MAX_VALUE;
    for (JsonNode row : rows) {
      double score = row.get(1).asDouble();
      assertTrue(score > 0, "BM25 score must be positive for a match: " + row);
      assertTrue(score <= previous, "Rows must be sorted by score: " + rows);
      previous = score;
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testScoreIsZeroForNonMatchingDocs(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long matching = rows(postQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(title, 'usb')"))
        .get(0).get(0).asLong();
    long positive = rows(postQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_SCORE(title, 'usb') > 0"))
        .get(0).get(0).asLong();
    long zero = rows(postQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_SCORE(title, 'usb') = 0"))
        .get(0).get(0).asLong();
    assertTrue(matching > 0 && matching < NUM_RECORDS);
    assertEquals(positive, matching);
    assertEquals(zero, NUM_RECORDS - matching);
  }

  /// RRF with `UNION ALL` + `GROUP BY`: `rrf(d) = sum over lists of 1 / (k + rank(d))`.
  @Test
  public void testRrfWithUnionAll()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "WITH text_hits AS (" + rankedTextQuery() + "), vec_hits AS (" + rankedVectorQuery() + ") "
        + "SELECT id, SUM(1.0 / (" + RRF_K + " + r)) AS rrf FROM ("
        + "SELECT id, r FROM text_hits UNION ALL SELECT id, r FROM vec_hits) "
        + "GROUP BY id ORDER BY rrf DESC, id LIMIT 10";
    assertRrfMatchesClientSide(postQuery(query));
  }

  /// RRF with `FULL OUTER JOIN`, which also exposes each list's rank.
  @Test
  public void testRrfWithFullOuterJoin()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "WITH text_hits AS (" + rankedTextQuery() + "), vec_hits AS (" + rankedVectorQuery() + ") "
        + "SELECT COALESCE(t.id, v.id) AS id, "
        + "COALESCE(1.0 / (" + RRF_K + " + t.r), 0.0) + COALESCE(1.0 / (" + RRF_K + " + v.r), 0.0) AS rrf "
        + "FROM text_hits t FULL OUTER JOIN vec_hits v ON t.id = v.id "
        + "ORDER BY rrf DESC, id LIMIT 10";
    assertRrfMatchesClientSide(postQuery(query));
  }

  private void assertRrfMatchesClientSide(JsonNode response)
      throws Exception {
    assertTrue(response.get("exceptions").isEmpty(), response.toString());
    Map<Integer, Double> expected = new HashMap<>();
    addRrf(expected, rows(postQuery(rankedTextQuery())));
    addRrf(expected, rows(postQuery(rankedVectorQuery())));
    JsonNode rows = rows(response);
    assertEquals(rows.size(), 10);
    List<Double> sortedExpected = new ArrayList<>(expected.values());
    sortedExpected.sort((a, b) -> Double.compare(b, a));
    for (int i = 0; i < rows.size(); i++) {
      int id = rows.get(i).get(0).asInt();
      double rrf = rows.get(i).get(1).asDouble();
      assertEquals(rrf, expected.get(id), 1e-9, "id " + id);
      assertEquals(rrf, sortedExpected.get(i), 1e-9, "rank " + i);
    }
  }

  private static void addRrf(Map<Integer, Double> rrf, JsonNode rankedRows) {
    for (JsonNode row : rankedRows) {
      rrf.merge(row.get(0).asInt(), 1.0 / (RRF_K + row.get(1).asLong()), Double::sum);
    }
  }

  private static String textTopKQuery(int k) {
    return "SELECT id, TEXT_SCORE(title, '" + TEXT_QUERY + "') AS s FROM " + TABLE_NAME
        + " WHERE TEXT_MATCH(title, '" + TEXT_QUERY + "') ORDER BY s DESC, id LIMIT " + k;
  }

  private static String rankedTextQuery() {
    return "SELECT id, ROW_NUMBER() OVER (ORDER BY s DESC, id) AS r FROM (" + textTopKQuery(CANDIDATES) + ")";
  }

  private static String rankedVectorQuery() {
    String vector = queryVector();
    return "SELECT id, ROW_NUMBER() OVER (ORDER BY d, id) AS r FROM ("
        + "SELECT id, cosineDistance(embedding, " + vector + ") AS d FROM " + TABLE_NAME
        + " WHERE VECTOR_SIMILARITY(embedding, " + vector + ", " + CANDIDATES + ") ORDER BY d, id LIMIT "
        + CANDIDATES + ")";
  }

  private static String queryVector() {
    StringBuilder sb = new StringBuilder("ARRAY[");
    for (int d = 0; d < DIM; d++) {
      sb.append(d > 0 ? ", " : "").append(0.1 * (d + 1));
    }
    return sb.append(']').toString();
  }

  private static JsonNode rows(JsonNode response) {
    return response.get("resultTable").get("rows");
  }
}
