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
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;
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
        String title = nextTitle(random);
        List<Float> embedding = new ArrayList<>(DIM);
        for (int d = 0; d < DIM; d++) {
          embedding.add(random.nextFloat());
        }
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COL, i);
        record.put(TITLE_COL, title);
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

  /// Two-step global BM25: step 1 collects corpus statistics with `TEXT_SCORE_STATS` (summed over both servers'
  /// segments), step 2 passes them to `TEXT_SCORE`. Scores must equal BM25 computed over the whole corpus, which the
  /// per-segment default does not.
  @Test(dataProvider = "useBothQueryEngines")
  public void testGlobalStatsTwoStep(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode statsResponse =
        postQuery("SELECT TEXT_SCORE_STATS(title, '" + TEXT_QUERY + "') FROM " + TABLE_NAME);
    assertTrue(statsResponse.get("exceptions").isEmpty(), statsResponse.toString());
    String stats = rows(statsResponse).get(0).get(0).asText();
    TextScoreStats.FieldStats fieldStats = TextScoreStats.fromJson(stats).getField(TITLE_COL);
    assertEquals(fieldStats.getDocCount(), NUM_RECORDS);

    List<String> titles = generateTitles();
    Map<Integer, Double> expected = corpusBm25(titles, TEXT_QUERY.split(" "));
    assertEquals(fieldStats.getTermStats("usb")[0], countDocsContaining(titles, "usb"));

    Map<Integer, Double> global = scoresById(
        "SELECT id, TEXT_SCORE(title, '" + TEXT_QUERY + "', '', '" + stats + "') FROM " + TABLE_NAME
            + " WHERE TEXT_MATCH(title, '" + TEXT_QUERY + "') LIMIT 1000");
    Map<Integer, Double> local = scoresById(
        "SELECT id, TEXT_SCORE(title, '" + TEXT_QUERY + "') FROM " + TABLE_NAME
            + " WHERE TEXT_MATCH(title, '" + TEXT_QUERY + "') LIMIT 1000");
    assertEquals(global.keySet(), expected.keySet());
    double maxLocalError = 0;
    for (Map.Entry<Integer, Double> entry : expected.entrySet()) {
      assertEquals(global.get(entry.getKey()), entry.getValue(), 1e-4, "id " + entry.getKey());
      maxLocalError = Math.max(maxLocalError, Math.abs(local.get(entry.getKey()) - entry.getValue()));
    }
    // Per-segment statistics (2 segments of 200 random titles) do not reproduce corpus-wide BM25
    assertTrue(maxLocalError > 1e-3, "per-segment max error " + maxLocalError);
  }

  private Map<Integer, Double> scoresById(String query)
      throws Exception {
    JsonNode response = postQuery(query);
    assertTrue(response.get("exceptions").isEmpty(), response.toString());
    Map<Integer, Double> scores = new HashMap<>();
    for (JsonNode row : rows(response)) {
      scores.put(row.get(0).asInt(), row.get(1).asDouble());
    }
    return scores;
  }

  /// Regenerates the titles in id order, consuming the random stream exactly like [#createAvroFiles].
  private static List<String> generateTitles() {
    Random random = new Random(42);
    List<String> titles = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      titles.add(nextTitle(random));
      for (int d = 0; d < DIM; d++) {
        random.nextFloat();
      }
    }
    return titles;
  }

  private static String nextTitle(Random random) {
    int numWords = 2 + random.nextInt(7);
    StringBuilder title = new StringBuilder();
    for (int w = 0; w < numWords; w++) {
      if (w > 0) {
        title.append(' ');
      }
      title.append(VOCABULARY[random.nextInt(VOCABULARY.length)]);
    }
    return title.toString();
  }

  private static long countDocsContaining(List<String> titles, String term) {
    return titles.stream().filter(t -> List.of(t.split(" ")).contains(term)).count();
  }

  /// Lucene BM25 (k1 = 1.2, b = 0.75) of an OR query over the whole corpus, for matching ids. Titles have at most 8
  /// tokens, which Lucene's norm encoding stores exactly.
  private static Map<Integer, Double> corpusBm25(List<String> titles, String[] terms) {
    double k1 = 1.2;
    double b = 0.75;
    long sumLength = 0;
    for (String title : titles) {
      sumLength += title.split(" ").length;
    }
    double avgdl = (float) ((double) sumLength / titles.size());
    Map<Integer, Double> scores = new HashMap<>();
    for (String term : terms) {
      long docFreq = countDocsContaining(titles, term);
      double idf = Math.log(1 + (titles.size() - docFreq + 0.5) / (docFreq + 0.5));
      for (int id = 0; id < titles.size(); id++) {
        String[] tokens = titles.get(id).split(" ");
        int tf = 0;
        for (String token : tokens) {
          if (token.equals(term)) {
            tf++;
          }
        }
        if (tf > 0) {
          double score = idf * tf / (tf + k1 * (1 - b + b * tokens.length / avgdl));
          scores.merge(id, score, Double::sum);
        }
      }
    }
    return scores;
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
