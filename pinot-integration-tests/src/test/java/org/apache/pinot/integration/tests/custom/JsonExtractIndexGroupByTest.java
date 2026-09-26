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
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Reproduction attempt for PINOT-489 / pinot-planning#491:
/// `dateTrunc(json_extract_scalar(...))` combined with a second group-by key computed by
/// `json_extract_index(...)` is reported to return non-deterministic per-bucket attribution on the
/// multistage engine, while the single-key variant and the single-stage engine are correct.
@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonExtractIndexGroupByTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "JsonExtractIndexGroupByTest";
  protected static final String PROPERTIES_FIELD = "properties";
  protected static final String BUCKET_FIELD = "bucket";

  // Large enough to span several 10k doc-id blocks per segment.
  // Several segments per server matters: the SSE sort-aggregate combine only pair-wise merges segment results when a
  // server holds more than one segment, and that merge is where PINOT-489 corrupts group keys.
  protected static final int NUM_DOCS_PER_SEGMENT = 12_000;
  protected static final int NUM_AVRO_FILES = 8;
  protected static final int NUM_MONTHS = 12;
  protected static final int NUM_BUCKETS = 10;
  protected static final int SELECTED_BUCKET = 3;
  protected static final String[] MOVEMENT_TYPES = {"new", "expansion", "contraction", "churn", "reactivation"};

  protected static final String MONTH_KEY =
      "dateTrunc('month', json_extract_scalar(properties, '$.hs_revenue_month', 'Long', 0), 'MILLISECONDS', 'UTC')";
  protected static final String TYPE_KEY_INDEX =
      "json_extract_index(properties, '$.hs_mrr_movement_type', 'String', 'null')";
  protected static final String TYPE_KEY_SCALAR =
      "json_extract_scalar(properties, '$.hs_mrr_movement_type', 'String', 'null')";
  protected static final String SUM_EXPR =
      "sumprecision(json_extract_scalar(properties, '$.hs_mrr_in_company_currency', 'Double', 0))";

  /// month start epoch millis -> movement type -> expected sum, for all rows
  protected final Map<Long, Map<String, BigDecimal>> _expected = new LinkedHashMap<>();
  /// month start epoch millis -> movement type -> expected sum, restricted to bucket = SELECTED_BUCKET
  protected final Map<Long, Map<String, BigDecimal>> _expectedFiltered = new LinkedHashMap<>();
  protected final Map<Long, BigDecimal> _expectedByMonth = new LinkedHashMap<>();

  @Override
  public int getNumAvroFiles() {
    return NUM_AVRO_FILES;
  }

  @Override
  protected long getCountStarResult() {
    long numDocsPerSegment = NUM_DOCS_PER_SEGMENT;
    return numDocsPerSegment * NUM_AVRO_FILES;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(PROPERTIES_FIELD, FieldSpec.DataType.STRING)
        .addSingleValueDimension(BUCKET_FIELD, FieldSpec.DataType.INT)
        .addDateTime(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(TIMESTAMP_FIELD_NAME)
        .setJsonIndexColumns(List.of(PROPERTIES_FIELD))
        .setNoDictionaryColumns(List.of(PROPERTIES_FIELD))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(PROPERTIES_FIELD,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(BUCKET_FIELD,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)));

    long[] monthStarts = new long[NUM_MONTHS];
    for (int m = 0; m < NUM_MONTHS; m++) {
      monthStarts[m] = LocalDate.of(2024, m + 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int fileId = 0; fileId < writers.size(); fileId++) {
        DataFileWriter<GenericData.Record> writer = writers.get(fileId);
        for (int i = 0; i < NUM_DOCS_PER_SEGMENT; i++) {
          // Give every segment a DIFFERENT, partially-overlapping subset of (month, type) pairs. If all segments
          // produce the same sorted key sequence, a positional merge lines up by accident and hides key-comparison
          // bugs in the sort-aggregate combine, so the subsets must genuinely diverge.
          int monthIdx = (fileId + (i / 2) % 6) % NUM_MONTHS;
          String type = MOVEMENT_TYPES[(fileId + (i % 2)) % MOVEMENT_TYPES.length];
          int bucket = (i * 7 + fileId) % NUM_BUCKETS;
          long monthStart = monthStarts[monthIdx];
          long revenueMonth = monthStart + (i % 27) * 86_400_000L;
          BigDecimal amount = BigDecimal.valueOf((i % 100) + 1);

          Map<String, Object> properties = new HashMap<>();
          properties.put("hs_revenue_month", revenueMonth);
          properties.put("hs_mrr_movement_type", type);
          properties.put("hs_mrr_in_company_currency", amount.doubleValue());
          properties.put("filler", "padding-" + i + "-" + fileId);

          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(PROPERTIES_FIELD, JsonUtils.objectToString(properties));
          record.put(BUCKET_FIELD, bucket);
          record.put(TIMESTAMP_FIELD_NAME, revenueMonth);
          writer.append(record);

          _expected.computeIfAbsent(monthStart, k -> new LinkedHashMap<>()).merge(type, amount, BigDecimal::add);
          _expectedByMonth.merge(monthStart, amount, BigDecimal::add);
          if (bucket == SELECTED_BUCKET) {
            _expectedFiltered.computeIfAbsent(monthStart, k -> new LinkedHashMap<>())
                .merge(type, amount, BigDecimal::add);
          }
        }
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSingleKeyGroupBy(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT %s AS m, %s AS s FROM %s GROUP BY %s ORDER BY %s LIMIT 500", MONTH_KEY,
        SUM_EXPR, getTableName(), MONTH_KEY, MONTH_KEY);
    for (int run = 0; run < 5; run++) {
      Map<Long, BigDecimal> actual = new LinkedHashMap<>();
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      for (JsonNode row : rows) {
        actual.put(row.get(0).asLong(), new BigDecimal(row.get(1).asText()));
      }
      assertEquals(actual.keySet(), _expectedByMonth.keySet(), "run " + run + " month keys");
      for (Map.Entry<Long, BigDecimal> entry : _expectedByMonth.entrySet()) {
        assertEquals(actual.get(entry.getKey()).compareTo(entry.getValue()), 0,
            "run " + run + " month " + entry.getKey());
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTwoKeyGroupByWithJsonExtractIndex(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertTwoKeyGroupBy(TYPE_KEY_INDEX, null, _expected);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTwoKeyGroupByWithJsonExtractScalar(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertTwoKeyGroupBy(TYPE_KEY_SCALAR, null, _expected);
  }

  /// Same as above but with a sparse filter, so the doc-id sets handed to the JSON index reader are a small,
  /// scattered subset of each block. This is the shape the production query has.
  @Test(dataProvider = "useBothQueryEngines")
  public void testTwoKeyGroupByWithSparseFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertTwoKeyGroupBy(TYPE_KEY_INDEX, BUCKET_FIELD + " = " + SELECTED_BUCKET, _expectedFiltered);
  }

  protected String buildQuery(String typeKey, @Nullable String whereClause) {
    String where = whereClause == null ? "" : " WHERE " + whereClause;
    return String.format("SELECT %s AS m, %s AS t, %s AS s FROM %s%s GROUP BY %s, %s ORDER BY %s, %s LIMIT 500",
        MONTH_KEY, typeKey, SUM_EXPR, getTableName(), where, MONTH_KEY, typeKey, MONTH_KEY, typeKey);
  }

  protected void assertTwoKeyGroupBy(String typeKey, @Nullable String whereClause,
      Map<Long, Map<String, BigDecimal>> expected)
      throws Exception {
    String query = buildQuery(typeKey, whereClause);
    List<String> failures = new ArrayList<>();
    for (int run = 0; run < 5; run++) {
      Map<Long, Map<String, BigDecimal>> actual = new LinkedHashMap<>();
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      for (JsonNode row : rows) {
        actual.computeIfAbsent(row.get(0).asLong(), k -> new LinkedHashMap<>())
            .merge(row.get(1).asText(), new BigDecimal(row.get(2).asText()), BigDecimal::add);
      }
      if (!actual.keySet().equals(expected.keySet())) {
        failures.add("run " + run + ": month keys " + actual.keySet() + " != " + expected.keySet());
        continue;
      }
      for (Map.Entry<Long, Map<String, BigDecimal>> monthEntry : expected.entrySet()) {
        Map<String, BigDecimal> actualForMonth = actual.get(monthEntry.getKey());
        if (!actualForMonth.keySet().equals(monthEntry.getValue().keySet())) {
          failures.add("run " + run + " month " + monthEntry.getKey() + ": types " + actualForMonth.keySet() + " != "
              + monthEntry.getValue().keySet());
          continue;
        }
        for (Map.Entry<String, BigDecimal> typeEntry : monthEntry.getValue().entrySet()) {
          BigDecimal actualSum = actualForMonth.get(typeEntry.getKey());
          if (actualSum.compareTo(typeEntry.getValue()) != 0) {
            failures.add("run " + run + " (" + monthEntry.getKey() + ", " + typeEntry.getKey() + "): " + actualSum
                + " != " + typeEntry.getValue());
          }
        }
      }
    }
    assertEquals(failures, List.of(), "mismatches for query: " + query + "\n" + String.join("\n", failures));
  }
}
