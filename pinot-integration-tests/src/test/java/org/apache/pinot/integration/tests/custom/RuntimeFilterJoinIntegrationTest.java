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
import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end correctness coverage for the additive INNER-join probe-side runtime filter
/// ({@code PinotJoinToInnerRuntimeFilterRule}). Uses self-joins over a single table so a selective
/// filter on the build (right) side makes the build small; the probe (left) side is then reduced by the
/// runtime filter. The core invariant tested: results MUST be identical with the runtime filter on (in
/// every mode: in / bloom / auto) and off — the filter is a no-false-negative reducer and the real hash
/// join remains the source of truth. Covers exact-IN, bloom (numeric + string keys), AUTO, multi-key,
/// empty build, and null keys.
@Test(suiteName = "CustomClusterIntegrationTest")
public class RuntimeFilterJoinIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "RuntimeFilterJoinIntegrationTest";
  private static final String ID = "id";        // unique INT key
  private static final String LID = "lid";       // unique LONG key
  private static final String DKEY = "dkey";     // unique DOUBLE key (id + 0.5)
  private static final String SKEY = "skey";     // STRING key, 25 distinct values (groups of 4)
  private static final String NKEY = "nkey";     // nullable LONG key (null when id % 7 == 0)
  private static final String NANKEY = "nankey"; // DOUBLE key that is NaN for id < 3, else distinct (id + 1000)
  private static final String CAT = "cat";       // category: "rare" for id < 5, else "common"
  private static final String VAL = "val";       // INT value == id
  private static final int NUM_DOCS = 100;
  private static final int NUM_RARE = 5;         // ids 0..4

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    // Null handling on so NKEY genuinely stores NULLs (exercises the build-side IS NOT NULL filter).
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setNullHandlingEnabled(true).build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(ID, DataType.INT)
        .addSingleValueDimension(LID, DataType.LONG)
        .addSingleValueDimension(DKEY, DataType.DOUBLE)
        .addSingleValueDimension(SKEY, DataType.STRING)
        .addSingleValueDimension(NKEY, DataType.LONG)
        .addSingleValueDimension(NANKEY, DataType.DOUBLE)
        .addSingleValueDimension(CAT, DataType.STRING)
        .addMetric(VAL, DataType.INT)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema nullableLong = org.apache.avro.Schema.createUnion(
        List.of(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)));
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null),
        new org.apache.avro.Schema.Field(LID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null,
            null),
        new org.apache.avro.Schema.Field(DKEY, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null,
            null),
        new org.apache.avro.Schema.Field(SKEY, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null,
            null),
        new org.apache.avro.Schema.Field(NKEY, nullableLong, null, null),
        new org.apache.avro.Schema.Field(NANKEY, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(CAT, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null,
            null),
        new org.apache.avro.Schema.Field(VAL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int id = 0; id < NUM_DOCS; id++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID, id);
        record.put(LID, (long) id);
        record.put(DKEY, id + 0.5);
        record.put(SKEY, "key_" + (id % 25));
        record.put(NKEY, id % 7 == 0 ? null : (long) id);
        record.put(NANKEY, id < 3 ? Double.NaN : (double) (id + 1000));
        record.put(CAT, id < NUM_RARE ? "rare" : "common");
        record.put(VAL, id);
        writers.get(id % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /// Runs an MSE query, surfacing any broker/server exception.
  private JsonNode runMse(String query)
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode response = postQuery(query);
    JsonNode exceptions = response.get("exceptions");
    assertTrue(exceptions == null || exceptions.isEmpty(),
        "query failed: " + query + " -> " + response.toPrettyString());
    return response.get("resultTable");
  }

  /// Asserts that the same query (built by injecting the runtime_filter hint into {@code selectBody})
  /// produces identical results in every mode and with the filter off (the baseline). The {@code rest}
  /// is the FROM/WHERE/ORDER BY tail shared by all variants.
  private void assertAllModesMatchBaseline(String selectBody, String rest)
      throws Exception {
    JsonNode baseline = runMse("SELECT " + selectBody + " " + rest);
    for (String mode : new String[]{"in", "bloom", "auto"}) {
      String hinted = "SELECT /*+ joinOptions(runtime_filter='" + mode + "') */ " + selectBody + " " + rest;
      JsonNode withFilter = runMse(hinted);
      assertEquals(withFilter, baseline,
          "runtime_filter='" + mode + "' must produce identical results to the baseline for: " + rest);
    }
  }

  @Test
  public void testCountSelfJoinSelectiveBuild()
      throws Exception {
    // Build (t2) filtered to the 5 rare ids; probe (t1) reduced to those ids by the runtime filter.
    assertAllModesMatchBaseline("COUNT(*)",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + ID + " = t2." + ID
            + " WHERE t2." + CAT + " = 'rare'");
  }

  @Test
  public void testCountAndSumValueMatchesBaseline()
      throws Exception {
    assertAllModesMatchBaseline("COUNT(*), SUM(t1." + VAL + ")",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + ID + " = t2." + ID
            + " WHERE t2." + CAT + " = 'rare'");
  }

  @Test
  public void testProjectedRowsMatchBaseline()
      throws Exception {
    // Compares the actual row set (ordered), catching wrong-row bugs the COUNT cannot.
    assertAllModesMatchBaseline("t1." + ID + ", t1." + VAL,
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + ID + " = t2." + ID
            + " WHERE t2." + CAT + " = 'rare' ORDER BY t1." + ID);
  }

  @Test
  public void testLongKeyMatchesBaseline()
      throws Exception {
    assertAllModesMatchBaseline("COUNT(*)",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + LID + " = t2." + LID
            + " WHERE t2." + CAT + " = 'rare'");
  }

  @Test
  public void testStringKeyMatchesBaseline()
      throws Exception {
    // Build filtered to a single id -> a single skey; probe reduced to that skey (4 rows share it).
    // Exercises the bloom path for a STRING key (no BETWEEN range predicate).
    assertAllModesMatchBaseline("COUNT(*)",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + SKEY + " = t2." + SKEY
            + " WHERE t2." + ID + " = 0");
  }

  @Test
  public void testMultiKeyMatchesBaseline()
      throws Exception {
    assertAllModesMatchBaseline("COUNT(*)",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + ID + " = t2." + ID
            + " AND t1." + LID + " = t2." + LID + " WHERE t2." + CAT + " = 'rare'");
  }

  @Test
  public void testMixedTypeKeyMatchesBaseline()
      throws Exception {
    // Join an INT key to a LONG key (id == lid for every row). Calcite coerces both sides to a common
    // type, so the probe-leaf key column and the build-key set share a stored type end-to-end; the filter
    // must stay sound. Guards the bloom path (which keys its IdSet on the build stored type) against a
    // probe/build type skew.
    assertAllModesMatchBaseline("COUNT(*)",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + ID + " = t2." + LID
            + " WHERE t2." + CAT + " = 'rare'");
  }

  @Test
  public void testNaNKeyMatchesBaseline()
      throws Exception {
    // NaN join keys on both sides (ids 0,1,2 have nankey = NaN); the build (t2 where id < 3) is all-NaN.
    // Confirms the exact-IN and bloom reducers agree with the MSE hash join on NaN equality: whatever the
    // join does with NaN = NaN, the filter must preserve it (no false negatives). If the leaf IN/bloom
    // dropped a probe NaN row the join keeps, the on/off counts would diverge and this would fail.
    String rest = "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + NANKEY + " = t2." + NANKEY
        + " WHERE t2." + ID + " < 3";
    assertAllModesMatchBaseline("COUNT(*)", rest);
    // Pin the regime so the parity check is not vacuous: the MSE hash join treats NaN = NaN, so the 3 NaN
    // probe rows join the 3 NaN build rows -> 9 pairs, and the reducer preserves all of them.
    JsonNode baseline = runMse("SELECT COUNT(*) " + rest);
    assertEquals(baseline.get("rows").get(0).get(0).asLong(), 9L,
        "the MSE hash join matches NaN = NaN (3 x 3); if this changes, revisit the exact-IN NaN handling");
  }

  @Test
  public void testEmptyBuildYieldsEmptyResult()
      throws Exception {
    // Build side matches nothing -> the probe is pruned entirely (constant-false). Result must be empty
    // in every mode and the baseline.
    String rest = "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + ID + " = t2." + ID
        + " WHERE t2." + CAT + " = 'does_not_exist'";
    assertAllModesMatchBaseline("t1." + ID, rest + " ORDER BY t1." + ID);
    JsonNode baseline = runMse("SELECT t1." + ID + " " + rest);
    assertEquals(baseline.get("rows").size(), 0, "empty build must yield no rows");
  }

  @Test
  public void testDoubleKeyMatchesBaseline()
      throws Exception {
    // DOUBLE join key exercises the FLOAT/DOUBLE exact-IN and bloom-BETWEEN paths end-to-end.
    assertAllModesMatchBaseline("COUNT(*), SUM(t1." + VAL + ")",
        "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + DKEY + " = t2." + DKEY
            + " WHERE t2." + CAT + " = 'rare'");
  }

  @Test
  public void testNullKeysMatchBaseline()
      throws Exception {
    // Nullable join key with genuine NULLs on both sides. An INNER join drops NULL keys; the build-side
    // IS NOT NULL filter plus the reducer must preserve on==off parity (NULL rows never join either way).
    String rest = "FROM " + TABLE_NAME + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + NKEY + " = t2." + NKEY
        + " WHERE t2." + CAT + " = 'rare' ORDER BY t1." + ID;
    JsonNode baseline = runMse("SET enableNullHandling=true; SELECT t1." + ID + " " + rest);
    for (String mode : new String[]{"in", "bloom", "auto"}) {
      JsonNode withFilter = runMse("SET enableNullHandling=true; SELECT /*+ joinOptions(runtime_filter='" + mode
          + "') */ t1." + ID + " " + rest);
      assertEquals(withFilter, baseline, "null-key runtime_filter='" + mode + "' must match baseline");
    }
    // Of the 5 rare ids (0..4), id 0 has a NULL nkey and is dropped; ids 1..4 join -> 4 rows.
    assertEquals(baseline.get("rows").size(), 4);
  }

  @Test
  public void testExactCountValues()
      throws Exception {
    // Pin the concrete expected counts (not just on==off) so a silent reduction-to-zero would be caught.
    JsonNode rare = runMse("SELECT /*+ joinOptions(runtime_filter='in') */ COUNT(*) FROM " + TABLE_NAME + " t1 JOIN "
        + TABLE_NAME + " t2 ON t1." + ID + " = t2." + ID + " WHERE t2." + CAT + " = 'rare'");
    assertEquals(rare.get("rows").get(0).get(0).asLong(), NUM_RARE);

    JsonNode stringKey = runMse("SELECT /*+ joinOptions(runtime_filter='bloom') */ COUNT(*) FROM " + TABLE_NAME
        + " t1 JOIN " + TABLE_NAME + " t2 ON t1." + SKEY + " = t2." + SKEY + " WHERE t2." + ID + " = 0");
    // skey "key_0" is shared by ids 0,25,50,75 in t1; t2 has only id 0 -> 4 joined rows.
    assertEquals(stringKey.get("rows").get(0).get(0).asLong(), 4L);
  }
}
