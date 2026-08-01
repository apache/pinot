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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/// End-to-end coverage for creating a VARIANT table, ingesting Parquet VARIANT(1), materializing a hot path, and
/// querying nested values with both Pinot query engines.
@Test(suiteName = "CustomClusterIntegrationTest")
public class VariantTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String RESOURCE_DIRECTORY = "examples/batch/variantEvents/";
  private static final String TABLE_NAME = "variantEvents";
  private static final String EVENT_ID = "eventId";
  private static final String EVENT_TYPE = "eventType";
  private static final String PAYLOAD = "payload";
  private static final int NUM_DOCS = 5;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public Schema createSchema() {
    try (InputStream inputStream = openResource("variantEvents_schema.json")) {
      return Schema.fromInputStream(inputStream);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load the VARIANT quickstart schema", e);
    }
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    try (InputStream inputStream = openResource("variantEvents_offline_table_config.json")) {
      return JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load the VARIANT quickstart table config", e);
    }
  }

  @Override
  protected void setUpTable()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    File parquetFile = new File(_tempDir, "variantEvents_data.parquet");
    try (InputStream inputStream = openResource("rawdata/variantEvents_data.parquet")) {
      Files.copy(inputStream, parquetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
    ClusterIntegrationTestUtils.buildSegmentFromFile(parquetFile, tableConfig, schema, "0", _segmentDir, _tarDir,
        FileFormat.PARQUET);
    uploadSegments(getTableName(), _tarDir);
  }

  @Override
  public List<File> createAvroFiles() {
    throw new UnsupportedOperationException("VariantTypeTest ingests Parquet, not Avro");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMaterializedPathAndDirectExtraction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postVariantQuery(
        "SELECT " + EVENT_ID + ", " + EVENT_TYPE + " FROM " + TABLE_NAME
            + " WHERE " + EVENT_TYPE + " = 'checkout' ORDER BY " + EVENT_ID);
    assertNoExceptions(response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(),
        "[\"STRING\",\"STRING\"]");
    Assert.assertEquals(rows.size(), 2);
    Assert.assertEquals(rows.get(0).get(0).asText(), "evt-001");
    Assert.assertEquals(rows.get(0).get(1).asText(), "checkout");
    Assert.assertEquals(rows.get(1).get(0).asText(), "evt-003");
    Assert.assertEquals(rows.get(1).get(1).asText(), "checkout");

    response = postVariantQuery(
        "SELECT " + EVENT_ID + ", variantGet(" + PAYLOAD + ", '$.user.id', 'STRING'), "
            + "variantGet(" + PAYLOAD + ", '$.amount', 'DOUBLE') FROM " + TABLE_NAME
            + " WHERE " + EVENT_TYPE + " = 'checkout' ORDER BY " + EVENT_ID);
    assertNoExceptions(response);
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(),
        "[\"STRING\",\"STRING\",\"DOUBLE\"]");
    rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 2);
    Assert.assertEquals(rows.get(0).get(0).asText(), "evt-001");
    Assert.assertEquals(rows.get(0).get(1).asText(), "u-1");
    Assert.assertEquals(rows.get(0).get(2).asDouble(), 42.5);
    Assert.assertEquals(rows.get(1).get(0).asText(), "evt-003");
    Assert.assertEquals(rows.get(1).get(1).asText(), "u-3");
    Assert.assertEquals(rows.get(1).get(2).asDouble(), 19.0);

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0L,
          "The materialized eventType filter should use its inverted index");
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonProjectionAndNullStates(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postVariantQuery(
        "SELECT " + PAYLOAD + ", variantToJson(" + PAYLOAD + ") FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " = 'evt-001'");
    assertNoExceptions(response);
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(),
        "[\"VARIANT\",\"STRING\"]");
    JsonNode row = response.get("resultTable").get("rows").get(0);
    String json = row.get(0).asText();
    Assert.assertEquals(row.get(1).asText(), json);
    Assert.assertEquals(json,
        "{\"amount\":42.5,\"eventType\":\"checkout\",\"items\":[\"sku-1\",\"sku-2\"],"
            + "\"user\":{\"id\":\"u-1\"}}");
    JsonNode payload = JsonUtils.stringToJsonNode(json);
    Assert.assertEquals(payload.get("eventType").asText(), "checkout");
    Assert.assertEquals(payload.get("user").get("id").asText(), "u-1");
    Assert.assertEquals(payload.get("items").size(), 2);

    response = postVariantQuery(
        "SELECT " + EVENT_ID + ", variantExists(" + PAYLOAD + ", '$.coupon'), "
            + "isVariantNull(" + PAYLOAD + ", '$.coupon') FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " IN ('evt-001', 'evt-002', 'evt-003') ORDER BY " + EVENT_ID);
    assertNoExceptions(response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 3);
    Assert.assertFalse(rows.get(0).get(1).asBoolean());
    Assert.assertFalse(rows.get(0).get(2).asBoolean());
    Assert.assertTrue(rows.get(1).get(1).asBoolean());
    Assert.assertTrue(rows.get(1).get(2).asBoolean());
    Assert.assertTrue(rows.get(2).get(1).asBoolean());
    Assert.assertFalse(rows.get(2).get(2).asBoolean());

    response = postVariantQuery(
        "SELECT " + EVENT_ID + ", variantTypeOf(" + PAYLOAD + ", '$') FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " IN ('evt-004', 'evt-005') ORDER BY " + EVENT_ID);
    assertNoExceptions(response);
    rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 2);
    Assert.assertEquals(rows.get(0).get(1).asText(), "NULL");
    Assert.assertTrue(rows.get(1).get(1).isNull());

    response = postVariantQuery(
        "SELECT " + EVENT_ID + ", " + PAYLOAD + " FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " IN ('evt-004', 'evt-005') ORDER BY " + EVENT_ID);
    assertNoExceptions(response);
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(),
        "[\"STRING\",\"VARIANT\"]");
    rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.get(0).get(1).asText(), "null",
        "An encoded Variant null must render as JSON text");
    Assert.assertTrue(rows.get(1).get(1).isNull(), "A missing Parquet payload must remain SQL null");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSparkCompatibleFunctionSemantics(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postVariantQuery(
        "SELECT variant_get(" + PAYLOAD + ", '$.user'), "
            + "variant_get(" + PAYLOAD + ", '$.missing'), "
            + "variant_get(" + PAYLOAD + ", '$.items[1]', 'STRING'), "
            + "try_variant_get(" + PAYLOAD + ", '$.eventType', 'DOUBLE') "
            + "FROM " + TABLE_NAME + " WHERE " + EVENT_ID + " = 'evt-001'");
    assertNoExceptions(response);
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(),
        "[\"VARIANT\",\"VARIANT\",\"STRING\",\"DOUBLE\"]");
    JsonNode row = response.get("resultTable").get("rows").get(0);
    Assert.assertEquals(row.get(0).asText(), "{\"id\":\"u-1\"}");
    Assert.assertTrue(row.get(1).isNull(), "A missing path must return SQL null");
    Assert.assertEquals(row.get(2).asText(), "sku-2");
    Assert.assertTrue(row.get(3).isNull(), "try_variant_get must return SQL null for an incompatible cast");

    response = postVariantQuery(
        "SELECT variant_get(parse_json('{\"answer\":42}'), '$.answer', 'INT') "
            + "FROM " + TABLE_NAME + " LIMIT 1");
    assertNoExceptions(response);
    Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), 42);

    response = postVariantQuery(
        "SELECT is_variant_null(" + PAYLOAD + ") FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " = 'evt-005'");
    assertNoExceptions(response);
    JsonNode isVariantNull = response.get("resultTable").get("rows").get(0).get(0);
    Assert.assertFalse(isVariantNull.isNull(), "is_variant_null(SQL NULL) must return a non-null boolean");
    Assert.assertFalse(isVariantNull.asBoolean(),
        "SQL null is not an encoded Variant null");

    response = postVariantQuery(
        "SELECT variant_get(" + PAYLOAD + ", '$.eventType', 'DOUBLE') FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " = 'evt-001'");
    assertExceptionContains(response, "cannot convert variant", "double");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRawVariantInAndNotInAreRejected(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    for (String operator : List.of("IN", "NOT IN")) {
      JsonNode response = postVariantQuery(
          "SELECT " + EVENT_ID + " FROM " + TABLE_NAME + " WHERE " + PAYLOAD + " " + operator
              + " (parse_json('{\"candidate\":1}'), parse_json('{\"candidate\":2}'))");
      assertExceptionContains(response, "raw variant", "in");
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRawVariantOrderByIsRejected(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response =
        postVariantQuery("SELECT " + EVENT_ID + " FROM " + TABLE_NAME + " ORDER BY " + PAYLOAD + " LIMIT 5");
    assertExceptionContains(response, "raw variant", "order by");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRawVariantComparisonGroupingAndDistinctAreRejected(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postVariantQuery(
        "SELECT " + EVENT_ID + " FROM " + TABLE_NAME + " WHERE " + PAYLOAD
            + " = parse_json('{\"eventType\":\"checkout\"}')");
    assertExceptionContains(response, "raw variant", "comparison");
    for (String operator : List.of("IS DISTINCT FROM", "IS NOT DISTINCT FROM")) {
      response = postVariantQuery(
          "SELECT " + EVENT_ID + " FROM " + TABLE_NAME + " WHERE " + PAYLOAD + " " + operator
              + " parse_json(variant_to_json(" + PAYLOAD + "))");
      assertExceptionContains(response, "raw variant", "comparison");
    }
    response = postVariantQuery(
        "SELECT " + PAYLOAD + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + PAYLOAD);
    assertExceptionContains(response, "raw variant", "group by");
    response = postVariantQuery("SELECT DISTINCT " + PAYLOAD + " FROM " + TABLE_NAME);
    assertExceptionContains(response, "raw variant", useMultiStageQueryEngine ? "group by" : "distinct");

    for (String query : List.of(
        "SELECT " + EVENT_ID + " FROM " + TABLE_NAME + " WHERE variant_get(" + PAYLOAD
            + ", '$.eventType', 'STRING') = 'checkout'",
        "SELECT variant_get(" + PAYLOAD + ", '$.eventType', 'STRING'), COUNT(*) FROM " + TABLE_NAME
            + " GROUP BY variant_get(" + PAYLOAD + ", '$.eventType', 'STRING')",
        "SELECT DISTINCT variant_get(" + PAYLOAD + ", '$.eventType', 'STRING') FROM " + TABLE_NAME)) {
      assertNoExceptions(postVariantQuery(query));
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRawVariantAggregatesAreRejected(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    for (String aggregate : List.of("DISTINCTCOUNT", "DISTINCTCOUNTHLL", "DISTINCTCOUNTTHETASKETCH")) {
      JsonNode response = postVariantQuery("SELECT " + aggregate + "(" + PAYLOAD + ") FROM " + TABLE_NAME);
      assertExceptionContains(response, "raw variant", aggregate);
    }

    JsonNode response = postVariantQuery("SELECT COUNT(" + PAYLOAD + ") FROM " + TABLE_NAME);
    assertNoExceptions(response);
    Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), 4L,
        "COUNT is raw-value-independent and must retain SQL-null semantics");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testVariantFunctionsRequireQueryNullHandling(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postQuery(
        "SELECT variant_get(" + PAYLOAD + ", '$.eventType', 'STRING') FROM " + TABLE_NAME + " LIMIT 1");
    assertExceptionContains(response, "requires query null handling");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRawVariantProjectionRequiresQueryNullHandling(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postQuery(
        "SELECT " + PAYLOAD + " FROM " + TABLE_NAME
            + " WHERE " + EVENT_ID + " IN ('evt-001', 'evt-005') ORDER BY " + EVENT_ID);
    assertExceptionContains(response, "raw variant", "requires query null handling");

    if (!useMultiStageQueryEngine) {
      for (String predicate : List.of("1 = 0", "eventTime < 0")) {
        response = postQuery("SELECT " + PAYLOAD + " FROM " + TABLE_NAME + " WHERE " + predicate);
        assertExceptionContains(response, "raw variant", "requires query null handling");
      }
    }
  }

  @Test
  public void testRawVariantJoinIsRejectedButTypedPathJoinWorks()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String rawJoin = "SELECT leftTable." + EVENT_ID + " FROM " + TABLE_NAME + " leftTable JOIN " + TABLE_NAME
        + " rightTable ON leftTable." + PAYLOAD + " = rightTable." + PAYLOAD;
    JsonNode response = postVariantQuery(rawJoin);
    assertExceptionContains(response, "raw variant", "join");

    String typedJoin = "SELECT leftTable." + EVENT_ID + ", rightTable." + EVENT_ID + " FROM " + TABLE_NAME
        + " leftTable JOIN " + TABLE_NAME + " rightTable ON variant_get(leftTable." + PAYLOAD
        + ", '$.eventType', 'STRING') = variant_get(rightTable." + PAYLOAD + ", '$.eventType', 'STRING')"
        + " WHERE leftTable." + EVENT_ID + " = 'evt-001' AND rightTable." + EVENT_ID + " = 'evt-003'";
    response = postVariantQuery(typedJoin);
    assertNoExceptions(response);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 1);
    Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asText(), "evt-001");
    Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(1).asText(), "evt-003");
  }

  @Test
  public void testRawVariantWindowKeysAreRejectedButTypedPathWorks()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    for (String window : List.of(
        "COUNT(*) OVER (PARTITION BY " + PAYLOAD + ")",
        "COUNT(*) OVER (ORDER BY " + PAYLOAD + ")")) {
      JsonNode response = postVariantQuery("SELECT " + window + " FROM " + TABLE_NAME);
      assertExceptionContains(response, "raw variant", "window");
    }

    JsonNode response = postVariantQuery(
        "SELECT " + EVENT_ID + ", variant_get(" + PAYLOAD + ", '$.eventType', 'STRING'), "
            + "COUNT(*) OVER (PARTITION BY variant_get(" + PAYLOAD
            + ", '$.eventType', 'STRING')) FROM " + TABLE_NAME + " ORDER BY " + EVENT_ID);
    assertNoExceptions(response);
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(),
        "[\"STRING\",\"STRING\",\"LONG\"]");
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), NUM_DOCS);
    Assert.assertEquals(rows.get(0).get(0).asText(), "evt-001");
    Assert.assertEquals(rows.get(0).get(1).asText(), "checkout");
    Assert.assertEquals(rows.get(0).get(2).asLong(), 2L);
    Assert.assertEquals(rows.get(1).get(0).asText(), "evt-002");
    Assert.assertEquals(rows.get(1).get(1).asText(), "view");
    Assert.assertEquals(rows.get(1).get(2).asLong(), 1L);
    Assert.assertEquals(rows.get(2).get(0).asText(), "evt-003");
    Assert.assertEquals(rows.get(2).get(1).asText(), "checkout");
    Assert.assertEquals(rows.get(2).get(2).asLong(), 2L);
    Assert.assertEquals(rows.get(3).get(0).asText(), "evt-004");
    Assert.assertTrue(rows.get(3).get(1).isNull(), "Variant null must extract to the SQL-null partition");
    Assert.assertEquals(rows.get(3).get(2).asLong(), 2L);
    Assert.assertEquals(rows.get(4).get(0).asText(), "evt-005");
    Assert.assertTrue(rows.get(4).get(1).isNull(), "SQL null must remain in the SQL-null partition");
    Assert.assertEquals(rows.get(4).get(2).asLong(), 2L);
  }

  @Test
  public void testRawVariantSetOperationsAreRejected()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String left = "SELECT " + PAYLOAD + " FROM " + TABLE_NAME + " WHERE " + EVENT_ID + " = 'evt-001'";
    String right = "SELECT " + PAYLOAD + " FROM " + TABLE_NAME + " WHERE " + EVENT_ID + " = 'evt-002'";

    for (String operator : List.of("UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
      JsonNode response = postVariantQuery(left + " " + operator + " " + right);
      assertExceptionContains(response, "raw variant", "extract a typed path");
    }

    JsonNode response = postVariantQuery(left + " UNION ALL " + right);
    assertNoExceptions(response);
    Assert.assertEquals(
        response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(), "[\"VARIANT\"]");
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 2);
  }

  private static InputStream openResource(String relativePath) {
    String resourcePath = RESOURCE_DIRECTORY + relativePath;
    InputStream inputStream = VariantTypeTest.class.getClassLoader().getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw new IllegalStateException("Missing VARIANT quickstart resource: " + resourcePath);
    }
    return inputStream;
  }

  private static void assertNoExceptions(JsonNode response) {
    Assert.assertEquals(response.get("exceptions").size(), 0, response.toPrettyString());
  }

  private static void assertExceptionContains(JsonNode response, String... expectedFragments) {
    JsonNode exceptions = response.get("exceptions");
    Assert.assertNotNull(exceptions, response.toPrettyString());
    Assert.assertFalse(exceptions.isEmpty(), response.toPrettyString());
    String exceptionText = exceptions.toString().toLowerCase(Locale.ROOT);
    for (String expectedFragment : expectedFragments) {
      Assert.assertTrue(exceptionText.contains(expectedFragment.toLowerCase(Locale.ROOT)),
          "Expected exception text to contain '" + expectedFragment + "': " + response.toPrettyString());
    }
  }

  private JsonNode postVariantQuery(String query)
      throws Exception {
    return postQuery("SET enableNullHandling=true; " + query);
  }
}
