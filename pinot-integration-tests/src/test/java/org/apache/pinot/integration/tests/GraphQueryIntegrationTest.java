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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for the experimental graph query feature with distributed data.
 *
 * <p>Creates a standalone cluster with 2 servers, loads vertex and edge tables
 * with multiple segments each, then validates graph queries produce correct results
 * when data is distributed across servers.</p>
 *
 * <p>Graph (50 users, 200 follow edges):
 * <ul>
 *   <li>Users 1..50 with names user_1..user_50</li>
 *   <li>Each user i follows (i%50)+1, (i%50)+2, ..., (i%50)+4</li>
 *   <li>Data split across 4 Avro files per table</li>
 * </ul>
 * </p>
 */
public class GraphQueryIntegrationTest extends BaseClusterIntegrationTest {

  private static final String USERS_TABLE = "graphUsers";
  private static final String FOLLOWS_TABLE = "graphFollows";

  private static final int NUM_USERS = 50;
  private static final int FOLLOWS_PER_USER = 4;
  private static final int NUM_FOLLOWS = NUM_USERS * FOLLOWS_PER_USER;
  private static final int NUM_AVRO_FILES = 4;

  private static final String GRAPH_SCHEMA_JSON = "{"
      + "\"vertexLabels\": {"
      + "  \"User\": {"
      + "    \"tableName\": \"graphUsers\","
      + "    \"primaryKey\": \"user_id\","
      + "    \"properties\": {\"id\": \"user_id\", \"name\": \"user_name\"}"
      + "  }"
      + "},"
      + "\"edgeLabels\": {"
      + "  \"FOLLOWS\": {"
      + "    \"tableName\": \"graphFollows\","
      + "    \"sourceVertexLabel\": \"User\","
      + "    \"sourceKey\": \"follower_id\","
      + "    \"targetVertexLabel\": \"User\","
      + "    \"targetKey\": \"followee_id\","
      + "    \"properties\": {}"
      + "  }"
      + "}"
      + "}";

  private final Map<Integer, Set<Integer>> _expectedFollowees = new HashMap<>();
  private final Map<Integer, Set<Integer>> _expectedFollowers = new HashMap<>();

  public GraphQueryIntegrationTest() {
    for (int i = 1; i <= NUM_USERS; i++) {
      _expectedFollowees.put(i, new HashSet<>());
      _expectedFollowers.put(i, new HashSet<>());
    }
    for (int i = 1; i <= NUM_USERS; i++) {
      for (int j = 1; j <= FOLLOWS_PER_USER; j++) {
        int target = ((i - 1 + j) % NUM_USERS) + 1;
        _expectedFollowees.get(i).add(target);
        _expectedFollowers.get(target).add(i);
      }
    }
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Graph.CONFIG_OF_GRAPH_QUERY_ENABLED, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start cluster with 2 servers for distributed execution
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Set up vertex table
    Schema usersSchema = new Schema.SchemaBuilder().setSchemaName(USERS_TABLE)
        .addSingleValueDimension("user_id", FieldSpec.DataType.INT)
        .addSingleValueDimension("user_name", FieldSpec.DataType.STRING)
        .build();
    addSchema(usersSchema);
    TableConfig usersConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(USERS_TABLE).build();
    addTableConfig(usersConfig);

    List<File> usersAvroFiles = generateUsersAvroFiles();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(usersAvroFiles, usersConfig, usersSchema,
        0, _segmentDir, _tarDir);
    uploadSegments(USERS_TABLE, _tarDir);

    // Wait for users to load
    TestUtils.waitForCondition(
        () -> getCurrentCountStarResult(USERS_TABLE) == NUM_USERS,
        100L, 120_000L, "Failed to load user records",
        Duration.ofSeconds(10));

    // Set up edge table
    Schema followsSchema = new Schema.SchemaBuilder().setSchemaName(FOLLOWS_TABLE)
        .addSingleValueDimension("follower_id", FieldSpec.DataType.INT)
        .addSingleValueDimension("followee_id", FieldSpec.DataType.INT)
        .build();
    addSchema(followsSchema);
    TableConfig followsConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(FOLLOWS_TABLE).build();
    addTableConfig(followsConfig);

    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);
    List<File> followsAvroFiles = generateFollowsAvroFiles();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(followsAvroFiles, followsConfig, followsSchema,
        0, _segmentDir, _tarDir);
    uploadSegments(FOLLOWS_TABLE, _tarDir);

    // Wait for follows to load
    TestUtils.waitForCondition(
        () -> getCurrentCountStarResult(FOLLOWS_TABLE) == NUM_FOLLOWS,
        100L, 120_000L, "Failed to load follow records",
        Duration.ofSeconds(10));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(USERS_TABLE);
    dropOfflineTable(FOLLOWS_TABLE);
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private List<File> generateUsersAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("User", null, null, false);
    avroSchema.setFields(List.of(
        new Field("user_id", org.apache.avro.Schema.create(Type.INT), null, null),
        new Field("user_name", org.apache.avro.Schema.create(Type.STRING), null, null)
    ));

    List<File> avroFiles = new ArrayList<>();
    List<DataFileWriter<GenericData.Record>> writers = new ArrayList<>();
    for (int s = 0; s < NUM_AVRO_FILES; s++) {
      File avroFile = new File(_tempDir, "users-" + s + ".avro");
      avroFiles.add(avroFile);
      DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
      writer.create(avroSchema, avroFile);
      writers.add(writer);
    }
    for (int i = 1; i <= NUM_USERS; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("user_id", i);
      record.put("user_name", "user_" + i);
      writers.get((i - 1) % NUM_AVRO_FILES).append(record);
    }
    for (DataFileWriter<GenericData.Record> writer : writers) {
      writer.close();
    }
    return avroFiles;
  }

  private List<File> generateFollowsAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("Follows", null, null, false);
    avroSchema.setFields(List.of(
        new Field("follower_id", org.apache.avro.Schema.create(Type.INT), null, null),
        new Field("followee_id", org.apache.avro.Schema.create(Type.INT), null, null)
    ));

    List<File> avroFiles = new ArrayList<>();
    List<DataFileWriter<GenericData.Record>> writers = new ArrayList<>();
    for (int s = 0; s < NUM_AVRO_FILES; s++) {
      File avroFile = new File(_tempDir, "follows-" + s + ".avro");
      avroFiles.add(avroFile);
      DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
      writer.create(avroSchema, avroFile);
      writers.add(writer);
    }
    int edgeIdx = 0;
    for (int i = 1; i <= NUM_USERS; i++) {
      for (int j = 1; j <= FOLLOWS_PER_USER; j++) {
        int target = ((i - 1 + j) % NUM_USERS) + 1;
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("follower_id", i);
        record.put("followee_id", target);
        writers.get(edgeIdx % NUM_AVRO_FILES).append(record);
        edgeIdx++;
      }
    }
    for (DataFileWriter<GenericData.Record> writer : writers) {
      writer.close();
    }
    return avroFiles;
  }

  // ---- Data verification tests ----

  @Test
  public void testTablesLoaded()
      throws Exception {
    // Use SSE for simple count queries
    setUseMultiStageQueryEngine(false);
    JsonNode usersResponse = postQuery("SELECT COUNT(*) FROM " + USERS_TABLE);
    assertEquals(usersResponse.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_USERS);

    JsonNode followsResponse = postQuery("SELECT COUNT(*) FROM " + FOLLOWS_TABLE);
    assertEquals(followsResponse.get("resultTable").get("rows").get(0).get(0).asLong(), (long) NUM_FOLLOWS);
  }

  @Test
  public void testDistributedSqlJoinWorks()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String sql = "SET useMultistageEngine=true; "
        + "SELECT b.user_id FROM graphFollows AS e"
        + " JOIN graphUsers AS a ON e.follower_id = a.user_id"
        + " JOIN graphUsers AS b ON e.followee_id = b.user_id"
        + " WHERE a.user_id = 1 LIMIT 100";
    JsonNode response = postQuery(sql);
    assertNotNull(response.get("resultTable"),
        "Expected resultTable in response, got: " + response);
    Set<Integer> followees = collectIntColumn(response.get("resultTable").get("rows"), 0);
    assertEquals(followees, _expectedFollowees.get(1),
        "SQL JOIN should return correct followees for user 1");
  }

  // ---- Graph query tests ----

  @Test
  public void testGraphQueryBasicOneHop()
      throws Exception {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    JsonNode response = postGraphQuery(cypher);
    Set<Integer> followees = collectIntColumn(response.get("resultTable").get("rows"), 0);
    assertEquals(followees, _expectedFollowees.get(1));
  }

  @Test
  public void testGraphQueryMultipleUsers()
      throws Exception {
    for (int userId : List.of(1, 13, 25, 37, 49)) {
      String cypher = String.format(
          "MATCH (a:User {id: %d})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100", userId);
      JsonNode response = postGraphQuery(cypher);
      Set<Integer> followees = collectIntColumn(response.get("resultTable").get("rows"), 0);
      assertEquals(followees, _expectedFollowees.get(userId),
          "User " + userId + " followees mismatch");
    }
  }

  @Test
  public void testGraphQueryIncomingEdge()
      throws Exception {
    for (int userId : List.of(1, 25, 50)) {
      String cypher = String.format(
          "MATCH (a:User {id: %d})<-[:FOLLOWS]-(b:User) RETURN b.id LIMIT 100", userId);
      JsonNode response = postGraphQuery(cypher);
      Set<Integer> followers = collectIntColumn(response.get("resultTable").get("rows"), 0);
      assertEquals(followers, _expectedFollowers.get(userId),
          "User " + userId + " followers mismatch");
    }
  }

  @Test
  public void testGraphQueryWithLimit()
      throws Exception {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 2";
    JsonNode response = postGraphQuery(cypher);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 2, "LIMIT 2 should return exactly 2 results");
    Set<Integer> followees = collectIntColumn(rows, 0);
    assertTrue(_expectedFollowees.get(1).containsAll(followees));
  }

  @Test
  public void testGraphQueryMultipleReturnColumns()
      throws Exception {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id, b.name LIMIT 100";
    JsonNode response = postGraphQuery(cypher);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), FOLLOWS_PER_USER);
    for (int i = 0; i < rows.size(); i++) {
      int userId = rows.get(i).get(0).asInt();
      assertEquals(rows.get(i).get(1).asText(), "user_" + userId);
    }
  }

  @Test
  public void testGraphQueryNoResults()
      throws Exception {
    String cypher = "MATCH (a:User {id: 999})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    JsonNode response = postGraphQuery(cypher);
    assertEquals(response.get("resultTable").get("rows").size(), 0);
  }

  // ---- Error handling tests ----

  @Test
  public void testGraphQueryInvalidCypher()
      throws Exception {
    try {
      postGraphQuery("CREATE (n:User {id: 1})");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("400") || e.getMessage().contains("Unsupported"),
          "Unsupported CREATE should return error: " + e.getMessage());
    }
  }

  @Test
  public void testGraphQueryMissingSchema()
      throws Exception {
    String url = getBrokerBaseApiUrl() + "/query/graph";
    String payload = "{\"query\": \"MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 10\"}";
    try {
      sendPostRequest(url, payload);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("400") || e.getMessage().contains("graphSchema"),
          "Missing schema should return 400: " + e.getMessage());
    }
  }

  // ---- Helpers ----

  private JsonNode postGraphQuery(String cypherQuery)
      throws Exception {
    String url = getBrokerBaseApiUrl() + "/query/graph";
    String payload = "{\"query\": " + JsonUtils.objectToString(cypherQuery)
        + ", \"graphSchema\": " + GRAPH_SCHEMA_JSON + "}";
    String response = sendPostRequest(url, payload);
    return JsonUtils.stringToJsonNode(response);
  }

  private static Set<Integer> collectIntColumn(JsonNode rows, int colIndex) {
    Set<Integer> values = new HashSet<>();
    if (rows != null) {
      for (int i = 0; i < rows.size(); i++) {
        values.add(rows.get(i).get(colIndex).asInt());
      }
    }
    return values;
  }
}
