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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JSONClusterIntegrationTest extends BaseClusterIntegrationTest {

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String JSON_COLUMN = "jsonColumn";
  private static final String JSON_COLUMN_WITHOUT_INDEX = "jsonColumnWithoutIndex";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    String rawTableName = getTableName();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
        .addSingleValueDimension(JSON_COLUMN_WITHOUT_INDEX, FieldSpec.DataType.JSON).build();
    addSchema(schema);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName)
            .setJsonIndexColumns(Collections.singletonList(JSON_COLUMN)).build();

    addTableConfig(tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentFromRows(getRecords(), tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(rawTableName, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(60_000);
  }

  protected long getCountStarResult() {
    return 15;
  }

  private static List<GenericRow> getRecords() {
    List<GenericRow> records = new ArrayList<>();
    records.add(createRecord(1, 1, "daffy duck",
        "{\"name\": {\"first\": \"daffy\", \"last\": \"duck\"}, \"id\": 101, \"data\": [\"a\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(2, 2, "mickey mouse",
        "{\"name\": {\"first\": \"mickey\", \"last\": \"mouse\"}, \"id\": 111, \"data\": [\"e\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(3, 3, "donald duck",
        "{\"name\": {\"first\": \"donald\", \"last\": \"duck\"}, \"id\": 121, \"data\": [\"f\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(4, 4, "scrooge mcduck",
        "{\"name\": {\"first\": \"scrooge\", \"last\": \"mcduck\"}, \"id\": 131, \"data\": [\"g\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(5, 5, "minnie mouse",
        "{\"name\": {\"first\": \"minnie\", \"last\": \"mouse\"}, \"id\": 141, \"data\": [\"h\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(6, 6, "daisy duck",
        "{\"name\": {\"first\": \"daisy\", \"last\": \"duck\"}, \"id\": 161.5, \"data\": [\"i\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(7, 7, "pluto dog",
        "{\"name\": {\"first\": \"pluto\", \"last\": \"dog\"}, \"id\": 161, \"data\": [\"j\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(8, 8, "goofy dwag",
        "{\"name\": {\"first\": \"goofy\", \"last\": \"dwag\"}, \"id\": 171, \"data\": [\"k\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(9, 9, "ludwik von drake",
        "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, \"data\": [\"l\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(10, 10, "nested array",
        "{\"name\":{\"first\":\"nested\",\"last\":\"array\"},\"id\":111,\"data\":[{\"e\":[{\"x\":[{\"i1\":1,"
            + "\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":1,\"i2\":2}]}]},{\"b\":[{\"x\":[{\"i1\":1,"
            + "\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":10,\"i2\":20}]}]}]}"));
    records.add(createRecord(11, 11, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(12, 12, "multi-dimensional-2 array",
        "{\"name\": {\"first\": \"multi-dimensional-2\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, 13, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, 13, "days",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"days\": 111}"));
    records.add(createRecord(14, 14, "top level array", "[{\"i1\":1,\"i2\":2}, {\"i1\":3,\"i2\":4}]"));

    return records;
  }

  private static GenericRow createRecord(int intValue, long longValue, String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(LONG_COLUMN, longValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);
    record.putValue(JSON_COLUMN_WITHOUT_INDEX, jsonValue);

    return record;
  }

  @Test
  public void testPathExpressionQueries()
      throws Exception {
    JsonNode queryResponse = postSqlQuery(
        "SELECT jsonColumn.name.first, jsonColumn.data[1] FROM mytable WHERE " + "jsonColumn.name.last = 'duck'");
    Assert.assertEquals(queryResponse.get("resultTable").get("rows").toString(),
        "[[\"daffy\",\"b\"],[\"donald\",\"b\"],[\"daisy\",\"b\"]]");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
