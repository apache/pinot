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
package org.apache.pinot.integration.tests.realtime.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Function;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.activation.UnsupportedDataTypeException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

public class RealtimeKinesisIntegrationTest extends BaseKinesisIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeKinesisIntegrationTest.class);

  private static final int NUM_SHARDS = 10;

  // Localstack Kinesis doesn't support large rows.
  // So, this airlineStats data file consists of only few fields and rows from the original data
  private static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  private static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";

  private long _totalRecordsPushedInStream = 0;

  List<String> _h2FieldNameAndTypes = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    super.setUp();

    // Create new stream
    createStream(NUM_SHARDS);

    // Create and upload the schema and table config
    addSchema(createSchema(SCHEMA_FILE_PATH));
    addTableConfig(createRealtimeTableConfig(null));

    createH2ConnectionAndTable();

    // Push data into Kinesis
    publishRecordsToKinesis();

    // Wait for all documents loaded
    waitForAllDocsLoadedKinesis(120_000L);
  }

  protected void waitForAllDocsLoadedKinesis(long timeoutMs)
      throws Exception {
    waitForAllDocsLoadedKinesis(timeoutMs, true);
  }

  protected void waitForAllDocsLoadedKinesis(long timeoutMs, boolean raiseError) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getCurrentCountStarResult() >= _totalRecordsPushedInStream;
        } catch (Exception e) {
          LOGGER.warn("Could not fetch current number of rows in pinot table {}", getTableName(), e);
          return null;
        }
      }
    }, 1000L, timeoutMs, "Failed to load " + _totalRecordsPushedInStream + " documents", raiseError);
  }

  @Override
  public List<String> getNoDictionaryColumns() {
    return Collections.emptyList();
  }

  @Override
  public String getSortedColumn() {
    return null;
  }

  private void publishRecordsToKinesis() {
    try {
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < _h2FieldNameAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + getTableName() + " VALUES (" + params.toString() + ")");

      InputStream inputStream =
          RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          JsonNode data = JsonUtils.stringToJsonNode(line);

          PutRecordResponse putRecordResponse = putRecord(line, data.get("Origin").textValue());
          if (putRecordResponse.sdkHttpResponse().statusCode() == 200) {
            if (StringUtils.isNotBlank(putRecordResponse.sequenceNumber()) && StringUtils.isNotBlank(
                putRecordResponse.shardId())) {
              _totalRecordsPushedInStream++;

              int fieldIndex = 1;
              for (String fieldNameAndDatatype : _h2FieldNameAndTypes) {
                String[] fieldNameAndDatatypeList = fieldNameAndDatatype.split(" ");
                String fieldName = fieldNameAndDatatypeList[0];
                String h2DataType = fieldNameAndDatatypeList[1];
                switch (h2DataType) {
                  case "int": {
                    h2Statement.setObject(fieldIndex++, data.get(fieldName).intValue());
                    break;
                  }
                  case "varchar(128)": {
                    h2Statement.setObject(fieldIndex++, data.get(fieldName).textValue());
                    break;
                  }
                  default:
                    break;
                }
              }
              h2Statement.execute();
            }
          }
        }
      }

      inputStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Could not publish records to Kinesis Stream", e);
    }
  }

  @Test
  public void testRecords()
      throws Exception {
    Assert.assertNotEquals(_totalRecordsPushedInStream, 0);

    ResultSet pinotResultSet =
        getPinotConnection().execute("SELECT * FROM " + getTableName() + " ORDER BY Origin LIMIT 10000")
            .getResultSet(0);

    Assert.assertNotEquals(pinotResultSet.getRowCount(), 0);

    Statement h2statement =
        _h2Connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
    h2statement.execute("SELECT * FROM " + getTableName() + " ORDER BY Origin");
    java.sql.ResultSet h2ResultSet = h2statement.getResultSet();

    Assert.assertFalse(h2ResultSet.isLast());

    h2ResultSet.beforeFirst();
    int row = 0;
    Map<String, Integer> columnToIndex = new HashMap<>();
    for (int i = 0; i < _h2FieldNameAndTypes.size(); i++) {
      columnToIndex.put(pinotResultSet.getColumnName(i), i);
    }

    while (h2ResultSet.next()) {

      for (String fieldNameAndDatatype : _h2FieldNameAndTypes) {
        String[] fieldNameAndDatatypeList = fieldNameAndDatatype.split(" ");
        String fieldName = fieldNameAndDatatypeList[0];
        String h2DataType = fieldNameAndDatatypeList[1];
        switch (h2DataType) {
          case "int": {
            int expectedValue = h2ResultSet.getInt(fieldName);
            int actualValue = pinotResultSet.getInt(row, columnToIndex.get(fieldName));
            Assert.assertEquals(expectedValue, actualValue);
            break;
          }
          case "varchar(128)": {
            String expectedValue = h2ResultSet.getString(fieldName);
            String actualValue = pinotResultSet.getString(row, columnToIndex.get(fieldName));
            Assert.assertEquals(expectedValue, actualValue);
            break;
          }
          default:
            break;
        }
      }

      row++;

      if (row >= pinotResultSet.getRowCount()) {
        int cnt = 0;
        while (h2ResultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(cnt, 0);
        break;
      }
    }
  }

  @Test
  public void testCountRecords() {
    long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + getTableName()).getResultSet(0).getLong(0);
    Assert.assertEquals(count, _totalRecordsPushedInStream);
  }

  public void createH2ConnectionAndTable()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
    _h2Connection.prepareCall("DROP TABLE IF EXISTS " + getTableName()).execute();
    _h2FieldNameAndTypes = new ArrayList<>();

    InputStream inputStream = RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);

    String line;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      while ((line = br.readLine()) != null) {
        break;
      }
    } finally {
      inputStream.close();
    }

    if (StringUtils.isNotBlank(line)) {
      JsonNode dataObject = JsonUtils.stringToJsonNode(line);

      Iterator<Map.Entry<String, JsonNode>> fieldIterator = dataObject.fields();
      while (fieldIterator.hasNext()) {
        Map.Entry<String, JsonNode> field = fieldIterator.next();
        String fieldName = field.getKey();
        JsonNodeType fieldDataType = field.getValue().getNodeType();

        String h2DataType;

        switch (fieldDataType) {
          case NUMBER: {
            h2DataType = "int";
            break;
          }
          case STRING: {
            h2DataType = "varchar(128)";
            break;
          }
          case BOOLEAN: {
            h2DataType = "boolean";
            break;
          }
          default: {
            throw new UnsupportedDataTypeException(
                "Kinesis Integration test doesn't support datatype: " + fieldDataType.name());
          }
        }

        _h2FieldNameAndTypes.add(fieldName + " " + h2DataType);
      }
    }

    _h2Connection.prepareCall("CREATE TABLE " + getTableName() + "(" + StringUtil.join(",",
        _h2FieldNameAndTypes.toArray(new String[_h2FieldNameAndTypes.size()])) + ")").execute();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    super.tearDown();
  }

}
