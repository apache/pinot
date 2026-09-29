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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CursorAwareBrokerResponseTest {

  @Test
  public void testCursorAwareBrokerResponseWithAllFields() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String jsonResponse = "{"
        + "\"requestId\": \"test-request-123\","
        + "\"brokerId\": \"broker-1\","
        + "\"offset\": 100,"
        + "\"numRows\": 50,"
        + "\"numRowsResultSet\": 1000,"
        + "\"cursorResultWriteTimeMs\": 1234567890,"
        + "\"submissionTimeMs\": 1234567800,"
        + "\"expirationTimeMs\": 1234567900,"
        + "\"brokerHost\": \"localhost\","
        + "\"brokerPort\": 8099,"
        + "\"bytesWritten\": 2048,"
        + "\"cursorFetchTimeMs\": 150,"
        + "\"resultTable\": {"
        + "  \"dataSchema\": {"
        + "    \"columnNames\": [\"col1\", \"col2\"],"
        + "    \"columnDataTypes\": [\"STRING\", \"INT\"]"
        + "  },"
        + "  \"rows\": [[\"value1\", 123], [\"value2\", 456]]"
        + "}"
        + "}";

    JsonNode jsonNode = mapper.readTree(jsonResponse);
    CursorAwareBrokerResponse response = CursorAwareBrokerResponse.fromJson(jsonNode);

    // Test cursor-specific fields
    Assert.assertEquals(response.getOffset(), Long.valueOf(100));
    Assert.assertEquals(response.getNumRows(), Integer.valueOf(50));
    Assert.assertEquals(response.getNumRowsResultSet(), Long.valueOf(1000));
    Assert.assertEquals(response.getCursorResultWriteTimeMs(), Long.valueOf(1234567890));
    Assert.assertEquals(response.getSubmissionTimeMs(), Long.valueOf(1234567800));
    Assert.assertEquals(response.getExpirationTimeMs(), Long.valueOf(1234567900));
    Assert.assertEquals(response.getBrokerHost(), "localhost");
    Assert.assertEquals(response.getBrokerPort(), Integer.valueOf(8099));
    Assert.assertEquals(response.getBytesWritten(), Long.valueOf(2048));
    Assert.assertEquals(response.getCursorFetchTimeMs(), Long.valueOf(150));

    // Test inherited BrokerResponse fields
    Assert.assertEquals(response.getRequestId(), "test-request-123");
    Assert.assertEquals(response.getBrokerId(), "broker-1");
    Assert.assertNotNull(response.getResultTable());
  }

  @Test
  public void testCursorAwareBrokerResponseWithNullFields() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String jsonResponse = "{"
        + "\"requestId\": \"test-request-456\","
        + "\"offset\": null,"
        + "\"numRows\": null,"
        + "\"numRowsResultSet\": null,"
        + "\"brokerHost\": null"
        + "}";

    JsonNode jsonNode = mapper.readTree(jsonResponse);
    CursorAwareBrokerResponse response = CursorAwareBrokerResponse.fromJson(jsonNode);

    // Test null fields are handled correctly
    Assert.assertNull(response.getOffset());
    Assert.assertNull(response.getNumRows());
    Assert.assertNull(response.getNumRowsResultSet());
    Assert.assertNull(response.getBrokerHost());
    Assert.assertNull(response.getBrokerPort());
    Assert.assertNull(response.getCursorResultWriteTimeMs());
    Assert.assertNull(response.getSubmissionTimeMs());
    Assert.assertNull(response.getExpirationTimeMs());
    Assert.assertNull(response.getBytesWritten());
    Assert.assertNull(response.getCursorFetchTimeMs());
  }

  @Test
  public void testCursorAwareBrokerResponseWithNullInput() {
    CursorAwareBrokerResponse response = CursorAwareBrokerResponse.fromJson(null);

    // All cursor fields should be null
    Assert.assertNull(response.getOffset());
    Assert.assertNull(response.getNumRows());
    Assert.assertNull(response.getNumRowsResultSet());
    Assert.assertNull(response.getCursorResultWriteTimeMs());
    Assert.assertNull(response.getExpirationTimeMs());
    Assert.assertNull(response.getSubmissionTimeMs());
    Assert.assertNull(response.getBrokerHost());
    Assert.assertNull(response.getBrokerPort());
    Assert.assertNull(response.getBytesWritten());
    Assert.assertNull(response.getCursorFetchTimeMs());
  }

  @Test
  public void testCursorAwareBrokerResponseWithMissingFields() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String jsonResponse = "{"
        + "\"requestId\": \"test-request-789\","
        + "\"brokerId\": \"broker-2\""
        + "}";

    JsonNode jsonNode = mapper.readTree(jsonResponse);
    CursorAwareBrokerResponse response = CursorAwareBrokerResponse.fromJson(jsonNode);

    // Test missing fields are handled as null
    Assert.assertNull(response.getOffset());
    Assert.assertNull(response.getNumRows());
    Assert.assertNull(response.getNumRowsResultSet());
    Assert.assertNull(response.getBrokerHost());
    Assert.assertNull(response.getBrokerPort());

    // Test inherited fields work
    Assert.assertEquals(response.getRequestId(), "test-request-789");
    Assert.assertEquals(response.getBrokerId(), "broker-2");
  }

  @Test
  public void testCursorAwareBrokerResponseWithZeroValues() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String jsonResponse = "{"
        + "\"requestId\": \"test-request-000\","
        + "\"offset\": 0,"
        + "\"numRows\": 0,"
        + "\"numRowsResultSet\": 0,"
        + "\"brokerPort\": 0,"
        + "\"bytesWritten\": 0"
        + "}";

    JsonNode jsonNode = mapper.readTree(jsonResponse);
    CursorAwareBrokerResponse response = CursorAwareBrokerResponse.fromJson(jsonNode);

    // Test zero values are preserved
    Assert.assertEquals(response.getOffset(), Long.valueOf(0));
    Assert.assertEquals(response.getNumRows(), Integer.valueOf(0));
    Assert.assertEquals(response.getNumRowsResultSet(), Long.valueOf(0));
    Assert.assertEquals(response.getBrokerPort(), Integer.valueOf(0));
    Assert.assertEquals(response.getBytesWritten(), Long.valueOf(0));
  }
}
