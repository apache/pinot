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

/**
 * Unit tests for CursorResultSetGroup class.
 */
public class CursorResultSetGroupTest {

  @Test
  public void testConstructorWithValidParameters() {
    // Create a mock CursorAwareBrokerResponse
    ObjectMapper mapper = new ObjectMapper();
    JsonNode mockResponse = mapper.createObjectNode()
        .put("offset", 0)
        .put("numRows", 10)
        .put("numRowsResultSet", 100);

    CursorAwareBrokerResponse cursorResponse = CursorAwareBrokerResponse.fromJson(mockResponse);

    CursorResultSetGroup cursorResultSetGroup = new CursorResultSetGroup(cursorResponse);
    Assert.assertNotNull(cursorResultSetGroup);
  }

  @Test
  public void testGetCursorFields() {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode mockResponse = mapper.createObjectNode()
        .put("offset", 5)
        .put("numRows", 15)
        .put("numRowsResultSet", 200);

    CursorAwareBrokerResponse cursorResponse = CursorAwareBrokerResponse.fromJson(mockResponse);

    CursorResultSetGroup cursorResultSetGroup = new CursorResultSetGroup(cursorResponse);
    Assert.assertEquals(cursorResultSetGroup.getOffset(), Long.valueOf(5));
    Assert.assertEquals(cursorResultSetGroup.getPageSize(), 15);
    Assert.assertEquals(cursorResultSetGroup.getNumRowsResultSet(), Long.valueOf(200));
  }


  @Test
  public void testResultSetDelegation() {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode mockResponse = mapper.createObjectNode();

    CursorAwareBrokerResponse cursorResponse = CursorAwareBrokerResponse.fromJson(mockResponse);

    CursorResultSetGroup cursorResultSetGroup = new CursorResultSetGroup(cursorResponse);
    Assert.assertEquals(cursorResultSetGroup.getResultSetCount(), 0);
  }


  @Test
  public void testMetadataExtraction() {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode mockResponse = mapper.createObjectNode()
        .put("offset", 10)
        .put("numRows", 20)
        .put("numRowsResultSet", 500);

    CursorAwareBrokerResponse cursorResponse = CursorAwareBrokerResponse.fromJson(mockResponse);

    CursorResultSetGroup cursorResultSetGroup = new CursorResultSetGroup(cursorResponse);
    Assert.assertEquals(cursorResultSetGroup.getOffset(), Long.valueOf(10));
    Assert.assertEquals(cursorResultSetGroup.getPageSize(), 20);
    Assert.assertEquals(cursorResultSetGroup.getNumRowsResultSet(), Long.valueOf(500));
  }
  }
