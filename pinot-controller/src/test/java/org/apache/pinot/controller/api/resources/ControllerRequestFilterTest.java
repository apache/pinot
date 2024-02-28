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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test
public class ControllerRequestFilterTest extends ControllerTest {
  private static final String TABLE_NAME = "table1";
  private static final String DATABASE_NAME = "db1";
  private static final String FULLY_QUALIFIED_TABLE_NAME = String.format("%s.%s", DATABASE_NAME, TABLE_NAME);


  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  public void testTableNameTranslationWithHeader()
      throws IOException {
    Map<String, String> headers = new HashMap<>();
    headers.put(CommonConstants.DATABASE, DATABASE_NAME);
    // with logical table name param value
    assertResponse(TABLE_NAME, headers, FULLY_QUALIFIED_TABLE_NAME);

    // with fully qualified table name param value. This should take precedence over the database header.
    headers.put(CommonConstants.DATABASE, "randomName");
    assertResponse(FULLY_QUALIFIED_TABLE_NAME, headers, FULLY_QUALIFIED_TABLE_NAME);

    // with 'default' database header. Database prefix should be trimmed upon translation.
    headers.put(CommonConstants.DATABASE, CommonConstants.DEFAULT_DATABASE);
    assertResponse(TABLE_NAME, headers, TABLE_NAME);
  }

  public void testTableNameTranslationWithoutHeader()
      throws IOException {
    assertResponse(FULLY_QUALIFIED_TABLE_NAME, null, FULLY_QUALIFIED_TABLE_NAME);

    // with 'default' database prefix. Database prefix should be trimmed upon translation.
    assertResponse(String.format("%s.%s", CommonConstants.DEFAULT_DATABASE, TABLE_NAME), null, TABLE_NAME);

    Map<String, String> headers = new HashMap<>();
    // unsanitized database header values
    headers.put(CommonConstants.DATABASE, null);
    assertResponse(FULLY_QUALIFIED_TABLE_NAME, headers, FULLY_QUALIFIED_TABLE_NAME);
    headers.put(CommonConstants.DATABASE, "");
    assertResponse(FULLY_QUALIFIED_TABLE_NAME, headers, FULLY_QUALIFIED_TABLE_NAME);
  }

  private void assertResponse(String paramValue, Map<String, String> headers, String fqn)
      throws IOException {
    String uri = String.format("%s/%s", getControllerBaseApiUrl(), "testResource/requestFilter");
    ObjectMapper mapper = new ObjectMapper();
    // when "tableName" query param is passed
    JsonNode resp = mapper.readTree(
        ControllerTest.sendGetRequest(String.format("%s?%s=%s", uri, "tableName", paramValue), headers));
    assertEquals(resp.get("tableName").asText(), fqn);

    // when "tableNameWithType" query param is passed
    resp = mapper.readTree(
        ControllerTest.sendGetRequest(String.format("%s?%s=%s", uri, "tableNameWithType", paramValue), headers));
    assertEquals(resp.get("tableNameWithType").asText(), fqn);

    // when "schemaName" query param is passed
    resp = mapper.readTree(
        ControllerTest.sendGetRequest(String.format("%s?%s=%s", uri, "schemaName", paramValue), headers));
    assertEquals(resp.get("schemaName").asText(), fqn);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
