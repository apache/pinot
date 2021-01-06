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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GroupByResultSetTest {

  private JsonNode mockJsonObject;

  private GroupByResultSet groupByResultSetUnderTest;

  @BeforeMethod
  public void setUp() throws Exception {
    String jsonString =
        "{\"groupByResult\":[{\"value\":1, \"group\":[\"testGroup1\"]},{\"value\":2, \"group\":[\"testGroup2\"]}], \"groupByColumns\":[\"testGroupColumn\"], \"function\":\"testFunction\"}";
    ObjectMapper objectMapper = new ObjectMapper();
    mockJsonObject = objectMapper.readTree(jsonString);
    groupByResultSetUnderTest = new GroupByResultSet(mockJsonObject);
  }

  @Test
  public void testGetRowCount() {
    // Run the test
    final int result = groupByResultSetUnderTest.getRowCount();

    // Verify the results
    assertEquals(2, result);
  }

  @Test
  public void testGetColumnCount() {
    // Run the test
    final int result = groupByResultSetUnderTest.getColumnCount();

    // Verify the results
    assertEquals(1, result);
  }

  @Test
  public void testGetColumnName() {
    // Run the test
    final String result = groupByResultSetUnderTest.getColumnName(0);

    // Verify the results
    assertEquals("testFunction", result);
  }

  @Test
  public void testGetString() {
    // Run the test
    final String result = groupByResultSetUnderTest.getString(0, 0);

    // Verify the results
    assertEquals("1", result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetString_exception() {
    // Run the test
    final String result = groupByResultSetUnderTest.getString(0, 1);
  }

  @Test
  public void testGetGroupKeyLength() {
    // Run the test
    final int result = groupByResultSetUnderTest.getGroupKeyLength();

    // Verify the results
    assertEquals(1, result);
  }

  @Test
  public void testGetGroupKeyString() {
    // Run the test
    final String result = groupByResultSetUnderTest.getGroupKeyString(0, 0);

    // Verify the results
    assertEquals("testGroup1", result);
  }

  @Test
  public void testGetGroupKeyColumnName() {
    // Run the test
    final String result = groupByResultSetUnderTest.getGroupKeyColumnName(0);

    // Verify the results
    assertEquals("testGroupColumn", result);
  }

  @Test(priority = 1)
  public void testToString() {
    // Run the test
    final String result = groupByResultSetUnderTest.toString();

    // Verify the results
    assertNotEquals("", result);
  }
}
