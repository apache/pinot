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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class AggregationResultSetTest {

  private JsonNode mockJsonObject;

  private AggregationResultSet aggregationResultSetUnderTest;

  @BeforeMethod
  public void setUp() throws Exception {
    String jsonString = "{\"function\":\"testFunction\", \"value\":\"123\"}";
    ObjectMapper objectMapper = new ObjectMapper();
    mockJsonObject = objectMapper.readTree(jsonString);
    aggregationResultSetUnderTest = new AggregationResultSet(mockJsonObject);
  }

  @Test
  public void testGetRowCount() {
    // Run the test
    final int result = aggregationResultSetUnderTest.getRowCount();

    // Verify the results
    assertEquals(1, result);
  }

  @Test
  public void testGetColumnCount() {
    // Run the test
    final int result = aggregationResultSetUnderTest.getColumnCount();

    // Verify the results
    assertEquals(1, result);
  }

  @Test
  public void testGetColumnName() {
    // Run the test
    final String result = aggregationResultSetUnderTest.getColumnName(0);

    // Verify the results
    assertEquals("testFunction", result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetStringForNonZeroRow() {
    // Run the test
    aggregationResultSetUnderTest.getString(1, 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetStringForNonZeroColumn() {
    // Run the test
    aggregationResultSetUnderTest.getString(0, 1);
  }

  @Test
  public void testGetString() {
    // Run the test
    final String result = aggregationResultSetUnderTest.getString(0, 0);

    // Verify the results
    assertEquals("123", result);
  }

  @Test
  public void testGetGroupKeyLength() {
    // Run the test
    final int result = aggregationResultSetUnderTest.getGroupKeyLength();

    // Verify the results
    assertEquals(0, result);
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testGetGroupKeyColumnName() {
    aggregationResultSetUnderTest.getGroupKeyColumnName(0);
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testGetGroupKeyString() {
    aggregationResultSetUnderTest.getGroupKeyString(0, 0);
  }

  @Test
  public void testToString() {
    // Run the test
    final String result = aggregationResultSetUnderTest.toString();

    // Verify the results
    assertNotEquals("", result);
  }
}
