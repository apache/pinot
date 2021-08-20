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


public class SelectionResultSetTest {

  private JsonNode _mockSelectionResults;

  private SelectionResultSet _selectionResultSetUnderTest;

  @BeforeMethod
  public void setUp()
      throws Exception {
    String jsonString =
        "{\"results\":[[\"r1c1\",\"r1c2\"]], \"columns\":[\"column1\", \"column2\"]}";
    ObjectMapper objectMapper = new ObjectMapper();
    _mockSelectionResults = objectMapper.readTree(jsonString);
    _selectionResultSetUnderTest = new SelectionResultSet(_mockSelectionResults);
  }

  @Test
  public void testGetRowCount() {
    // Run the test
    final int result = _selectionResultSetUnderTest.getRowCount();

    // Verify the results
    assertEquals(1, result);
  }

  @Test
  public void testGetColumnCount() {
    // Run the test
    final int result = _selectionResultSetUnderTest.getColumnCount();

    // Verify the results
    assertEquals(2, result);
  }

  @Test
  public void testGetColumnName() {
    // Run the test
    final String result = _selectionResultSetUnderTest.getColumnName(0);

    // Verify the results
    assertEquals("column1", result);
  }

  @Test
  public void testGetString() {
    // Run the test
    final String result = _selectionResultSetUnderTest.getString(0, 0);

    // Verify the results
    assertEquals("r1c1", result);
  }

  @Test
  public void testGetGroupKeyLength() {
    // Run the test
    final int result = _selectionResultSetUnderTest.getGroupKeyLength();

    // Verify the results
    assertEquals(0, result);
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testGetGroupKeyString() {
    // Run the test
    _selectionResultSetUnderTest.getGroupKeyString(0, 0);
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testGetGroupKeyColumnName() {
    // Run the test
    _selectionResultSetUnderTest.getGroupKeyColumnName(0);
  }

  @Test
  public void testToString() {
    // Run the test
    final String result = _selectionResultSetUnderTest.toString();

    // Verify the results
    assertNotEquals("", result);
  }
}
