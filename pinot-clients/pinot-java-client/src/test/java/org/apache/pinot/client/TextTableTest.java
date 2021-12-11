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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class TextTableTest {

  private TextTable _textTableUnderTest;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _textTableUnderTest = new TextTable();
  }

  @Test(priority = 0)
  public void testRightPad() {
    assertEquals("str ", TextTable.rightPad("str", 4, ' '));
  }

  @Test(priority = 1)
  public void testToStringForEmptyTextTable() {
    // Run the test
    final String result = _textTableUnderTest.toString();

    // Verify the results
    assertEquals("", result);
  }

  @Test(priority = 2)
  public void testAddHeader() {
    // Run the test
    String initalTable = _textTableUnderTest.toString();
    _textTableUnderTest.addHeader("header1");
    String finalTable = _textTableUnderTest.toString();

    // Verify the results
    assertEquals("", initalTable);
    assertNotEquals("", finalTable);
  }

  @Test(priority = 2)
  public void testAddRow() {
    // Run the test
    String initalTable = _textTableUnderTest.toString();
    _textTableUnderTest.addRow("row1");
    String finalTable = _textTableUnderTest.toString();

    // Verify the results
    assertEquals("", initalTable);
    assertNotEquals("", finalTable);
  }
}
