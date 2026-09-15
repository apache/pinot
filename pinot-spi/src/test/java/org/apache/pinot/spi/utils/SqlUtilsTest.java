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
package org.apache.pinot.spi.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests for [SqlUtils].
public class SqlUtilsTest {

  @Test
  public void testQuoteIdentifier() {
    assertEquals(SqlUtils.quoteIdentifier("column"), "\"column\"");
    assertEquals(SqlUtils.quoteIdentifier("column with space"), "\"column with space\"");
    assertEquals(SqlUtils.quoteIdentifier("database.table"), "\"database.table\"");
    assertEquals(SqlUtils.quoteIdentifier("column\"name"), "\"column\"\"name\"");
    assertEquals(SqlUtils.quoteIdentifier("column\"; DROP TABLE events; --"),
        "\"column\"\"; DROP TABLE events; --\"");
  }
}
