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
package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SystemTableQueryDetectorTest {

  @Test
  public void testDoesNotMatchInsideStringLiteral() {
    assertFalse(containsSystemTableReference("SELECT 'from system.foo' FROM my_table"));
    assertFalse(containsSystemTableReference("SELECT 'join system.foo' FROM my_table"));
    assertFalse(containsSystemTableReference("SELECT 'from system.foo and join system.bar' FROM my_table"));
  }

  @Test
  public void testDoesNotMatchInsideEscapedStringLiteral() {
    assertFalse(containsSystemTableReference("SELECT 'from system.''foo''' FROM my_table"));
  }

  @Test
  public void testDoesNotMatchInsideComments() {
    assertFalse(containsSystemTableReference("SELECT 1 FROM my_table -- from system.foo"));
    assertFalse(containsSystemTableReference("SELECT 1 FROM my_table /* join system.foo */"));
    assertFalse(containsSystemTableReference("SELECT 1\nFROM my_table\n-- from system.foo\nWHERE col = 1"));
  }

  @Test
  public void testMatchesRealSystemTableReferences() {
    assertTrue(containsSystemTableReference("SELECT * FROM system.tables"));
    assertTrue(containsSystemTableReference("select * from SYSTEM.tables"));
    assertTrue(containsSystemTableReference("SELECT * FROM my_table JOIN system.tables"));
  }

  @Test
  public void testMatchesNestedSystemTableReferences() {
    assertTrue(containsSystemTableReference(
        "SELECT * FROM my_table WHERE col IN (SELECT col FROM system.tables WHERE type = 'OFFLINE')"));
    assertTrue(containsSystemTableReference("WITH t AS (SELECT * FROM system.tables) SELECT * FROM t"));
  }

  private static boolean containsSystemTableReference(String sql) {
    return SystemTableQueryDetector.containsSystemTableReference(CalciteSqlParser.compileToSqlNodeAndOptions(sql)
        .getSqlNode());
  }
}
