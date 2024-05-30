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

package org.apache.pinot.spi.auth;

import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableAuthorizationResultTest {

  @Test
  public void testParameterizedConstructor() {
    Set<String> failedTables = new HashSet<>();
    failedTables.add("table1");
    TableAuthorizationResult result = new TableAuthorizationResult(failedTables);
    Assert.assertFalse(result.hasAccess());
    Assert.assertTrue(result.getFailureMessage().contains("table1"));
  }

  @Test
  public void testAddFailedTable() {
    TableAuthorizationResult result = new TableAuthorizationResult(Set.of("table1"));
    Assert.assertFalse(result.hasAccess());
    Assert.assertEquals(result.getFailureMessage(), "Authorization Failed for tables: [table1]");
  }

  @Test
  public void testSetGetFailedTables() {
    Set<String> failedTables = new HashSet<>();
    failedTables.add("table1");
    failedTables.add("table2");
    TableAuthorizationResult result = new TableAuthorizationResult(failedTables);
    Assert.assertFalse(result.hasAccess());
    Assert.assertEquals(result.getFailedTables(), failedTables);
  }

  @Test
  public void testGetFailureMessage() {
    TableAuthorizationResult result = new TableAuthorizationResult(Set.of("table1", "table2"));
    Assert.assertEquals(result.getFailureMessage(), "Authorization Failed for tables: [table1, table2]");
  }

  @Test
  public void testNoFailureResult() {
    TableAuthorizationResult result = TableAuthorizationResult.success();
    Assert.assertTrue(result.hasAccess());
    Assert.assertEquals("", result.getFailureMessage());
  }
}
