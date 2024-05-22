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

import java.util.Arrays;
import java.util.HashSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BasicAuthorizationResultImplTest {
  @Test
  public void testConstructorWithAccessAndMessage() {
    BasicAuthorizationResultImpl result = new BasicAuthorizationResultImpl(true, "Failure Message");
    assertTrue(result.hasAccess());
    assertEquals("Failure Message", result.getFailureMessage());
  }

  @Test
  public void testConstructorWithAccessOnly() {
    BasicAuthorizationResultImpl result = new BasicAuthorizationResultImpl(true);
    assertTrue(result.hasAccess());
    assertEquals("", result.getFailureMessage());
  }

  @Test
  public void testNoFailureResult() {
    BasicAuthorizationResultImpl result = BasicAuthorizationResultImpl.noFailureResult();
    assertTrue(result.hasAccess());
    assertEquals("", result.getFailureMessage());
  }

  @Test
  public void testJoinResultsBothHaveAccess() {
    AuthorizationResult result1 = new BasicAuthorizationResultImpl(true);
    AuthorizationResult result2 = new BasicAuthorizationResultImpl(true);
    BasicAuthorizationResultImpl result = BasicAuthorizationResultImpl.joinResults(result1, result2);
    assertTrue(result.hasAccess());
    assertEquals("", result.getFailureMessage());
  }

  @Test
  public void testJoinResultsOneHasNoAccess() {
    AuthorizationResult result1 = new BasicAuthorizationResultImpl(true);
    AuthorizationResult result2 = new BasicAuthorizationResultImpl(false, "Access Denied");
    BasicAuthorizationResultImpl result = BasicAuthorizationResultImpl.joinResults(result1, result2);
    assertFalse(result.hasAccess());
    assertEquals("; Access Denied", result.getFailureMessage());
  }

  @Test
  public void testJoinResultsOneHasNoAccessCrossImplementations() {
    AuthorizationResult result1 = new BasicAuthorizationResultImpl(true);
    AuthorizationResult result2 = new TableAuthorizationResult(new HashSet<>(Arrays.asList("table1", "table2")));
    BasicAuthorizationResultImpl result = BasicAuthorizationResultImpl.joinResults(result1, result2);
    assertFalse(result.hasAccess());
    assertEquals("; Authorization Failed for tables: table1, table2,", result.getFailureMessage());
  }

  @Test
  public void testJoinResultsBothHaveNoAccess() {
    AuthorizationResult result1 = new BasicAuthorizationResultImpl(false, "Access Denied 1");
    AuthorizationResult result2 = new BasicAuthorizationResultImpl(false, "Access Denied 2");
    BasicAuthorizationResultImpl result = BasicAuthorizationResultImpl.joinResults(result1, result2);
    assertFalse(result.hasAccess());
    assertEquals("Access Denied 1 ; Access Denied 2", result.getFailureMessage());
  }

  @Test
  public void testSetHasAccess() {
    BasicAuthorizationResultImpl result = new BasicAuthorizationResultImpl(true);
    result.setHasAccess(false);
    assertFalse(result.hasAccess());
  }

  @Test
  public void testSetFailureMessage() {
    BasicAuthorizationResultImpl result = new BasicAuthorizationResultImpl(true);
    result.setFailureMessage("New Failure Message");
    assertEquals("New Failure Message", result.getFailureMessage());
  }
}
