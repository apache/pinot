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
    BasicAuthorizationResultImpl result = BasicAuthorizationResultImpl.success();
    assertTrue(result.hasAccess());
    assertEquals("", result.getFailureMessage());
  }

  @Test
  public void testSetHasAccess() {
    BasicAuthorizationResultImpl result = new BasicAuthorizationResultImpl(false);
    assertFalse(result.hasAccess());
  }

  @Test
  public void testSetFailureMessage() {
    BasicAuthorizationResultImpl result = new BasicAuthorizationResultImpl(true, "New Failure Message");
    assertEquals("New Failure Message", result.getFailureMessage());
  }
}
