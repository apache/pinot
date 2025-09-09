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
package org.apache.pinot.client.cursor.exceptions;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for CursorExpiredException.
 */
public class CursorExpiredExceptionTest {

  @Test
  public void testBasicConstructor() {
    String cursorId = "test_cursor_123";
    long expirationTime = System.currentTimeMillis();
    CursorExpiredException exception = new CursorExpiredException(cursorId, expirationTime);

    Assert.assertTrue(exception.getMessage().contains(cursorId));
    Assert.assertTrue(exception.getMessage().contains(String.valueOf(expirationTime)));
    Assert.assertEquals(exception.getCursorId(), cursorId);
    Assert.assertEquals(exception.getExpirationTime(), expirationTime);
    Assert.assertTrue(exception instanceof CursorException);
  }

  @Test
  public void testCustomMessageConstructor() {
    String cursorId = "custom_cursor_456";
    long expirationTime = System.currentTimeMillis() + 1000;
    String customMessage = "Custom expiration message";

    CursorExpiredException exception = new CursorExpiredException(cursorId, expirationTime, customMessage);
    Assert.assertEquals(exception.getMessage(), customMessage);
    Assert.assertEquals(exception.getCursorId(), cursorId);
    Assert.assertEquals(exception.getExpirationTime(), expirationTime);
    Assert.assertTrue(exception instanceof CursorException);
  }

  @Test
  public void testGetCursorId() {
    String cursorId = "cursor_id_test";
    long expirationTime = 12345L;
    CursorExpiredException exception = new CursorExpiredException(cursorId, expirationTime);

    Assert.assertEquals(exception.getCursorId(), cursorId);
  }

  @Test
  public void testGetExpirationTime() {
    String cursorId = "cursor_time_test";
    long expirationTime = 98765L;
    CursorExpiredException exception = new CursorExpiredException(cursorId, expirationTime);

    Assert.assertEquals(exception.getExpirationTime(), expirationTime);
  }

  @Test
  public void testExceptionHierarchy() {
    CursorExpiredException exception = new CursorExpiredException("test", 0L);

    // Verify inheritance chain
    Assert.assertTrue(exception instanceof CursorExpiredException);
    Assert.assertTrue(exception instanceof CursorException);
    Assert.assertTrue(exception instanceof Exception);
    Assert.assertTrue(exception instanceof Throwable);
  }

  @Test
  public void testNullCursorId() {
    long expirationTime = System.currentTimeMillis();
    CursorExpiredException exception = new CursorExpiredException(null, expirationTime);

    Assert.assertNull(exception.getCursorId());
    Assert.assertEquals(exception.getExpirationTime(), expirationTime);
    Assert.assertTrue(exception.getMessage().contains("null"));
  }

  @Test
  public void testNullCustomMessage() {
    String cursorId = "null_message_test";
    long expirationTime = 555L;
    CursorExpiredException exception = new CursorExpiredException(cursorId, expirationTime, null);

    Assert.assertNull(exception.getMessage());
    Assert.assertEquals(exception.getCursorId(), cursorId);
    Assert.assertEquals(exception.getExpirationTime(), expirationTime);
  }

  @Test
  public void testDistinctionFromNotFoundException() {
    CursorExpiredException expired = new CursorExpiredException("cursor1", 1000L);
    CursorNotFoundException notFound = new CursorNotFoundException("cursor2");

    // Verify they are different exception types but both extend CursorException
    Assert.assertNotEquals(expired.getClass(), notFound.getClass());
    Assert.assertTrue(expired instanceof CursorException);
    Assert.assertTrue(notFound instanceof CursorException);
  }
}
