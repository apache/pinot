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
 * Unit tests for CursorNotFoundException.
 */
public class CursorNotFoundExceptionTest {

  @Test
  public void testBasicConstructor() {
    String cursorId = "test_cursor_123";
    CursorNotFoundException exception = new CursorNotFoundException(cursorId);

    Assert.assertTrue(exception.getMessage().contains(cursorId));
    Assert.assertEquals(exception.getCursorId(), cursorId);
    Assert.assertTrue(exception instanceof CursorException);
  }

  @Test
  public void testCustomMessageConstructor() {
    String cursorId = "custom_cursor_456";
    String customMessage = "Custom not found message";

    CursorNotFoundException exception = new CursorNotFoundException(cursorId, customMessage);
    Assert.assertEquals(exception.getMessage(), customMessage);
    Assert.assertEquals(exception.getCursorId(), cursorId);
    Assert.assertTrue(exception instanceof CursorException);
  }

  @Test
  public void testMessageAndCauseConstructor() {
    String cursorId = "cause_cursor_789";
    String message = "Cursor not found with cause";
    RuntimeException cause = new RuntimeException("Not found cause");

    CursorNotFoundException exception = new CursorNotFoundException(cursorId, message, cause);
    Assert.assertEquals(exception.getMessage(), message);
    Assert.assertEquals(exception.getCause(), cause);
    Assert.assertEquals(exception.getCursorId(), cursorId);
    Assert.assertTrue(exception instanceof CursorException);
  }

  @Test
  public void testGetCursorId() {
    String cursorId = "cursor_id_test";
    CursorNotFoundException exception = new CursorNotFoundException(cursorId);

    Assert.assertEquals(exception.getCursorId(), cursorId);
  }

  @Test
  public void testExceptionHierarchy() {
    CursorNotFoundException exception = new CursorNotFoundException("test");

    // Verify inheritance chain
    Assert.assertTrue(exception instanceof CursorNotFoundException);
    Assert.assertTrue(exception instanceof CursorException);
    Assert.assertTrue(exception instanceof Exception);
    Assert.assertTrue(exception instanceof Throwable);
  }

  @Test
  public void testNullCursorId() {
    CursorNotFoundException exception = new CursorNotFoundException(null);

    Assert.assertNull(exception.getCursorId());
    Assert.assertTrue(exception.getMessage().contains("null"));
  }

  @Test
  public void testNullCustomMessage() {
    String cursorId = "null_message_test";
    CursorNotFoundException exception = new CursorNotFoundException(cursorId, null);

    Assert.assertNull(exception.getMessage());
    Assert.assertEquals(exception.getCursorId(), cursorId);
  }

  @Test
  public void testNullCause() {
    String cursorId = "null_cause_test";
    String message = "Test message";
    CursorNotFoundException exception = new CursorNotFoundException(cursorId, message, null);

    Assert.assertEquals(exception.getMessage(), message);
    Assert.assertNull(exception.getCause());
    Assert.assertEquals(exception.getCursorId(), cursorId);
  }

  @Test
  public void testDistinctionFromExpiredException() {
    CursorNotFoundException notFound = new CursorNotFoundException("cursor1");
    CursorExpiredException expired = new CursorExpiredException("cursor2", 1000L);

    // Verify they are different exception types but both extend CursorException
    Assert.assertNotEquals(notFound.getClass(), expired.getClass());
    Assert.assertTrue(notFound instanceof CursorException);
    Assert.assertTrue(expired instanceof CursorException);
  }
}
