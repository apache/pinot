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

import org.apache.pinot.client.PinotClientException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for CursorException class.
 */
public class CursorExceptionTest {

  @Test
  public void testMessageConstructorWithNull() {
    CursorException exception = new CursorException((String) null);
    Assert.assertNull(exception.getMessage());
    Assert.assertNull(exception.getCause());
    Assert.assertTrue(exception instanceof PinotClientException);
  }

  @Test
  public void testMessageConstructor() {
    String message = "Test cursor exception message";
    CursorException exception = new CursorException(message);
    Assert.assertEquals(exception.getMessage(), message);
    Assert.assertNull(exception.getCause());
    Assert.assertTrue(exception instanceof PinotClientException);
  }

  @Test
  public void testCauseConstructor() {
    RuntimeException cause = new RuntimeException("Root cause");
    CursorException exception = new CursorException(cause);
    Assert.assertEquals(exception.getCause(), cause);
    Assert.assertTrue(exception.getMessage().contains("RuntimeException"));
    Assert.assertTrue(exception instanceof PinotClientException);
  }

  @Test
  public void testMessageAndCauseConstructor() {
    String message = "Test cursor exception with cause";
    RuntimeException cause = new RuntimeException("Root cause");
    CursorException exception = new CursorException(message, cause);
    Assert.assertEquals(exception.getMessage(), message);
    Assert.assertEquals(exception.getCause(), cause);
    Assert.assertTrue(exception instanceof PinotClientException);
  }

  @Test
  public void testInheritanceFromPinotClientException() {
    String message = "Test cursor exception inheritance";
    RuntimeException cause = new RuntimeException("Root cause");
    CursorException exception = new CursorException(message, cause);

    Assert.assertEquals(exception.getMessage(), message);
    Assert.assertEquals(exception.getCause(), cause);
    Assert.assertTrue(exception instanceof PinotClientException);
    Assert.assertTrue(exception instanceof Exception);
  }

  @Test
  public void testExceptionChaining() {
    Exception rootCause = new IllegalArgumentException("Invalid argument");
    RuntimeException intermediateCause = new RuntimeException("Intermediate", rootCause);
    CursorException exception = new CursorException("Cursor operation failed", intermediateCause);

    Assert.assertEquals(exception.getMessage(), "Cursor operation failed");
    Assert.assertEquals(exception.getCause(), intermediateCause);
    Assert.assertEquals(exception.getCause().getCause(), rootCause);
  }

  @Test
  public void testNullMessageHandling() {
    CursorException exception = new CursorException((String) null);
    Assert.assertNull(exception.getMessage());
    Assert.assertNull(exception.getCause());
  }

  @Test
  public void testNullCauseHandling() {
    CursorException exception = new CursorException((Throwable) null);
    Assert.assertNull(exception.getCause());
  }
}
