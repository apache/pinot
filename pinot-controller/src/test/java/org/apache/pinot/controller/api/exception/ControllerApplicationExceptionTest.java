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
package org.apache.pinot.controller.api.exception;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException.ExceptionLogMode;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests exception logging modes used by Controller API responses, and which exceptions keep their cause for them.
public class ControllerApplicationExceptionTest {
  private static final String PUBLIC_MESSAGE = "Request failed";
  private static final String SENSITIVE_MESSAGE = "file:///private/controller-secret.csv?signature=secret";

  @Test
  public void testTypeOnlyClientErrorLoggingDoesNotExposeExceptionMessage() {
    Logger logger = mock(Logger.class);
    IllegalArgumentException cause = new IllegalArgumentException(SENSITIVE_MESSAGE);

    ControllerApplicationException exception = new ControllerApplicationException(logger, PUBLIC_MESSAGE,
        Response.Status.BAD_REQUEST, cause, ExceptionLogMode.TYPE_ONLY);

    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertEquals(exception.getMessage(), PUBLIC_MESSAGE);
    ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
    verify(logger).info(eq("{} exception type: {}"), eq(PUBLIC_MESSAGE), description.capture());
    assertTypeOnlyDescription(description.getValue(), IllegalArgumentException.class.getName());
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testTypeOnlyServerErrorLoggingDoesNotExposeExceptionMessageOrThrowable() {
    Logger logger = mock(Logger.class);
    IllegalStateException cause = new IllegalStateException(SENSITIVE_MESSAGE);

    ControllerApplicationException exception = new ControllerApplicationException(logger, PUBLIC_MESSAGE,
        Response.Status.INTERNAL_SERVER_ERROR, cause, ExceptionLogMode.TYPE_ONLY);

    assertEquals(exception.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    assertEquals(exception.getMessage(), PUBLIC_MESSAGE);
    ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
    verify(logger).error(eq("{} exception type: {}"), eq(PUBLIC_MESSAGE), description.capture());
    assertTypeOnlyDescription(description.getValue(), IllegalStateException.class.getName());
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testTypeOnlyLoggingNamesTheCauseChainWithoutMessages() {
    Logger logger = mock(Logger.class);
    RuntimeException failure = new RuntimeException(SENSITIVE_MESSAGE,
        new IllegalStateException(SENSITIVE_MESSAGE, new IOException(SENSITIVE_MESSAGE)));

    new ControllerApplicationException(logger, PUBLIC_MESSAGE, Response.Status.INTERNAL_SERVER_ERROR, failure,
        ExceptionLogMode.TYPE_ONLY);

    ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
    verify(logger).error(eq("{} exception type: {}"), eq(PUBLIC_MESSAGE), description.capture());
    assertTypeOnlyDescription(description.getValue(), RuntimeException.class.getName() + " caused by "
        + IllegalStateException.class.getName() + " caused by " + IOException.class.getName());
    verifyNoMoreInteractions(logger);
  }

  /// The description starts with the exception types, names the frame that threw the innermost exception (this test),
  /// and never repeats an exception message.
  private static void assertTypeOnlyDescription(Object description, String expectedTypes) {
    String text = String.valueOf(description);
    assertTrue(text.startsWith(expectedTypes + " at "), text);
    assertTrue(text.contains(ControllerApplicationExceptionTest.class.getName()), text);
    assertFalse(text.contains(SENSITIVE_MESSAGE), text);
  }

  @Test
  public void testConstructorsKeepTheCauseOutOfTheResponse() {
    Logger logger = mock(Logger.class);
    IllegalStateException cause = new IllegalStateException(SENSITIVE_MESSAGE);

    ControllerApplicationException exception = new ControllerApplicationException(logger, PUBLIC_MESSAGE,
        Response.Status.INTERNAL_SERVER_ERROR, cause);

    assertNull(exception.getCause());
    assertFalse(exception.shouldIncludeCausesInResponse());
    verify(logger).error(PUBLIC_MESSAGE, cause);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testWithCausesInResponseKeepsTheCauseAndLogsItAsUsual() {
    Logger logger = mock(Logger.class);
    IllegalArgumentException cause = new IllegalArgumentException("Cannot read single-value for column: name");

    ControllerApplicationException exception =
        ControllerApplicationException.withCausesInResponse(logger, PUBLIC_MESSAGE, Response.Status.BAD_REQUEST, cause);

    assertSame(exception.getCause(), cause);
    assertTrue(exception.shouldIncludeCausesInResponse());
    assertEquals(exception.getMessage(), PUBLIC_MESSAGE);
    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    verify(logger).info("{} exception: {}", PUBLIC_MESSAGE, cause.getMessage());
    verifyNoMoreInteractions(logger);
  }
}
