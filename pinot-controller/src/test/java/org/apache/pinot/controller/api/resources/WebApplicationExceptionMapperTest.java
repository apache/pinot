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
package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests that [WebApplicationExceptionMapper] appends the cause chain only for exceptions created to return it.
public class WebApplicationExceptionMapperTest {
  private static final Logger LOGGER = mock(Logger.class);
  private static final String MESSAGE = "Caught exception when ingesting file into table: testTable_OFFLINE";
  private static final String ROOT_CAUSE = "Cannot read single-value from Object[]: [cooper, max] for column: name";

  @Test
  public void testExceptionCreatedWithCausesInResponseIsDescribedByItsCauseChain() {
    Exception cause =
        new RuntimeException("Caught exception while reading data", new IllegalArgumentException(ROOT_CAUSE));
    ControllerApplicationException e = ControllerApplicationException.withCausesInResponse(LOGGER, MESSAGE,
        Response.Status.INTERNAL_SERVER_ERROR, cause);

    Response response = new WebApplicationExceptionMapper().toResponse(e);

    assertEquals(response.getStatus(), 500);
    SimpleHttpErrorInfo errorInfo = (SimpleHttpErrorInfo) response.getEntity();
    assertEquals(errorInfo.getCode(), 500);
    assertEquals(errorInfo.getError(), MESSAGE + " -> Caught exception while reading data -> " + ROOT_CAUSE);
  }

  @Test
  public void testCauseThatTheMessageAlreadyContainsIsNotRepeated() {
    Exception cause =
        new RuntimeException("Caught exception while reading data", new IllegalArgumentException(ROOT_CAUSE));
    ControllerApplicationException e = ControllerApplicationException.withCausesInResponse(LOGGER,
        MESSAGE + ". " + cause.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, cause);

    assertEquals(WebApplicationExceptionMapper.getErrorMessage(e),
        MESSAGE + ". Caught exception while reading data -> " + ROOT_CAUSE);
  }

  @Test
  public void testCausesAppendedToTheMessageAreBounded() {
    String rootCause =
        "Cannot read single-value from Object[]: [" + StringUtils.repeat('x', 10_000) + "] for column: v";
    ControllerApplicationException e = ControllerApplicationException.withCausesInResponse(LOGGER, MESSAGE,
        Response.Status.BAD_REQUEST, new RuntimeException("wrapper", new IllegalArgumentException(rootCause)));

    String error = WebApplicationExceptionMapper.getErrorMessage(e);

    assertEquals(error.length(), MESSAGE.length() + WebApplicationExceptionMapper.MAX_CAUSES_LENGTH);
    assertTrue(error.startsWith(MESSAGE + " -> wrapper -> Cannot read single-value"), error);
    assertTrue(error.endsWith("] for column: v"), error);
  }

  /// Every other failure is described by its message alone, as before.
  @Test
  public void testOtherFailuresAreDescribedByTheirMessageOnly() {
    Throwable[] failures = {
        new ControllerApplicationException(LOGGER, MESSAGE, Response.Status.INTERNAL_SERVER_ERROR,
            new IllegalArgumentException(ROOT_CAUSE)),
        new WebApplicationException(new NumberFormatException("For input string: \"abc\""), 404),
        new IllegalStateException("Failed to rebalance table", new IOException("Connection reset"))
    };
    for (Throwable failure : failures) {
      SimpleHttpErrorInfo errorInfo = (SimpleHttpErrorInfo) new WebApplicationExceptionMapper().toResponse(failure)
          .getEntity();
      assertEquals(errorInfo.getError(), failure.getMessage(), failure.toString());
    }
    assertNull(WebApplicationExceptionMapper.getErrorMessage(new NullPointerException()));
  }
}
