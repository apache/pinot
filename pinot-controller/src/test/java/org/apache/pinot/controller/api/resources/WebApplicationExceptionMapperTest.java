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
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.ControllerApplicationException.ExceptionLogMode;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests that [WebApplicationExceptionMapper] appends the cause chain to the error message.
public class WebApplicationExceptionMapperTest {
  private static final Logger LOGGER = mock(Logger.class);
  private static final String MESSAGE = "Caught exception when ingesting file into table: testTable_OFFLINE";
  private static final String ROOT_CAUSE = "Cannot read single-value from Object[]: [cooper, max] for column: name";
  private static final WebApplicationExceptionMapper MAPPER = new WebApplicationExceptionMapper(true);
  private static final WebApplicationExceptionMapper MAPPER_WITHOUT_CAUSES = new WebApplicationExceptionMapper(false);

  @Test
  public void testControllerApplicationExceptionWithCauseIsDescribedByItsCauseChain() {
    Exception rootCause = new IllegalArgumentException(ROOT_CAUSE);
    Exception cause = new RuntimeException("Caught exception while reading data", rootCause);
    ControllerApplicationException e =
        new ControllerApplicationException(LOGGER, MESSAGE, Response.Status.INTERNAL_SERVER_ERROR, cause);

    Response response = MAPPER.toResponse(e);

    assertEquals(response.getStatus(), 500);
    SimpleHttpErrorInfo errorInfo = (SimpleHttpErrorInfo) response.getEntity();
    assertEquals(errorInfo.getCode(), 500);
    assertEquals(errorInfo.getError(), MESSAGE + " -> Caught exception while reading data -> " + ROOT_CAUSE);
  }

  @Test
  public void testCauseThatTheMessageAlreadyContainsIsNotRepeated() {
    Exception cause = new IllegalStateException("Table config is invalid", new IllegalArgumentException(ROOT_CAUSE));
    ControllerApplicationException e = new ControllerApplicationException(LOGGER,
        "Failed to add table: " + cause.getMessage(), Response.Status.BAD_REQUEST, cause);

    assertEquals(MAPPER.getErrorMessage(e), "Failed to add table: Table config is invalid -> " + ROOT_CAUSE);
  }

  @Test
  public void testControllerApplicationExceptionWithoutCauseIsDescribedByItsMessage() {
    ControllerApplicationException e =
        new ControllerApplicationException(LOGGER, MESSAGE, Response.Status.NOT_FOUND);

    assertSame(MAPPER.getErrorMessage(e), MESSAGE);
  }

  @Test
  public void testFailureThatDoesNotKeepItsCauseIsDescribedByItsMessage() {
    for (ExceptionLogMode mode : new ExceptionLogMode[]{ExceptionLogMode.LOG_ONLY, ExceptionLogMode.TYPE_ONLY}) {
      ControllerApplicationException e = new ControllerApplicationException(LOGGER, "Failed to ingest from URI",
          Response.Status.INTERNAL_SERVER_ERROR, new IOException("/etc/controller-secret (Permission denied)"), mode);

      assertEquals(MAPPER.toResponse(e).getStatus(), 500, mode.name());
      assertEquals(((SimpleHttpErrorInfo) MAPPER.toResponse(e).getEntity()).getError(), "Failed to ingest from URI",
          mode.name());
    }
  }

  @Test
  public void testWebApplicationExceptionWithCauseIsDescribedByItsCauseChain() {
    WebApplicationException e =
        new WebApplicationException(new NumberFormatException("For input string: \"abc\""), 404);

    Response response = MAPPER.toResponse(e);

    assertEquals(response.getStatus(), 404);
    assertEquals(((SimpleHttpErrorInfo) response.getEntity()).getError(),
        "HTTP 404 Not Found -> For input string: \"abc\"");
  }

  @Test
  public void testUnexpectedExceptionIsDescribedByItsCauseChain() {
    Exception e = new IllegalStateException("Failed to rebalance table", new IOException("Connection reset"));

    Response response = MAPPER.toResponse(e);

    assertEquals(response.getStatus(), 500);
    assertEquals(((SimpleHttpErrorInfo) response.getEntity()).getError(),
        "Failed to rebalance table -> Connection reset");
  }

  @Test
  public void testUnexpectedExceptionWithoutMessageIsDescribedByItsType() {
    assertEquals(MAPPER.getErrorMessage(new NullPointerException()), "NullPointerException");
  }

  @Test
  public void testUnexpectedExceptionWithoutMessageIsDescribedByItsCauses() {
    assertEquals(MAPPER.getErrorMessage(new MessagelessException(new IOException("Connection reset"))),
        "Connection reset");
  }

  @Test
  public void testCauseChainIsBounded() {
    Throwable cause = new IllegalArgumentException("Cannot read single-value from Object[]: ["
        + StringUtils.repeat('x', 10_000) + "] for column: name");
    for (int i = 9; i >= 0; i--) {
      cause = new RuntimeException("wrapper-" + i, cause);
    }
    ControllerApplicationException e =
        new ControllerApplicationException(LOGGER, MESSAGE, Response.Status.INTERNAL_SERVER_ERROR, cause);

    String error = MAPPER.getErrorMessage(e);

    String[] parts = error.split(" -> ");
    assertEquals(parts.length, 1 + WebApplicationExceptionMapper.MAX_CAUSES + 1, error);
    assertEquals(parts[0], MESSAGE);
    assertEquals(parts[1], "wrapper-0");
    assertEquals(parts[2], "...");
    String rootCause = parts[parts.length - 1];
    assertEquals(rootCause.length(), WebApplicationExceptionMapper.MAX_CAUSE_LENGTH);
    assertTrue(rootCause.endsWith("] for column: name"), rootCause);
  }

  @Test
  public void testCausesCanBeTurnedOff() {
    Throwable[] failures = {
        new ControllerApplicationException(LOGGER, MESSAGE, Response.Status.INTERNAL_SERVER_ERROR,
            new IllegalArgumentException(ROOT_CAUSE)),
        new WebApplicationException(new NumberFormatException("For input string: \"abc\""), 404),
        new IllegalStateException("Failed to rebalance table", new IOException("Connection reset")),
        new NullPointerException()
    };
    for (Throwable failure : failures) {
      SimpleHttpErrorInfo errorInfo = (SimpleHttpErrorInfo) MAPPER_WITHOUT_CAUSES.toResponse(failure).getEntity();
      assertEquals(errorInfo.getError(), failure.getMessage(), failure.toString());
    }
    assertNull(((SimpleHttpErrorInfo) MAPPER_WITHOUT_CAUSES.toResponse(new NullPointerException()).getEntity())
        .getError());
  }

  @Test
  public void testCausesAreIncludedByDefault()
      throws IllegalAccessException {
    Exception e = new IllegalStateException("outer", new IOException("inner"));

    assertEquals(new WebApplicationExceptionMapper().getErrorMessage(e), "outer -> inner");
    assertEquals(getMapperForApplicationWith(new ControllerConf()).getErrorMessage(e), "outer -> inner");
  }

  @Test
  public void testCausesCanBeTurnedOffInTheControllerConfig()
      throws IllegalAccessException {
    ControllerConf conf = new ControllerConf();
    conf.setProperty(ControllerConf.API_ERROR_RESPONSE_INCLUDE_CAUSES, false);
    Exception e = new IllegalStateException("outer", new IOException("inner"));

    assertEquals(getMapperForApplicationWith(conf).getErrorMessage(e), "outer");
  }

  private static WebApplicationExceptionMapper getMapperForApplicationWith(ControllerConf conf)
      throws IllegalAccessException {
    Application application = new Application() {
      @Override
      public Map<String, Object> getProperties() {
        return Map.of(ControllerAdminApiApplication.PINOT_CONFIGURATION, conf);
      }
    };
    WebApplicationExceptionMapper mapper = new WebApplicationExceptionMapper();
    FieldUtils.writeField(mapper, "_application", application, true);
    return mapper;
  }

  private static class MessagelessException extends RuntimeException {
    MessagelessException(Throwable cause) {
      super(null, cause);
    }
  }
}
