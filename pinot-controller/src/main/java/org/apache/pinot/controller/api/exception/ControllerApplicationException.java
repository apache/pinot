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

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;


public class ControllerApplicationException extends WebApplicationException {

  /// Controls whether server logs include the supplied exception details or only its types.
  ///
  /// `TYPE_ONLY` is for failures whose messages can repeat request or configuration values: it logs the class names
  /// of the exception and its causes and the frame that threw the innermost one, and never a message.
  public enum ExceptionLogMode {
    FULL,
    TYPE_ONLY
  }

  private static final int MAX_LOGGED_CAUSES = 8;

  private final boolean _includeCausesInResponse;

  public ControllerApplicationException(Logger logger, String message, Response.Status status) {
    this(logger, message, status.getStatusCode(), null);
  }

  public ControllerApplicationException(Logger logger, String message, int status) {
    this(logger, message, status, null);
  }

  public ControllerApplicationException(Logger logger, String message, Response.Status status, @Nullable Throwable e) {
    this(logger, message, status.getStatusCode(), e, ExceptionLogMode.FULL);
  }

  public ControllerApplicationException(Logger logger, String message, Response.Status status, @Nullable Throwable e,
      ExceptionLogMode exceptionLogMode) {
    this(logger, message, status.getStatusCode(), e, exceptionLogMode);
  }

  public ControllerApplicationException(Logger logger, String message, int status, @Nullable Throwable e) {
    this(logger, message, status, e, ExceptionLogMode.FULL);
  }

  private ControllerApplicationException(Logger logger, String message, int status, @Nullable Throwable e,
      ExceptionLogMode exceptionLogMode) {
    this(logger, message, status, e, exceptionLogMode, false);
  }

  private ControllerApplicationException(Logger logger, String message, int status, @Nullable Throwable e,
      ExceptionLogMode exceptionLogMode, boolean includeCausesInResponse) {
    super(message, includeCausesInResponse ? e : null, status);
    _includeCausesInResponse = includeCausesInResponse;
    if (status >= 300 && status < 500) {
      if (e == null) {
        logger.info(message);
      } else if (exceptionLogMode == ExceptionLogMode.TYPE_ONLY) {
        logger.info("{} exception type: {}", message, describeTypes(e));
      } else {
        logger.info("{} exception: {}", message, e.getMessage());
      }
    } else {
      if (e == null) {
        logger.error(message);
      } else if (exceptionLogMode == ExceptionLogMode.TYPE_ONLY) {
        logger.error("{} exception type: {}", message, describeTypes(e));
      } else {
        logger.error(message, e);
      }
    }
  }

  /// Describes `e` without repeating any exception message: the class names of `e` and its causes, outermost first,
  /// followed by the frame that threw the innermost one. Class names and frames locate a failure but cannot carry
  /// request or configuration values.
  private static String describeTypes(Throwable e) {
    StringBuilder description = new StringBuilder(e.getClass().getName());
    Throwable innermost = e;
    for (int i = 0; i < MAX_LOGGED_CAUSES; i++) {
      Throwable cause = innermost.getCause();
      if (cause == null || cause == innermost) {
        break;
      }
      description.append(" caused by ").append(cause.getClass().getName());
      innermost = cause;
    }
    StackTraceElement[] stackTrace = innermost.getStackTrace();
    if (stackTrace.length > 0) {
      description.append(" at ").append(stackTrace[0]);
    }
    return description.toString();
  }

  /// Creates an exception whose response also carries the messages of `e` and its causes. Use it only where those
  /// messages describe the caller's own input: the response reaches the caller, and other causes can name files, hosts
  /// or plugins on the controller.
  public static ControllerApplicationException withCausesInResponse(Logger logger, String message,
      Response.Status status, Throwable e) {
    return new ControllerApplicationException(logger, message, status.getStatusCode(), e, ExceptionLogMode.FULL, true);
  }

  /// Whether the response carries the cause chain, which only [#withCausesInResponse] asks for.
  public boolean shouldIncludeCausesInResponse() {
    return _includeCausesInResponse;
  }
}
