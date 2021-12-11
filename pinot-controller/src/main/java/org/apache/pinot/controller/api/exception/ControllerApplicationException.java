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

  public ControllerApplicationException(Logger logger, String message, Response.Status status) {
    this(logger, message, status.getStatusCode(), null);
  }

  public ControllerApplicationException(Logger logger, String message, int status) {
    this(logger, message, status, null);
  }

  public ControllerApplicationException(Logger logger, String message, Response.Status status, @Nullable Throwable e) {
    this(logger, message, status.getStatusCode(), e);
  }

  public ControllerApplicationException(Logger logger, String message, int status, @Nullable Throwable e) {
    super(message, status);
    if (status >= 300 && status < 500) {
      if (e == null) {
        logger.info(message);
      } else {
        logger.info(message + " exception: " + e.getMessage());
      }
    } else {
      if (e == null) {
        logger.error(message);
      } else {
        logger.error(message, e);
      }
    }
  }
}
