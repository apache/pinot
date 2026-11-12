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

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebApplicationExceptionMapper.class);
  @VisibleForTesting
  static final int MAX_CAUSES_LENGTH = 2048;

  @Override
  public Response toResponse(Throwable t) {
    int status = 500;
    if (!(t instanceof WebApplicationException)) {
      LOGGER.error("Server error: ", t);
    } else {
      status = ((WebApplicationException) t).getResponse().getStatus();
    }
    SimpleHttpErrorInfo errorInfo = new SimpleHttpErrorInfo(status, getErrorMessage(t));
    return Response.status(status).entity(errorInfo).type(MediaType.APPLICATION_JSON).build();
  }

  /// Appends the cause chain only for exceptions created with [ControllerApplicationException#withCausesInResponse].
  @VisibleForTesting
  @Nullable
  static String getErrorMessage(Throwable t) {
    if (t instanceof ControllerApplicationException
        && ((ControllerApplicationException) t).shouldIncludeCausesInResponse()) {
      return ExceptionUtils.appendCauses(t.getMessage(), t.getCause(), MAX_CAUSES_LENGTH);
    }
    return t.getMessage();
  }
}
