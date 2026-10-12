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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Maps exceptions to JSON error responses, appending the cause chain to the message unless
/// [ControllerConf#API_ERROR_RESPONSE_INCLUDE_CAUSES] is false.
@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebApplicationExceptionMapper.class);
  @VisibleForTesting
  static final int MAX_CAUSES = 5;
  @VisibleForTesting
  static final int MAX_CAUSE_LENGTH = 1024;

  @Context
  private Application _application;

  @Nullable
  private final Boolean _includeCauses;

  public WebApplicationExceptionMapper() {
    _includeCauses = null;
  }

  @VisibleForTesting
  WebApplicationExceptionMapper(boolean includeCauses) {
    _includeCauses = includeCauses;
  }

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

  @VisibleForTesting
  @Nullable
  String getErrorMessage(Throwable t) {
    if (!shouldIncludeCauses()) {
      return t.getMessage();
    }
    String message = t.getMessage();
    if (StringUtils.isBlank(message) && !(t instanceof WebApplicationException) && t.getCause() == null) {
      String simpleName = t.getClass().getSimpleName();
      return simpleName.isEmpty() ? t.getClass().getName() : simpleName;
    }
    return ExceptionUtils.appendCauses(message, t.getCause(), MAX_CAUSES, MAX_CAUSE_LENGTH);
  }

  private boolean shouldIncludeCauses() {
    if (_includeCauses != null) {
      return _includeCauses;
    }
    if (_application != null) {
      Object config = _application.getProperties().get(ControllerAdminApiApplication.PINOT_CONFIGURATION);
      if (config instanceof ControllerConf) {
        return ((ControllerConf) config).isApiErrorResponseIncludeCauses();
      }
    }
    return ControllerConf.DEFAULT_API_ERROR_RESPONSE_INCLUDE_CAUSES;
  }
}
