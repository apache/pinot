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
package org.apache.pinot.broker.api.resources;

import javax.inject.Singleton;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
@Singleton
public class BrokerCommonExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCommonExceptionMapper.class);
  private static final int DEFAULT_STATUS = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();

  @Override
  public Response toResponse(Throwable t) {
    int status = DEFAULT_STATUS;
    if (t instanceof WebApplicationException) {
      status = ((WebApplicationException) t).getResponse().getStatus();
    }
    SimpleHttpErrorInfo errorInfo = new SimpleHttpErrorInfo(status, t.getMessage());
    return Response.status(status).entity(errorInfo).type(MediaType.APPLICATION_JSON).build();
  }
}
