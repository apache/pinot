/*
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
 *
 */

package org.apache.pinot.thirdeye.dashboard.resources;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * This exception is thrown when the user makes an error in the request. This in turn fires a
 * {@link WebApplicationException} with BAD_REQUEST(400) status code.
 *
 * Please check the utilites in the {@link ResourceUtils} class to easily use this class.
 */
public class BadRequestWebException extends WebApplicationException {

  public BadRequestWebException() {
    this("Bad or Malformed Request");
  }

  public BadRequestWebException(Exception e) {
    super(Response
        .status(Status.BAD_REQUEST)
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build());
  }

  public BadRequestWebException(String message) {
    super(Response
        .status(Status.BAD_REQUEST)
        .entity(message)
        .type(MediaType.TEXT_PLAIN)
        .build());
  }
}
