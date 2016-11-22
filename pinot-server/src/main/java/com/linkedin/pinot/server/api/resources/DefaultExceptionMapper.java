/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.server.api.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DefaultExceptionMapper implements ExceptionMapper<WebApplicationException>
{
  private static final transient ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public Response toResponse(final WebApplicationException exception)
  {
    Response.ResponseBuilder builder = Response.status(exception.getResponse().getStatus())
                                      .entity(toJson(exception))
                                      .type(MediaType.APPLICATION_JSON);
    return builder.build();
  }

  private String toJson(final WebApplicationException exception)
  {
    ErrorInfo errorInfo = new ErrorInfo(exception);

    // difference between try and catch block is that
    // ErrorInfo can contain more information that just message
    try
    {
      return MAPPER.writeValueAsString(errorInfo);
    }
    catch (JsonProcessingException e)
    {
      return "{\"message\":\"Error converting error message: " + e.getMessage() + " to string\"}";
    }
  }
}
