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
package org.apache.pinot.common.swagger;

import io.swagger.annotations.ApiOperation;
import io.swagger.jaxrs.listing.BaseApiListingResource;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;


/*
 This class is required to avoid the jersey2 warning messages regarding servlet config constructor missing while
 injecting the config. Please refer to Pinot issue 13047 & 5306 for more context.
 In this implementation, we added the ServletConfig as the class level member instead of injecting it.
*/
@Path("/swagger.{type:json|yaml}")
public class SwaggerApiListingResource extends BaseApiListingResource {
  @Context
  ServletContext _context;

  @Context
  ServletConfig _servletConfig;

  @GET
  @Produces({MediaType.APPLICATION_JSON, "application/yaml"})
  @ApiOperation(value = "The swagger definition in either JSON or YAML", hidden = true)
  public Response getListing(@Context Application app, @Context HttpHeaders headers, @Context UriInfo uriInfo,
      @PathParam("type") String type) {
    if (StringUtils.isNotBlank(type) && type.trim().equalsIgnoreCase("yaml")) {
      return getListingYamlResponse(app, _context, _servletConfig, headers, uriInfo);
    } else {
      return getListingJsonResponse(app, _context, _servletConfig, headers, uriInfo);
    }
  }
}
