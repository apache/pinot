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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Api(tags = "testResource")
@Path("/testResource")
public class DummyTestResource {
  @GET
  @Path("requestFilter")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Test API for table name translation in request filter")
  public Map<String, String> getDummyMsg(@QueryParam("tableName") String tableName,
      @QueryParam("tableNameWithType") String tableNameWithType,
      @QueryParam("schemaName") String schemaName) {
    Map<String, String> ret = new HashMap<>();
    ret.put("tableName", tableName);
    ret.put("tableNameWithType", tableNameWithType);
    ret.put("schemaName", schemaName);
    return ret;
  }
}
