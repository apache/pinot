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

import com.fasterxml.jackson.databind.JsonNode;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.requesthandler.SystemTableBrokerRequestHandler;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableImplV4;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.JsonUtils;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SQL;


/**
 * Internal endpoint used for broker-to-broker scatter-gather for system tables.
 * <p>
 * Returns a serialized {@link DataTable} payload (application/octet-stream) that represents the local broker's shard
 * of a system table query.
 */
@Path("/")
public class SystemTableDataTableResource {
  @Inject
  private SystemTableBrokerRequestHandler _systemTableBrokerRequestHandler;

  @POST
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("query/systemTable/datatable")
  @ManualAuthorization
  public Response processSystemTableDataTable(String requestBody,
      @Context org.glassfish.grizzly.http.server.Request requestContext, @Context HttpHeaders httpHeaders) {
    DataTable dataTable;
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(requestBody);
      if (requestJson == null || !requestJson.isObject() || !requestJson.has(SQL)) {
        dataTable = new DataTableImplV4();
        dataTable.addException(QueryErrorCode.JSON_PARSING, "Payload is missing the query string field 'sql'");
      } else {
        try (RequestScope requestScope = Tracing.getTracer().createRequestScope()) {
          requestScope.setRequestArrivalTimeMillis(System.currentTimeMillis());
          dataTable = _systemTableBrokerRequestHandler.handleSystemTableDataTableRequest(requestJson,
              PinotClientRequest.makeHttpIdentity(requestContext), requestScope, httpHeaders);
        }
      }
    } catch (Exception e) {
      dataTable = new DataTableImplV4();
      dataTable.addException(QueryErrorCode.QUERY_EXECUTION, e.getMessage());
    }

    try {
      return Response.ok(dataTable.toBytes(), MediaType.APPLICATION_OCTET_STREAM).build();
    } catch (Exception e) {
      // As a last resort, return an empty body; the caller will treat this as a failure.
      return Response.ok(new byte[0], MediaType.APPLICATION_OCTET_STREAM).build();
    }
  }
}
