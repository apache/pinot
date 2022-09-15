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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.QUERY_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotRunningQueryResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRunningQueryResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  private Executor _executor;

  @Inject
  private HttpConnectionManager _httpConnMgr;

  @DELETE
  @Path("query/{brokerId}/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Cancel a query as identified by the queryId", notes = "No effect if no query exists for the "
      + "given queryId on the requested broker. Query may continue to run for a short while after calling cancel as "
      + "it's done in a non-blocking manner. The cancel method can be called multiple times.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Query not found on the requested broker")
  })
  public String cancelQuery(
      @ApiParam(value = "Broker that's running the query", required = true) @PathParam("brokerId") String brokerId,
      @ApiParam(value = "QueryId as assigned by the broker", required = true) @PathParam("queryId") long queryId,
      @ApiParam(value = "Timeout for servers to respond the cancel request") @QueryParam("timeoutMs")
      @DefaultValue("3000") int timeoutMs,
      @ApiParam(value = "Return verbose responses for troubleshooting") @QueryParam("verbose") @DefaultValue("false")
          boolean verbose, @Context HttpHeaders httpHeaders) {
    InstanceConfig broker = _pinotHelixResourceManager.getHelixInstanceConfig(brokerId);
    if (broker == null) {
      throw new WebApplicationException(
          Response.status(Response.Status.BAD_REQUEST).entity("Unknown broker: " + brokerId).build());
    }
    try {
      HttpClientParams clientParams = new HttpClientParams();
      clientParams.setConnectionManagerTimeout(timeoutMs);
      HttpClient client = new HttpClient(clientParams, _httpConnMgr);
      String protocol = _controllerConf.getControllerBrokerProtocol();
      int portOverride = _controllerConf.getControllerBrokerPortOverride();
      int port = portOverride > 0 ? portOverride : Integer.parseInt(broker.getPort());
      DeleteMethod deleteMethod = new DeleteMethod(
          String.format("%s://%s:%d/query/%d?verbose=%b", protocol, broker.getHostName(), port, queryId, verbose));
      try {
        Map<String, String> requestHeaders = createRequestHeaders(httpHeaders);
        requestHeaders.forEach(deleteMethod::setRequestHeader);
        client.executeMethod(deleteMethod);
        int status = deleteMethod.getStatusCode();
        if (status == 200) {
          return deleteMethod.getResponseBodyAsString();
        }
        if (status == 404) {
          throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
              .entity(String.format("Query: %s not found on the broker: %s", queryId, brokerId)).build());
        }
        throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(String
            .format("Failed to cancel query: %s on the broker: %s with unexpected status=%d and resp='%s'", queryId,
                brokerId, status, deleteMethod.getResponseBodyAsString())).build());
      } finally {
        deleteMethod.releaseConnection();
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(String
          .format("Failed to cancel query: %s on the broker: %s due to error: %s", queryId, brokerId, e.getMessage()))
          .build());
    }
  }

  @GET
  @Path("/queries")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get running queries from all brokers", notes = "The queries are returned with brokers "
      + "running them")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<String, Map<String, String>> getRunningQueries(
      @ApiParam(value = "Timeout for brokers to return running queries") @QueryParam("timeoutMs") @DefaultValue("3000")
          int timeoutMs, @Context HttpHeaders httpHeaders) {
    try {
      Map<String, List<InstanceInfo>> tableBrokers = _pinotHelixResourceManager.getTableToLiveBrokersMapping();
      Map<String, InstanceInfo> brokers = new HashMap<>();
      tableBrokers.values().forEach(list -> list.forEach(info -> brokers.putIfAbsent(getInstanceKey(info), info)));
      return getRunningQueries(brokers, timeoutMs, createRequestHeaders(httpHeaders));
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to get running queries due to error: " + e.getMessage()).build());
    }
  }

  private Map<String, Map<String, String>> getRunningQueries(Map<String, InstanceInfo> brokers, int timeoutMs,
      Map<String, String> requestHeaders)
      throws Exception {
    String protocol = _controllerConf.getControllerBrokerProtocol();
    int portOverride = _controllerConf.getControllerBrokerPortOverride();
    List<String> brokerUrls = new ArrayList<>();
    for (InstanceInfo broker : brokers.values()) {
      int port = portOverride > 0 ? portOverride : broker.getPort();
      brokerUrls.add(String.format("%s://%s:%d/queries", protocol, broker.getHost(), port));
    }
    LOGGER.debug("Getting running queries via broker urls: {}", brokerUrls);
    CompletionService<GetMethod> completionService =
        new MultiHttpRequest(_executor, _httpConnMgr).execute(brokerUrls, requestHeaders, timeoutMs);
    Map<String, Map<String, String>> queriesByBroker = new HashMap<>();
    List<String> errMsgs = new ArrayList<>(brokerUrls.size());
    for (int i = 0; i < brokerUrls.size(); i++) {
      GetMethod getMethod = null;
      try {
        // The completion order is different from brokerUrls, thus use uri in the response.
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        int status = getMethod.getStatusCode();
        // Unexpected server responses are collected and returned as exception.
        if (status != 200) {
          throw new Exception(String.format("Unexpected status=%d and response='%s' from uri='%s'", status,
              getMethod.getResponseBodyAsString(), uri));
        }
        queriesByBroker.put(brokers.get(getInstanceKey(uri)).getInstanceName(),
            JsonUtils.stringToObject(getMethod.getResponseBodyAsString(), Map.class));
      } catch (Exception e) {
        LOGGER.error("Failed to get queries", e);
        // Can't just throw exception from here as there is a need to release the other connections.
        // So just collect the error msg to throw them together after the for-loop.
        errMsgs.add(e.getMessage());
      } finally {
        if (getMethod != null) {
          getMethod.releaseConnection();
        }
      }
    }
    if (errMsgs.size() > 0) {
      throw new Exception("Unexpected responses from brokers: " + StringUtils.join(errMsgs, ","));
    }
    return queriesByBroker;
  }

  private static String getInstanceKey(InstanceInfo info) {
    return info.getHost() + ":" + info.getPort();
  }

  private static String getInstanceKey(URI uri)
      throws Exception {
    return uri.getHost() + ":" + uri.getPort();
  }

  private static Map<String, String> createRequestHeaders(HttpHeaders httpHeaders) {
    Map<String, String> requestHeaders = new HashMap<>();
    httpHeaders.getRequestHeaders().keySet().forEach(header -> {
      requestHeaders.put(header, httpHeaders.getHeaderString(header));
    });
    return requestHeaders;
  }
}
