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
import java.net.URI;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestResponse;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.QUERY_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE)}))
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
  private HttpClientConnectionManager _httpConnMgr;

  @DELETE
  @Path("query/{brokerId}/{queryId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.CANCEL_QUERY)
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
      RequestConfig defaultRequestConfig = RequestConfig.custom()
          .setConnectionRequestTimeout(timeoutMs)
          .setSocketTimeout(timeoutMs).build();

      CloseableHttpClient client = HttpClients.custom().setConnectionManager(_httpConnMgr)
          .setDefaultRequestConfig(defaultRequestConfig).build();

      String protocol = _controllerConf.getControllerBrokerProtocol();
      int portOverride = _controllerConf.getControllerBrokerPortOverride();
      int port = portOverride > 0 ? portOverride : Integer.parseInt(broker.getPort());
      HttpDelete deleteMethod = new HttpDelete(
          String.format("%s://%s:%d/query/%d?verbose=%b", protocol, broker.getHostName(), port, queryId, verbose));
      try {
        Map<String, String> requestHeaders = createRequestHeaders(httpHeaders);
        requestHeaders.forEach(deleteMethod::setHeader);
        CloseableHttpResponse response = client.execute(deleteMethod);
        int status = response.getStatusLine().getStatusCode();
        String responseContent = EntityUtils.toString(response.getEntity());
        if (status == 200) {
          return responseContent;
        }
        if (status == 404) {
          throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
              .entity(String.format("Query: %s not found on the broker: %s", queryId, brokerId)).build());
        }
        throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(String
            .format("Failed to cancel query: %s on the broker: %s with unexpected status=%d and resp='%s'", queryId,
                brokerId, status, responseContent)).build());
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
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_RUNNING_QUERY)
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
      Map<String, List<InstanceInfo>> tableBrokers = _pinotHelixResourceManager.getTableToLiveBrokersMapping(
          httpHeaders.getHeaderString(DATABASE));
      Map<String, InstanceInfo> brokers = new HashMap<>();
      tableBrokers.values().forEach(list -> list.forEach(info -> brokers.putIfAbsent(getInstanceKey(info), info)));
      return getRunningQueries(brokers, timeoutMs, createRequestHeaders(httpHeaders));
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to get running queries due to error: " + e.getMessage()).build());
    }
  }

  private Map<String, Map<String, String>> getRunningQueries(Map<String, InstanceInfo> brokers, int timeoutMs,
      Map<String, String> requestHeaders) throws Exception {
    String protocol = _controllerConf.getControllerBrokerProtocol();
    int portOverride = _controllerConf.getControllerBrokerPortOverride();
    List<String> brokerUrls = new ArrayList<>();
    for (InstanceInfo broker : brokers.values()) {
      int port = portOverride > 0 ? portOverride : broker.getPort();
      brokerUrls.add(String.format("%s://%s:%d/queries", protocol, broker.getHost(), port));
    }
    LOGGER.debug("Getting running queries via broker urls: {}", brokerUrls);
    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(_executor, _httpConnMgr).executeGet(brokerUrls, requestHeaders, timeoutMs);
    Map<String, Map<String, String>> queriesByBroker = new HashMap<>();
    List<String> errMsgs = new ArrayList<>(brokerUrls.size());
    for (int i = 0; i < brokerUrls.size(); i++) {
      MultiHttpRequestResponse httpRequestResponse = null;
      try {
        // The completion order is different from brokerUrls, thus use uri in the response.
        httpRequestResponse = completionService.take().get();
        URI uri = httpRequestResponse.getURI();
        int status = httpRequestResponse.getResponse().getStatusLine().getStatusCode();
        String responseString = EntityUtils.toString(httpRequestResponse.getResponse().getEntity());
        // Unexpected server responses are collected and returned as exception.
        if (status != 200) {
          throw new Exception(String.format("Unexpected status=%d and response='%s' from uri='%s'", status,
              responseString, uri));
        }
        queriesByBroker.put(brokers.get(getInstanceKey(uri)).getInstanceName(),
            JsonUtils.stringToObject(responseString, Map.class));
      } catch (Exception e) {
        LOGGER.error("Failed to get queries", e);
        // Can't just throw exception from here as there is a need to release the other connections.
        // So just collect the error msg to throw them together after the for-loop.
        errMsgs.add(e.getMessage());
      } finally {
        if (httpRequestResponse != null) {
          httpRequestResponse.close();
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
