/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.controller.api.access.AccessControl;
import com.linkedin.pinot.controller.api.access.AccessControlFactory;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PqlQueryResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PqlQueryResource.class);
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
  private static final Random RANDOM = new Random();

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  AccessControlFactory _accessControlFactory;

  @POST
  @Path("pql")
  public String post(String requestJsonStr, @Context HttpHeaders httpHeaders) {
    try {
      JSONObject requestJson = new JSONObject(requestJsonStr);
      String pqlQuery = requestJson.getString("pql");
      String traceEnabled = "false";
      if (requestJson.has("trace")) {
        traceEnabled = requestJson.get("trace").toString();
      }
      LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, pqlQuery);
      return getQueryResponse(pqlQuery, traceEnabled, httpHeaders);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing post request", e);
      return QueryException.getException(QueryException.INTERNAL_ERROR, e).toString();
    }
  }

  @GET
  @Path("pql")
  public String get(
      @QueryParam("pql") String pqlQuery,
      @QueryParam("trace") String traceEnabled,
      @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, pqlQuery);
      return getQueryResponse(pqlQuery, traceEnabled, httpHeaders);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing get request", e);
      return QueryException.getException(QueryException.INTERNAL_ERROR, e).toString();
    }
  }

  public String getQueryResponse(String pqlQuery, String traceEnabled, HttpHeaders httpHeaders) {
    // Get resource table name.
    BrokerRequest brokerRequest;
    try {
      brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pqlQuery);
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling PQL query: {}, {}", pqlQuery, e.getMessage());
      return QueryException.getException(QueryException.PQL_PARSING_ERROR, e).toString();
    }
    String tableName = TableNameBuilder.extractRawTableName(brokerRequest.getQuerySource().getTableName());

    // Validate data access
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasDataAccess(httpHeaders, tableName)) {
      return QueryException.ACCESS_DENIED_ERROR.toString();
    }

    // Get brokers for the resource table.
    List<String> instanceIds = _pinotHelixResourceManager.getBrokerInstancesFor(tableName);
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_RESOURCE_MISSING_ERROR.toString();
    }

    // Retain only online brokers.
    instanceIds.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_INSTANCE_MISSING_ERROR.toString();
    }

    // Send query to a random broker.
    String instanceId = instanceIds.get(RANDOM.nextInt(instanceIds.size()));
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
    String hostNameWithPrefix = instanceConfig.getHostName();
    String url = "http://" + hostNameWithPrefix.substring(hostNameWithPrefix.indexOf("_") + 1) + ":"
        + instanceConfig.getPort() + "/query";
    return sendPQLRaw(url, pqlQuery, traceEnabled);
  }

  public String sendPostRaw(String urlStr, String requestStr, Map<String, String> headers) {
    HttpURLConnection conn = null;
    try {
      /*if (LOG.isInfoEnabled()){
        LOGGER.info("Sending a post request to the server - " + urlStr);
      }

      if (LOG.isDebugEnabled()){
        LOGGER.debug("The request is - " + requestStr);
      }*/

      LOGGER.info("url string passed is : " + urlStr);
      final URL url = new URL(urlStr);
      conn = (HttpURLConnection) url.openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      // conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

      conn.setRequestProperty("Accept-Encoding", "gzip");

      final String string = requestStr;
      final byte[] requestBytes = string.getBytes("UTF-8");
      conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
      conn.setRequestProperty("http.keepAlive", String.valueOf(true));
      conn.setRequestProperty("default", String.valueOf(true));

      if (headers != null && headers.size() > 0) {
        final Set<Entry<String, String>> entries = headers.entrySet();
        for (final Entry<String, String> entry : entries) {
          conn.setRequestProperty(entry.getKey(), entry.getValue());
        }
      }

      //GZIPOutputStream zippedOutputStream = new GZIPOutputStream(conn.getOutputStream());
      final OutputStream os = new BufferedOutputStream(conn.getOutputStream());
      os.write(requestBytes);
      os.flush();
      os.close();
      final int responseCode = conn.getResponseCode();

      /*if (LOG.isInfoEnabled()){
        LOGGER.info("The http response code is " + responseCode);
      }*/
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("Failed : HTTP error code : " + responseCode);
      }
      final byte[] bytes = drain(new BufferedInputStream(conn.getInputStream()));

      final String output = new String(bytes, "UTF-8");
      /*if (LOG.isDebugEnabled()){
        LOGGER.debug("The response from the server is - " + output);
      }*/
      return output;
    } catch (final Exception ex) {
      LOGGER.error("Caught exception while sending pql request", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  byte[] drain(InputStream inputStream) throws IOException {
    try {
      final byte[] buf = new byte[1024];
      int len;
      final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      while ((len = inputStream.read(buf)) > 0) {
        byteArrayOutputStream.write(buf, 0, len);
      }
      return byteArrayOutputStream.toByteArray();
    } finally {
      inputStream.close();
    }
  }

  public String sendPQLRaw(String url, String pqlRequest, String traceEnabled) {
    try {
      final long startTime = System.currentTimeMillis();
      final JSONObject bqlJson = new JSONObject().put("pql", pqlRequest);
      if (traceEnabled != null && !traceEnabled.isEmpty()) {
        bqlJson.put("trace", traceEnabled);
      }

      final String pinotResultString = sendPostRaw(url, bqlJson.toString(), null);

      final long bqlQueryTime = System.currentTimeMillis() - startTime;
      LOGGER.info("BQL: " + pqlRequest + " Time: " + bqlQueryTime);

      return pinotResultString;
    } catch (final Exception ex) {
      LOGGER.error("Caught exception in sendPQLRaw", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    }
  }
}
