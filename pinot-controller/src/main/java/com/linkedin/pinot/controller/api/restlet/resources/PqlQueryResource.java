/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.antlr.runtime.RecognitionException;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


/**
 * Dec 8, 2014
 */

public class PqlQueryResource extends PinotRestletResourceBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(PqlQueryResource.class);
  private static final PQLCompiler compiler = new PQLCompiler(new HashMap<String, String[]>());;

  public PqlQueryResource() {
  }

  @Override
  public Representation get() {
    final String pqlString = getQuery().getValues("pql");
    LOGGER.info("*** found pql : " + pqlString);
    JSONObject compiledJSON;

    try {
      compiledJSON = compiler.compile(pqlString);
    } catch (final RecognitionException e) {
      LOGGER.error("Caught exception while processing get request", e);
      ProcessingException parsingException = QueryException.PQL_PARSING_ERROR.deepCopy();
      parsingException.setMessage(e.toString());
      return new StringRepresentation(parsingException.toString());
    }

    if (!compiledJSON.has("collection")) {
      return new StringRepresentation("your request does not contain the collection information");
    }

    final String resource;
    try {
      resource = compiledJSON.getString("collection");
    } catch (final JSONException e) {
      LOGGER.error("Caught exception while processing get request", e);
      return new StringRepresentation(QueryException.BROKER_RESOURCE_MISSING_ERROR.toString());
    }

    final String instanceId;
    final InstanceConfig config;
    try {
      final List<String> instanceIds = _pinotHelixResourceManager.getBrokerInstancesFor(resource);
      if (instanceIds.isEmpty()) {
        return new StringRepresentation(QueryException.BROKER_INSTANCE_MISSING_ERROR.toString());
      } else {
        Collections.shuffle(instanceIds);
        instanceId = instanceIds.get(0);
        config = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while processing get request", e);
      return new StringRepresentation(QueryException.BROKER_INSTANCE_MISSING_ERROR.toString());
    }


    final String resp = sendPQLRaw("http://" + config.getHostName().split("_")[1] + ":" + config.getPort() + "/query", pqlString);

    return new StringRepresentation(resp);
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

  public String sendPQLRaw(String url, String pqlRequest) {
    try {
      final long startTime = System.currentTimeMillis();
      final JSONObject bqlJson = new JSONObject().put("pql", pqlRequest);

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
