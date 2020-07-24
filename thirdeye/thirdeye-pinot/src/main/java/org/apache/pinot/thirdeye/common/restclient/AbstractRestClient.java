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
 */

package org.apache.pinot.thirdeye.common.restclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A generic Jersey Client API to perform GET and POST
 */
public abstract class AbstractRestClient {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final Client client;

  /**
   * Set up a default Jersey client.
   * For unit tests, we use the alternate constructor to pass a mock.
   */
  public AbstractRestClient() {
    this.client = ClientBuilder.newClient(new ClientConfig());
  }

  /**
   * For testing only, create a client with an alternate client. This constructor allows
   * unit tests to mock server communication.
   */
  public AbstractRestClient(Client client) {
    this.client = client;
  }

  /**
   * Perform a GET request to the given URL, accepts a method that will parse the response as a parameter.
   * @param <T>  the type parameter defined as the return type of the response parser method
   * @param url the http url
   */
  public <T> T doGet(String url, MultivaluedMap<String, Object> headers, ParseResponseFunction<Response, T> responseParserFunc) throws IOException {
    long startedMillis = System.currentTimeMillis();

    WebTarget webTarget = this.client.target(url);

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).headers(headers);
    Response response = invocationBuilder.get();

    T result = responseParserFunc.parse(response);
    long durationMillis = System.currentTimeMillis() - startedMillis;

    LOG.info(String.format("GET '%s' succeeded in '%s'ms, response code '%s', response content '%s'.", url,
        durationMillis, response.getStatus(), result));

    return result;
  }

  /**
   * Perform a POST request to the given URL, with a JSON or raw string as content.
   * @param url the http url
   * @param postContent the post content
   */
  public <T> T doPost(String url, MultivaluedMap<String, Object> headers, Object postContent,
      ParseResponseFunction<Response, T> responseParserFunc) throws IOException {
    long startedMillis = System.currentTimeMillis();

    WebTarget webTarget = this.client.target(url);

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).headers(headers);
    Response response = invocationBuilder.post(Entity.entity(postContent, MediaType.APPLICATION_JSON));

    T result = responseParserFunc.parse(response);
    long durationMillis = System.currentTimeMillis() - startedMillis;

    LOG.info(String.format("POST '%s' succeeded in '%s'ms, response code '%s', response content '%s'.", url,
            durationMillis, response.getStatus(), result));

    return result;
  }

  /**
   * Composes a url from the host, api and queryParameters
   */
  protected String composeUrl(String host, String api, SortedMap<String, String> queryParameters) throws IOException {
    return composeUrlGeneric(Protocol.HTTP, host, api, queryParameters);
  }

  /**
   * Composes a url from the protocol, host, api and queryParameters
   */
  protected String composeUrlGeneric(Protocol protocol, String host, String api,
      SortedMap<String, String> queryParameters) throws IOException {
    StringBuilder url = new StringBuilder();
    for (Protocol scheme : Protocol.values()) {
      if (host.contains(scheme.val())) {
        // protocol/scheme is already part of the host, do not append protocol
        url.append(host);
        break;
      }
    }
    if (url.toString().isEmpty()) {
      url.append(protocol.val()).append(host);
    }
    url.append(api);

    if (queryParameters != null) {
      try {
        boolean firstQueryParam = true;
        for (Map.Entry<String, String> entry : queryParameters.entrySet()) {
          url.append(firstQueryParam ? "?" : "&");
          firstQueryParam = false;
          url.append(entry.getKey()).append('=');
          url.append(URLEncoder.encode(entry.getValue(), "UTF-8")
              .replace("+", "%20")
              .replace("%2F", "/"));
        }
      } catch (UnsupportedEncodingException ex) {
        // Should never happen since UTF-8 is always supported.
        throw new IOException("Failed to URLEncode query parameters.", ex);
      }
    }
    return url.toString();
  }

  /**
   * Implementation for a single method class that can be used with doGet and doPost in order to parse a response stream
   * to a Map<String,Object>
   */
  public class ResponseToMap implements ParseResponseFunction<Response, Map<String, Object>> {
    public Map<String, Object> parse(Response response) {
      return response.readEntity(new GenericType<HashMap<String, Object>>() { });
    }
  }
}
