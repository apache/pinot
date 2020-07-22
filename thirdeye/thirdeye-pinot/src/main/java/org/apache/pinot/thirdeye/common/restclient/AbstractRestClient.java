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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A generic API Rest Client to perform short-lived http GET and POST
 */
public abstract class AbstractRestClient {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final HttpURLConnectionFactory connectionFactory;

  /**
   * Using a URLFactory to create URLs allows unit tests to mock out server communication.
   */
  public class HttpURLConnectionFactory {
    public HttpURLConnection openConnection(String url) throws MalformedURLException, IOException {
      return (HttpURLConnection) new URL(url).openConnection();
    }
  }

  /**
   * Set up the client with a default URLFactory that creates real HTTP connections.
   * For unit tests, we use the alternate constructor to pass a mock.
   */
  public AbstractRestClient() {
    connectionFactory = new HttpURLConnectionFactory();
  }

  /**
   * For testing only, create a client with an alternate URLFactory. This constructor allows
   * unit tests to mock server communication.
   */
  public AbstractRestClient(HttpURLConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  /**
   * Perform a GET request to the given URL, accepts a method that will parse the response as a parameter.
   * A timeout of zero is interpreted as an infinite timeout.
   * @param <T>  the type parameter defined as the return type of the response parser method
   * @param url the http url
   * @param host the host to connect to
   * @param headers the headers for communication
   */
  public <T> T doGet(String url, String host, Map<String, String> headers,
      ParseResponseFunction<InputStream, T> responseParserFunc) throws IOException {
    return doGet(url, host, headers, 0, 0, responseParserFunc);
  }

  /**
   * Perform a GET request to the given URL, accepts a method that will parse the response as a parameter.
   * @param <T>  the type parameter defined as the return type of the response parser method
   * @param url the http url
   * @param host the host to connect to
   * @param connectTimeout timeout in milliseconds
   * @param readTimeout timeout in milliseconds
   */
  public <T> T doGet(String url, String host, Map<String, String> reqHeaders, int connectTimeout, int readTimeout,
      ParseResponseFunction<InputStream, T> responseParserFunc) throws IOException {
    Map<String, String> headers = new HashMap<>(reqHeaders);
    headers.put("User-Agent", getClass().getName());
    headers.put("Host", host);
    headers.put("Accept", "application/json");

    return doRequest(url, "GET", new byte[0], null, connectTimeout, readTimeout, headers,
        responseParserFunc);
  }

  /**
   * Perform a POST request to the given URL, with a JSON or raw string as content.
   * If the payload is an object that will be serialized to JSON the isContentTypeJSON must be set to true
   * @param <T>  the type parameter defined as the return type of the response parser method
   * @param url the http url
   * @param host the host to connect to
   * @param postContent the post content
   * @param isContentTypeJson flag indicating if the content is json type or not
   */
  public <T> T doPost(String url, String host, Map<String, String> reqHeaders, Object postContent,
      boolean isContentTypeJson, ParseResponseFunction<InputStream, T> responseParserFunc) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OBJECT_MAPPER.writeValue(baos, postContent);
    byte[] content = baos.toByteArray();
    String contentType = isContentTypeJson ? "application/json" : null;

    Map<String, String> headers = new HashMap<>(reqHeaders);
    headers.put("User-Agent", getClass().getName());
    headers.put("Host", host);
    headers.put("Accept", "application/json");

    return doRequest(url, "POST", content, contentType, 0, 0, headers,
        responseParserFunc);
  }

  /**
   * Send a request to the given URL with the given parameters.
   * @param <T> the type parameter defined as the return type of the response parser method
   * @param url server url.
   * @param method HTTP method.
   * @param content request content.
   * @param contentType request content type.
   * @param connectTimeoutMillis connection timeout.
   * @param readTimeoutMillis read timeout.
   * @param headers any additional request headers.
   * @return T the result of the parser functions applied to the response content.
   */
  private <T> T doRequest(String url, String method, byte[] content, String contentType, int connectTimeoutMillis,
      int readTimeoutMillis, Map<String, String> headers, ParseResponseFunction<InputStream, T> responseParserFunc)
      throws IOException {
    long startedMillis = System.currentTimeMillis();

    HttpURLConnection conn = this.connectionFactory.openConnection(url);
    conn.setRequestMethod(method);
    conn.setRequestProperty("Content-Length", Integer.toString(content.length));
    conn.setConnectTimeout(connectTimeoutMillis);
    conn.setReadTimeout(readTimeoutMillis);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }
    if (content.length > 0) {
      if (contentType != null) {
        conn.setRequestProperty("Content-Type", contentType);
      }
      conn.setDoOutput(true);
      OutputStream outs = conn.getOutputStream();
      outs.write(content, 0, content.length);
      outs.flush();
      outs.close();
    }

    // Read the InputStream
    int code = conn.getResponseCode();
    boolean success = code >= 200 && code <= 299;
    InputStream in = success ? conn.getInputStream() : conn.getErrorStream();
    try {
      T result = responseParserFunc.parse(in);
      long durationMillis = System.currentTimeMillis() - startedMillis;

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format("'%s' '%s' succeeded in '%s'ms, response code '%s', response content '%s'.", method, url,
                durationMillis, code, result));
      }

      if (!success) {
        throw new IOException(
            String.format("'%s' '%s' failed in '%s'ms, response code '%s', response content '%s'.", method, url,
                durationMillis, code, result));
      }

      return result;
    } finally {
      drainAndClose(in);
    }
  }

  /**
   * Completely drain the input stream. This is important so that HTTP keep-alive functions properly.
   * See: http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
   */
  private void drainAndClose(InputStream stream) {
    if (stream == null) {
      return;
    }

    // Drain the stream completely
    try {
      while (stream.read() != -1) {
        ;
      }
    } catch (Exception ignored) {}

    // Close the stream
    try {
      stream.close();
    } catch (Exception ignored) {}
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
  public class ResponseToMap implements ParseResponseFunction<InputStream, Map<String, Object>> {
    public Map<String, Object> parse(InputStream inputStream) throws IOException {
      return OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
    }
  }
}
