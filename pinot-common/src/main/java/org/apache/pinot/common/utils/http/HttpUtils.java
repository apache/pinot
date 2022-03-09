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
package org.apache.pinot.common.utils.http;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.utils.TlsUtils;

public class HttpUtils {
  private HttpUtils() {
    // do not instantiate.
  }

  public static String sendGetRequest(String urlString)
      throws IOException {
    return constructResponse(open(urlString).getInputStream());
  }

  public static String sendGetRequestRaw(String urlString)
      throws IOException {
    return IOUtils.toString(open(urlString).getInputStream(), Charset.defaultCharset());
  }

  public static String sendPostRequest(String urlString, String payload)
      throws IOException {
    return sendPostRequest(urlString, payload, Collections.EMPTY_MAP);
  }

  public static String sendPostRequest(String urlString, String payload, Map<String, String> headers)
      throws IOException {
    HttpURLConnection httpConnection = open(urlString);
    httpConnection.setRequestMethod("POST");
    if (headers != null) {
      for (String key : headers.keySet()) {
        httpConnection.setRequestProperty(key, headers.get(key));
      }
    }

    if (payload != null && !payload.isEmpty()) {
      httpConnection.setDoOutput(true);
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
        writer.write(payload, 0, payload.length());
        writer.flush();
      }
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendPutRequest(String urlString, String payload)
      throws IOException {
    HttpURLConnection httpConnection = open(urlString);
    httpConnection.setDoOutput(true);
    httpConnection.setRequestMethod("PUT");

    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
      writer.write(payload);
      writer.flush();
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendPutRequest(String urlString, Map<String, String> headers, String payload)
      throws IOException {
    HttpURLConnection httpConnection = open(urlString);
    httpConnection.setDoOutput(true);
    httpConnection.setRequestMethod("PUT");
    if (headers != null) {
      for (Map.Entry<String, String> kv : headers.entrySet()) {
        httpConnection.setRequestProperty(kv.getKey(), kv.getValue());
      }
    }

    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
      writer.write(payload);
      writer.flush();
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendPutRequest(String urlString)
      throws IOException {
    HttpURLConnection httpConnection = open(urlString);
    httpConnection.setDoOutput(true);
    httpConnection.setRequestMethod("PUT");
    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendDeleteRequest(String urlString)
      throws IOException {
    HttpURLConnection httpConnection = open(urlString);
    httpConnection.setRequestMethod("DELETE");
    httpConnection.connect();

    return constructResponse(httpConnection.getInputStream());
  }

  private static HttpURLConnection open(String urlString)
      throws IOException {
    URLConnection urlConnection = new URL(urlString).openConnection();
    if (urlConnection instanceof HttpsURLConnection) {
      HttpsURLConnection httpsConnection = (HttpsURLConnection) urlConnection;
      httpsConnection.setSSLSocketFactory(TlsUtils.getDefaultSSLContext().getSocketFactory());
      return httpsConnection;
    } else if (urlConnection instanceof HttpURLConnection) {
      return (HttpURLConnection) urlConnection;
    } else {
      throw new IllegalArgumentException("urlString must use HTTP/HTTPS protocol");
    }
  }

  private static String constructResponse(InputStream inputStream)
      throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder responseBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        responseBuilder.append(line);
      }
      return responseBuilder.toString();
    }
  }

  public static PostMethod sendMultipartPostRequest(String url, String body)
      throws IOException {
    HttpClient httpClient = new HttpClient();
    PostMethod postMethod = new PostMethod(url);
    // our handlers ignore key...so we can put anything here
    Part[] parts = {new StringPart("body", body)};
    postMethod.setRequestEntity(new MultipartRequestEntity(parts, postMethod.getParams()));
    httpClient.executeMethod(postMethod);
    return postMethod;
  }

  public static PutMethod sendMultipartPutRequest(String url, String body)
      throws IOException {
    HttpClient httpClient = new HttpClient();
    PutMethod putMethod = new PutMethod(url);
    // our handlers ignore key...so we can put anything here
    Part[] parts = {new StringPart("body", body)};
    putMethod.setRequestEntity(new MultipartRequestEntity(parts, putMethod.getParams()));
    httpClient.executeMethod(putMethod);
    return putMethod;
  }
}
