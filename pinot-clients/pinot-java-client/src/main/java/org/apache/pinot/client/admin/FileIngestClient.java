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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;


/**
 * Explicit client for Pinot controller endpoints that accept file or raw-body uploads.
 */
public class FileIngestClient extends BaseServiceAdminClient {
  public FileIngestClient(PinotAdminTransport transport, String controllerAddress, Map<String, String> headers) {
    super(transport, controllerAddress, headers);
  }

  /**
   * Builds the ingestion URL for ingestFromFile.
   */
  public String buildIngestFromFileUrl(String tableNameWithType, Map<String, String> batchConfigMap)
      throws PinotAdminException {
    String batchConfigMapStr = serializeBatchConfigMap(batchConfigMap);
    String baseUrl = _transport.getScheme() + "://" + _controllerAddress;
    return baseUrl + "/ingestFromFile?tableNameWithType=" + tableNameWithType + "&batchConfigMapStr="
        + URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8);
  }

  /**
   * Builds the ingestion URL for ingestFromURI.
   */
  public String buildIngestFromUriUrl(String tableNameWithType, Map<String, String> batchConfigMap, String sourceUri)
      throws PinotAdminException {
    String batchConfigMapStr = serializeBatchConfigMap(batchConfigMap);
    String baseUrl = _transport.getScheme() + "://" + _controllerAddress;
    return baseUrl + "/ingestFromURI?tableNameWithType=" + tableNameWithType + "&batchConfigMapStr="
        + URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8) + "&sourceURIStr="
        + URLEncoder.encode(sourceUri, StandardCharsets.UTF_8);
  }

  private String serializeBatchConfigMap(Map<String, String> batchConfigMap)
      throws PinotAdminException {
    try {
      return PinotAdminTransport.getObjectMapper().writeValueAsString(batchConfigMap);
    } catch (JsonProcessingException e) {
      throw new PinotAdminException("Failed to serialize batch config map", e);
    }
  }

  /**
   * Posts a multipart file upload to the ingestFromFile endpoint.
   */
  public int ingestFromFile(String tableNameWithType, Map<String, String> batchConfigMap, File inputFile)
      throws PinotAdminException {
    return postFile(buildIngestFromFileUrl(tableNameWithType, batchConfigMap), inputFile);
  }

  /**
   * Posts a multipart file upload to the ingestFromURI endpoint.
   */
  public int ingestFromUri(String tableNameWithType, Map<String, String> batchConfigMap, String sourceUri,
      File inputFile)
      throws PinotAdminException {
    return postFile(buildIngestFromUriUrl(tableNameWithType, batchConfigMap, sourceUri), inputFile);
  }

  /**
   * Posts a multipart file to an explicit URL.
   */
  public int postFile(String url, File inputFile)
      throws PinotAdminException {
    HttpEntity reqEntity =
        MultipartEntityBuilder.create().addPart("file", new FileBody(inputFile.getAbsoluteFile())).build();
    return execute(url, reqEntity);
  }

  /**
   * Posts a plain string body to an explicit URL.
   */
  public int postString(String url, String body)
      throws PinotAdminException {
    return execute(url, new StringEntity(body));
  }

  private int execute(String url, HttpEntity entity)
      throws PinotAdminException {
    try (CloseableHttpClient httpClient = buildHttpClient()) {
      HttpPost httpPost = new HttpPost(url);
      for (Map.Entry<String, String> header : _headers.entrySet()) {
        httpPost.setHeader(header.getKey(), header.getValue());
      }
      httpPost.setEntity(entity);
      try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
        return response.getCode();
      }
    } catch (IOException e) {
      throw new PinotAdminException("Failed to execute file ingest request to " + url, e);
    }
  }

  private CloseableHttpClient buildHttpClient() {
    SSLContext sslContext = _transport.getSslContext();
    if (sslContext != null) {
      return HttpClients.custom()
          .setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
              .setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                  .setSslContext(sslContext)
                  .build())
              .build())
          .build();
    }
    return HttpClients.createDefault();
  }
}
