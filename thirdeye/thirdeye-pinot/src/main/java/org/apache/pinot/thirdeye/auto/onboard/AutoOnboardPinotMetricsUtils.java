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

package org.apache.pinot.thirdeye.auto.onboard;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.thirdeye.datasource.MetadataSourceConfig;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoOnboardPinotMetricsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardPinotMetricsUtils.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final org.codehaus.jackson.map.ObjectMapper CODEHAUS_OBJECT_MAPPER =
      new org.codehaus.jackson.map.ObjectMapper();

  private static final String PINOT_TABLES_ENDPOINT = "/tables/";
  private static final String PINOT_TABLES_ENDPOINT_TEMPLATE = "/tables/%s";
  private static final String PINOT_SCHEMA_ENDPOINT_TEMPLATE = "/schemas/%s";
  private static final String PINOT_TABLE_CONFIG_ENDPOINT_TEMPLATE = "/tables/%s/schema";
  private static final String UTF_8 = "UTF-8";

  private CloseableHttpClient pinotControllerClient;
  private HttpHost pinotControllerHost;

  public AutoOnboardPinotMetricsUtils(MetadataSourceConfig metadataSourceConfig)
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    PinotThirdEyeDataSourceConfig pinotThirdeyeDataSourceConfig =
        PinotThirdEyeDataSourceConfig.createFromMetadataSourceConfig(metadataSourceConfig);

    String controllerConnectionScheme = pinotThirdeyeDataSourceConfig.getControllerConnectionScheme();
    if (PinotThirdEyeDataSourceConfig.HTTPS_SCHEME.equals(controllerConnectionScheme)) {
      try {
        // Accept all SSL certificate because we assume that the Pinot broker are setup in the same internal network
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new AcceptAllTrustStrategy()).build();
        this.pinotControllerClient =
            HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
      } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
        // This section shouldn't happen because we use Accept All Strategy
        LOG.error("Failed to start auto onboard for Pinot data source.");
        throw e;
      }
    } else {
      this.pinotControllerClient = HttpClients.createDefault();
    }

    this.pinotControllerHost = new HttpHost(pinotThirdeyeDataSourceConfig.getControllerHost(),
        pinotThirdeyeDataSourceConfig.getControllerPort(), controllerConnectionScheme);
  }

  public JsonNode getAllTablesFromPinot() throws IOException {
    HttpGet tablesReq = new HttpGet(PINOT_TABLES_ENDPOINT);
    LOG.info("Retrieving datasets: {}", tablesReq);
    CloseableHttpResponse tablesRes = pinotControllerClient.execute(pinotControllerHost, tablesReq);
    JsonNode tables = null;
    try {
      if (tablesRes.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(tablesRes.getStatusLine().toString());
      }
      InputStream tablesContent = tablesRes.getEntity().getContent();
      tables = OBJECT_MAPPER.readTree(tablesContent).get("tables");
    } catch (Exception e) {
      LOG.error("Exception in loading collections", e);
    } finally {
      if (tablesRes.getEntity() != null) {
        EntityUtils.consume(tablesRes.getEntity());
      }
      tablesRes.close();
    }
    return tables;
  }

  /**
   * Fetches schema from pinot, from the tables endpoint or schema endpoint
   *
   * @param dataset
   *
   * @return
   *
   * @throws IOException
   */
  public Schema getSchemaFromPinot(String dataset) throws IOException {
    Schema schema = getSchemaFromPinotEndpoint(PINOT_TABLE_CONFIG_ENDPOINT_TEMPLATE, dataset);
    if (schema == null) {
      schema = getSchemaFromPinotEndpoint(PINOT_SCHEMA_ENDPOINT_TEMPLATE, dataset);
    }
    if (schema == null) {
      schema = getSchemaFromPinotEndpoint(PINOT_SCHEMA_ENDPOINT_TEMPLATE, dataset + "_OFFLINE");
    }
    return schema;
  }

  private Schema getSchemaFromPinotEndpoint(String endpointTemplate, String dataset) throws IOException {
    Schema schema = null;
    HttpGet schemaReq = new HttpGet(String.format(endpointTemplate, URLEncoder.encode(dataset, UTF_8)));
    LOG.info("Retrieving schema: {}", schemaReq);
    CloseableHttpResponse schemaRes = pinotControllerClient.execute(pinotControllerHost, schemaReq);
    try {
      if (schemaRes.getStatusLine().getStatusCode() != 200) {
        LOG.error("Schema {} not found, {}", dataset, schemaRes.getStatusLine().toString());
      } else {
        InputStream schemaContent = schemaRes.getEntity().getContent();
        schema = CODEHAUS_OBJECT_MAPPER.readValue(schemaContent, Schema.class);
      }

    } catch (Exception e) {
      LOG.error("Exception in retrieving schema collections, skipping {}", dataset);
    } finally {
      if (schemaRes.getEntity() != null) {
        EntityUtils.consume(schemaRes.getEntity());
      }
      schemaRes.close();
    }
    return schema;
  }

  /**
   * Verify schema name and presence of field spec for time column
   */
  public boolean verifySchemaCorrectness(Schema schema, @Nullable String timeColumnName) {
    if (StringUtils.isBlank(schema.getSchemaName()) || timeColumnName == null
        || schema.getSpecForTimeColumn(timeColumnName) == null) {
      return false;
    }
    return true;
  }

  public JsonNode getTableConfigFromPinotEndpoint(String dataset) throws IOException {
    HttpGet request = new HttpGet(String.format(PINOT_TABLES_ENDPOINT_TEMPLATE, dataset));
    CloseableHttpResponse response = pinotControllerClient.execute(pinotControllerHost, request);
    LOG.debug("Retrieving dataset's custom config: {}", request);

    // Retrieve table config
    JsonNode tables = null;
    try {
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(response.getStatusLine().toString());
      }
      InputStream tablesContent = response.getEntity().getContent();
      tables = OBJECT_MAPPER.readTree(tablesContent);
    } catch (Exception e) {
      LOG.error("Exception in loading dataset {}", dataset, e);
    } finally {
      if (response.getEntity() != null) {
        EntityUtils.consume(response.getEntity());
      }
      response.close();
    }

    JsonNode tableJson = null;
    if (tables != null) {
      tableJson = tables.get("REALTIME");
      if (tableJson == null || tableJson.isNull()) {
        tableJson = tables.get("OFFLINE");
      }
    }
    return tableJson;
  }

  /**
   * Returns the map of custom configs of the given dataset from the Pinot table config json.
   */
  public Map<String, String> extractCustomConfigsFromPinotTable(JsonNode tableConfigJson) {

    Map<String, String> customConfigs = Collections.emptyMap();
    try {
      JsonNode jsonNode = tableConfigJson.get("metadata").get("customConfigs");
      customConfigs = OBJECT_MAPPER.convertValue(jsonNode, new TypeReference<Map<String, String>>() {
      });
    } catch (Exception e) {
      LOG.warn("Failed to get custom config from table: {}. Reason: {}", tableConfigJson, e);
    }
    return customConfigs;
  }

  public String extractTimeColumnFromPinotTable(JsonNode tableConfigJson) {
    JsonNode timeColumnNode = tableConfigJson.get("segmentsConfig").get("timeColumnName");
    return (timeColumnNode != null && !timeColumnNode.isNull()) ? timeColumnNode.asText() : null;
  }

  /**
   * This class accepts (i.e., ignores) all SSL certificate.
   */
  private static class AcceptAllTrustStrategy implements TrustStrategy {
    @Override
    public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      return true;
    }
  }
}
