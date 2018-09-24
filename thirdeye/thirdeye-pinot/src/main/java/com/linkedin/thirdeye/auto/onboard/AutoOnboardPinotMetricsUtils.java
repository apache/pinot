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

package com.linkedin.thirdeye.auto.onboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSourceConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

  public boolean verifySchemaCorrectness(Schema schema) {
    boolean isSchemaCorrect = true;
    if (StringUtils.isBlank(schema.getSchemaName()) || schema.getTimeFieldSpec() == null
        || schema.getTimeFieldSpec().getOutgoingGranularitySpec() == null) {
      isSchemaCorrect = false;
    }
    return isSchemaCorrect;
  }

  /**
   * Returns the map of custom configs of the given dataset from the table config on Pinot.
   *
   * @param dataset the target dataset
   *
   * @return the field, which is a Map of string to string, of custom config on Pinot's table config
   *
   * @throws IOException if this method fails to connect to Pinot endpoint.
   */
  public Map<String, String> getCustomConfigsFromPinotEndpoint(String dataset) throws IOException {
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

    // Parse custom config
    Map<String, String> customConfigs = Collections.emptyMap();
    if (tables != null) {
      JsonNode table = tables.get("REALTIME");
      if (table == null || table.isNull()) {
        table = tables.get("OFFLINE");
      }
      if (table != null && !table.isNull()) {
        try {
          JsonNode jsonNode = table.get("metadata").get("customConfigs");
          customConfigs = OBJECT_MAPPER.convertValue(jsonNode, HashMap.class);
        } catch (Exception e) {
          LOG.warn("Failed to get custom config for dataset: {}. Reason: {}", dataset, e);
        }
      } else {
        LOG.debug("Dataset {} doesn't exists in Pinot.", dataset);
      }
    }
    return customConfigs;
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
