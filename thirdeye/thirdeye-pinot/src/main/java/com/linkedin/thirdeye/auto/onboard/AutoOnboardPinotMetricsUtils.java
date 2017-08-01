package com.linkedin.thirdeye.auto.onboard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSourceConfig;

public class AutoOnboardPinotMetricsUtils {
  private static final String PINOT_TABLES_ENDPOINT = "/tables/";
  private static final String PINOT_SCHEMA_ENDPOINT = "/schemas/%s";
  private static final String PINOT_SCHEMA_ENDPOINT_TEMPLATE = "/tables/%s/schema";
  private static final String UTF_8 = "UTF-8";

  private CloseableHttpClient pinotControllerClient;
  private HttpHost pinotControllerHost;

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardPinotMetricsUtils.class);

  public AutoOnboardPinotMetricsUtils(DataSourceConfig dataSourceConfig) {
      PinotThirdEyeDataSourceConfig pinotThirdeyeDataSourceConfig = PinotThirdEyeDataSourceConfig.createPinotThirdeyeDataSourceConfig(dataSourceConfig);
      this.pinotControllerClient = HttpClients.createDefault();
      this.pinotControllerHost = new HttpHost(pinotThirdeyeDataSourceConfig.getControllerHost(),
          pinotThirdeyeDataSourceConfig.getControllerPort());
  }
  public AutoOnboardPinotMetricsUtils(String host, int port) {
    this.pinotControllerClient = HttpClients.createDefault();
    this.pinotControllerHost = new HttpHost(host, port);
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
      tables = new ObjectMapper().readTree(tablesContent).get("tables");
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

  public String getAllTablesFromPinotURLStyle() throws IOException {
     InputStream is = new URL("http://ssubrama-ld1.linkedin.biz:21000/tables").openStream();

     try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
       StringBuilder responseBuilder = new StringBuilder();
       String line;
       while ((line = reader.readLine()) != null) {
         responseBuilder.append(line);
       }
       return responseBuilder.toString();
     }


  }

  /**
   * Fetches schema from pinot, from the tables endpoint or schema endpoint
   * @param dataset
   * @return
   * @throws IOException
   */
  public Schema getSchemaFromPinot(String dataset) throws IOException {
    Schema schema = null;
    schema = getSchemaFromTableConfig(dataset);
    if (schema == null) {
      schema = getSchemaFromSchemaEndpoint(dataset);
    }
    if (schema == null) {
      schema = getSchemaFromSchemaEndpoint(dataset + "_OFFLINE");
    }
    return schema;
  }

  public Schema getSchemaFromTableConfig(String dataset) throws IOException {
    Schema schema = null;
    HttpGet schemaReq = new HttpGet(String.format(PINOT_SCHEMA_ENDPOINT_TEMPLATE, URLEncoder.encode(dataset, UTF_8)));
    LOG.info("Retrieving schema: {}", schemaReq);
    CloseableHttpResponse schemaRes = pinotControllerClient.execute(pinotControllerHost, schemaReq);
    try {
      if (schemaRes.getStatusLine().getStatusCode() != 200) {
        LOG.error("Schema {} not found, {}", dataset, schemaRes.getStatusLine().toString());
      } else {
        InputStream schemaContent = schemaRes.getEntity().getContent();
        schema = new org.codehaus.jackson.map.ObjectMapper().readValue(schemaContent, Schema.class);
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

  public Schema getSchemaFromSchemaEndpoint(String dataset) throws IOException {
    Schema schema = null;
    HttpGet schemaReq = new HttpGet(String.format(PINOT_SCHEMA_ENDPOINT, URLEncoder.encode(dataset, UTF_8)));
    LOG.info("Retrieving schema: {}", schemaReq);
    CloseableHttpResponse schemaRes = pinotControllerClient.execute(pinotControllerHost, schemaReq);
    try {
      if (schemaRes.getStatusLine().getStatusCode() != 200) {
        LOG.error("Schema {} not found, {}", dataset, schemaRes.getStatusLine().toString());
      } else {
        InputStream schemaContent = schemaRes.getEntity().getContent();
        schema = new org.codehaus.jackson.map.ObjectMapper().readValue(schemaContent, Schema.class);
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

  public static void main(String[] args) throws IOException {
    String controllerHost = "lva1-pinot-controller-vip-1.corp.linkedin.com";
    //int controllerPort = 8100;
    int controllerPort = 11984;
    String tableName = "thirdeyeKbmi";
    String segmentName = "";
    AutoOnboardPinotMetricsUtils at = new AutoOnboardPinotMetricsUtils(controllerHost, controllerPort);

    //String allTablesFromPinotURL = at.getAllTablesFromPinotURLStyle();
    //System.out.println(allTablesFromPinotURL);

    JsonNode allTablesFromPinot = at.getAllTablesFromPinot();
    System.out.println(allTablesFromPinot);


    Schema schemaFromTableConfig = at.getSchemaFromTableConfig(tableName);
    System.out.println(schemaFromTableConfig);

    Schema schemaFromSchemaEndpoint = at.getSchemaFromSchemaEndpoint(tableName);
    System.out.println(schemaFromSchemaEndpoint);

  }
}