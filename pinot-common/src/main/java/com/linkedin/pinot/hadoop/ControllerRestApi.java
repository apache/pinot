/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.hadoop;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.hadoop.job.PushLocationTranslator;
import java.io.InputStream;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerRestApi {
  private static final Logger _logger = LoggerFactory.getLogger(ControllerRestApi.class);
  public static final String HDR_CONTROLLER_HOST = "Pinot-Controller-Host";
  public static final String HDR_CONTROLLER_VERSION = "Pinot-Controller-Version";
  public static final String NOT_IN_RESPONSE = "NotInResponse";

  private PushLocationTranslator _pushLocationTranslator;
  private final String _pushLocations;

  public ControllerRestApi(String pushLocations) {
    _logger.info("Push Locations are: " + pushLocations);
    _pushLocations = pushLocations;
    _pushLocationTranslator = new PushLocationTranslator();
  }

  public JSONObject sendGetRequest(String urlString) throws Exception {
    _logger.info("Sending get request: " + urlString);

    int timeoutMs = 3000;
    // TODO: Remove to correct API when we bump versions
    final BasicHttpParams httpParams = new BasicHttpParams();
    httpParams.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, timeoutMs);
    HttpClient httpClient = new DefaultHttpClient(httpParams);

    try {
      HttpGet get = new HttpGet(urlString);
      // execute the request
      HttpResponse getRequestHeader = httpClient.execute(get);
      _logger.info("getRequestHandler returned for " + urlString + " is " + getRequestHeader);

      ResponseHandler<String> handler = new BasicResponseHandler();
      String response = handler.handleResponse(getRequestHeader);
      _logger.info("Response returned is for " + urlString + " is " + response);

      // retrieve and process the response
      int responseCode = getRequestHeader.getStatusLine().getStatusCode();
      String host = NOT_IN_RESPONSE;
      if (getRequestHeader.containsHeader(HDR_CONTROLLER_HOST)) {
        host = getRequestHeader.getFirstHeader(HDR_CONTROLLER_HOST).getValue();
      }
      String version = NOT_IN_RESPONSE;
      if (getRequestHeader.containsHeader(HDR_CONTROLLER_VERSION)) {
        version = getRequestHeader.getFirstHeader(HDR_CONTROLLER_VERSION).getValue();
      }
      _logger.info("Controller host:" + host + ",Controller version:" + version);

      if (responseCode >= 400) {
        String errorString = "Response Code: " + responseCode;
        _logger.error("Error trying to send request " + urlString);
        InputStream content = getRequestHeader.getEntity().getContent();
        String respBody = org.apache.commons.io.IOUtils.toString(content);
        if (respBody != null && !respBody.isEmpty()) {
          _logger.error("Response body for url " + urlString + ":" + respBody);
        }
        throw new RuntimeException(errorString);
      } else {
        return new JSONObject(response);
      }
    } catch (Exception e) {
      _logger.error("Exception trying to send request " + urlString, e);
      throw new RuntimeException(e);
    }
  }

  public TableConfig getTableConfig(String tableName) throws Exception {

    List<String> tableConfigUris = _pushLocationTranslator.getTableConfigUris(_pushLocations, tableName);
    for (String tableConfigUri : tableConfigUris) {
      try {
        JSONObject queryResponse = sendGetRequest(tableConfigUri);
        if (!queryResponse.isNull("OFFLINE")) {
          String offlineTableConfig = queryResponse.getJSONObject("OFFLINE").toString();
          return TableConfig.init(offlineTableConfig);
        }
      } catch (Exception e) {
        _logger.warn("Caught exception while trying to get table config for " + tableName + " " + e);
      }
    }
    _logger.error("Could not get table configs from any push locations provided for " + tableName);
    throw new RuntimeException("Could not get table config for table " + tableName);
  }

  public String getSchema(String tableName) throws Exception {

    List<String> schemaUris = _pushLocationTranslator.getSchemaUrls(_pushLocations, tableName);
    for (String schemaUri : schemaUris) {
      try {
        JSONObject queryResponse = sendGetRequest(schemaUri);
        if (queryResponse != null) {
          return queryResponse.toString();
        }
      } catch (Exception e) {
        _logger.warn("Caught exception while trying to get schema for " + tableName + " " + e);
      }
    }
    _logger.error("Could not get schema configs for any push locations provided for " + tableName);
    throw new RuntimeException("Could not get schema for table " + tableName);
  }
}
