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
package com.linkedin.pinot.hadoop.job;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.SimpleHttpResponse;
import com.linkedin.pinot.hadoop.utils.PushLocation;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRestApi.class);
  private final List<PushLocation> _pushLocations;
  private final String _tableName;

  private static final String OFFLINE = "OFFLINE";

  public ControllerRestApi(List<PushLocation> pushLocations, String tableName) {
    LOGGER.info("Push Locations are: " + pushLocations);
    _pushLocations = pushLocations;
    _tableName = tableName;
  }

  public TableConfig getTableConfig() {
    FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient();
    List<URI> tableConfigURIs = new ArrayList<>();
    try {
      for (PushLocation pushLocation : _pushLocations) {
        tableConfigURIs.add(FileUploadDownloadClient.getRetrieveTableConfigURI(pushLocation.getHost(), pushLocation.getPort(), _tableName));
      }
    } catch (URISyntaxException e) {
      LOGGER.error("Could not construct table config URI for table {}", _tableName);
      throw new RuntimeException(e);
    }

    // Return the first table config it can retrieve
    for (URI uri : tableConfigURIs) {
      try {
        SimpleHttpResponse response = fileUploadDownloadClient.getTableConfig(uri);
        JSONObject queryResponse = new JSONObject(response.getResponse());
        JSONObject offlineTableConfig = queryResponse.getJSONObject(OFFLINE);
        LOGGER.info("Got table config {}", offlineTableConfig);
        if (!queryResponse.isNull(OFFLINE)) {
          return TableConfig.fromJSONConfig(offlineTableConfig);
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while trying to get table config for " + _tableName + " " + e);
      }
    }
    LOGGER.error("Could not get table configs from any push locations provided for " + _tableName);
    throw new RuntimeException("Could not get table config for table " + _tableName);
  }

  public String getSchema() {
    FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient();
    List<URI> schemaURIs = new ArrayList<>();
    try {
      for (PushLocation pushLocation : _pushLocations) {
        schemaURIs.add(FileUploadDownloadClient.getRetrieveSchemaHttpURI(pushLocation.getHost(), pushLocation.getPort(), _tableName));
      }
    } catch (URISyntaxException e) {
      LOGGER.error("Could not construct schema URI for table {}", _tableName);
      throw new RuntimeException(e);
    }

    for (URI schemaURI : schemaURIs) {
      try {
        SimpleHttpResponse response = fileUploadDownloadClient.getSchema(schemaURI);
        if (response != null) {
          return response.getResponse();
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while trying to get schema for " + _tableName + " " + e);
      }
    }
    LOGGER.error("Could not get schema configs for any push locations provided for " + _tableName);
    throw new RuntimeException("Could not get schema for table " + _tableName);
  }
}
