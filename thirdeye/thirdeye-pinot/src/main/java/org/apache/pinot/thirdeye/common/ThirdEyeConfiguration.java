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

package org.apache.pinot.thirdeye.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import io.dropwizard.Configuration;
import java.util.Map;
import org.apache.pinot.thirdeye.model.download.ModelDownloaderConfiguration;


public class ThirdEyeConfiguration extends Configuration {
  /**
   * Root directory for all other configuration
   */
  private String rootDir = "";
  private String dataSources = "data-sources/data-sources-config.yml";
  private String cacheDataSource = "data-sources/cache-config.yml";

  private String dashboardHost;

  @JsonProperty("swagger")
  public SwaggerBundleConfiguration swaggerBundleConfiguration;

  private Map<String, Map<String, Object>> alerterConfigurations;

  private String phantomJsPath = "";
  private String failureFromAddress;
  private String failureToAddress;

  private List<ModelDownloaderConfiguration> modelDownloaderConfig;

  /**
   * allow cross request for local development
   */
  private boolean cors = false;

  /**
   * Convert relative path to absolute URL
   *
   * Supported cases:
   * <pre>
   *   file:/....myDir/data-sources-config.yml
   *   myDir/data-sources-config.yml
   * </pre>
   *
   * @return the url of the data source
   */
  public URL getDataSourcesAsUrl() {
    return getSourceAsUrl(this.dataSources);
  }

  private URL getSourceAsUrl(String path) {
    try {
      return new URL(path);
    } catch (MalformedURLException ignore) {
      // ignore
    }

    try {
      URL rootUrl = new URL(String.format("file:%s/", this.rootDir));
      return new URL(rootUrl, path);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(String.format("Could not parse relative path for rootDir '%s' and dataSources/cacheConfig '%s'", this.rootDir, path));
    }
  }

  public String getDataSources() {
    return dataSources;
  }

  public void setDataSources(String dataSources) {
    this.dataSources = dataSources;
  }

  public URL getCacheConfigAsUrl() {
    return getSourceAsUrl(this.cacheDataSource);
  }

  public void setCacheDataSource(String cacheDataSource) { this.cacheDataSource = cacheDataSource; }

  public String getRootDir() {
    return rootDir;
  }

  public void setRootDir(String rootDir) {
    this.rootDir = rootDir;
  }

  public boolean isCors() {
    return cors;
  }

  public void setCors(boolean cors) {
    this.cors = cors;
  }

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }

  //alertFilter.properties format: {alert filter type} = {path to alert filter implementation}
  public String getAlertFilterConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilter.properties";
  }

  public String getCalendarApiKeyPath(){
    return getRootDir() + "/holiday-loader-key.json";
  }

  public String getPhantomJsPath() {
    return phantomJsPath;
  }

  public void setPhantomJsPath(String phantomJsPath) {
    this.phantomJsPath = phantomJsPath;
  }

  public String getDashboardHost() {
    return dashboardHost;
  }

  public void setDashboardHost(String dashboardHost) {
    this.dashboardHost = dashboardHost;
  }

  public String getFailureFromAddress() {
    return failureFromAddress;
  }

  public void setFailureFromAddress(String failureFromAddress) {
    this.failureFromAddress = failureFromAddress;
  }

  public String getFailureToAddress() {
    return failureToAddress;
  }

  public void setFailureToAddress(String failureToAddress) {
    this.failureToAddress = failureToAddress;
  }

  public Map<String, Map<String, Object>> getAlerterConfiguration() {
    return alerterConfigurations;
  }

  public void setAlerterConfiguration(Map<String, Map<String, Object>> alerterConfigurations) {
    this.alerterConfigurations = alerterConfigurations;
  }

  public List<ModelDownloaderConfiguration> getModelDownloaderConfig() {
    return modelDownloaderConfig;
  }

  public void setModelDownloaderConfig(List<ModelDownloaderConfiguration> modelDownloaderConfig) {
    this.modelDownloaderConfig = modelDownloaderConfig;
  }
}
