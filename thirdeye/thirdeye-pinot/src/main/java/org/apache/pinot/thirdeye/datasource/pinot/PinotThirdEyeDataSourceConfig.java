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

package org.apache.pinot.thirdeye.datasource.pinot;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardPinotMetadataSource;
import org.apache.pinot.thirdeye.datasource.MetadataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable configurations for setting up {@link PinotThirdEyeDataSource}'s connection to Pinot.
 */
public class PinotThirdEyeDataSourceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeDataSourceConfig.class);

  public static final String HTTP_SCHEME = "http";
  public static final String HTTPS_SCHEME = "https";

  private String zookeeperUrl;
  private String controllerHost;
  private int controllerPort;
  private String controllerConnectionScheme;
  private String clusterName;
  private String brokerUrl;
  private String tag;
  private String name;

  public String getZookeeperUrl() {
    return zookeeperUrl;
  }

  private void setZookeeperUrl(String zookeeperUrl) {
    this.zookeeperUrl = zookeeperUrl;
  }

  public String getControllerHost() {
    return controllerHost;
  }

  private void setControllerHost(String controllerHost) {
    this.controllerHost = controllerHost;
  }

  public int getControllerPort() {
    return controllerPort;
  }

  private void setControllerPort(int controllerPort) {
    this.controllerPort = controllerPort;
  }

  public String getTag() {
    return tag;
  }

  private void setTag(String tag) {
    this.tag = tag;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getBrokerUrl() {
    return brokerUrl;
  }

  private void setBrokerUrl(String brokerUrl) {
    this.brokerUrl = brokerUrl;
  }

  public String getClusterName() {
    return clusterName;
  }

  private void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getControllerConnectionScheme() {
    return controllerConnectionScheme;
  }

  private void setControllerConnectionScheme(String controllerConnectionScheme) {
    this.controllerConnectionScheme = controllerConnectionScheme;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PinotThirdEyeDataSourceConfig config = (PinotThirdEyeDataSourceConfig) o;
    return getControllerPort() == config.getControllerPort() && Objects
        .equals(getZookeeperUrl(), config.getZookeeperUrl()) && Objects
        .equals(getControllerHost(), config.getControllerHost()) && Objects
        .equals(getControllerConnectionScheme(), config.getControllerConnectionScheme()) && Objects
        .equals(getClusterName(), config.getClusterName()) && Objects.equals(getBrokerUrl(), config.getBrokerUrl())
        && Objects.equals(getTag(), config.getTag());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getZookeeperUrl(), getControllerHost(), getControllerPort(), getControllerConnectionScheme(),
        getClusterName(), getBrokerUrl(), getTag(), getName());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("zookeeperUrl", zookeeperUrl).add("controllerHost", controllerHost)
        .add("controllerPort", controllerPort).add("controllerConnectionScheme", controllerConnectionScheme)
        .add("clusterName", clusterName).add("brokerUrl", brokerUrl).add("tag", tag).add("name", name).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String zookeeperUrl;
    private String controllerHost;
    private int controllerPort = -1;
    private String controllerConnectionScheme = HTTP_SCHEME; // HTTP_SCHEME or HTTPS_SCHEME
    private String clusterName;
    private String brokerUrl;
    private String tag;
    private String name;

    public Builder setZookeeperUrl(String zookeeperUrl) {
      this.zookeeperUrl = zookeeperUrl;
      return this;
    }

    public Builder setControllerHost(String controllerHost) {
      this.controllerHost = controllerHost;
      return this;
    }

    public Builder setControllerPort(int controllerPort) {
      this.controllerPort = controllerPort;
      return this;
    }

    public Builder setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder setBrokerUrl(String brokerUrl) {
      this.brokerUrl = brokerUrl;
      return this;
    }

    public Builder setTag(String tag) {
      this.tag = tag;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setControllerConnectionScheme(String controllerConnectionScheme) {
      this.controllerConnectionScheme = controllerConnectionScheme;
      return this;
    }

    public PinotThirdEyeDataSourceConfig build() {
      final String className = PinotThirdEyeDataSourceConfig.class.getSimpleName();
      Preconditions.checkNotNull(controllerHost, "{} is missing 'Controller Host' property", className);
      Preconditions.checkArgument(controllerPort >= 0, "{} is missing 'Controller Port' property", className);
      Preconditions.checkNotNull(zookeeperUrl, "{} is missing 'Zookeeper URL' property", className);
      Preconditions.checkNotNull(clusterName, "{} is missing 'Cluster Name' property", className);
      Preconditions.checkArgument(
          controllerConnectionScheme.equals(HTTP_SCHEME) || controllerConnectionScheme.equals(HTTPS_SCHEME),
          "{} accepts only 'http' or 'https' connection schemes", className);

      PinotThirdEyeDataSourceConfig config = new PinotThirdEyeDataSourceConfig();
      config.setControllerHost(controllerHost);
      config.setControllerPort(controllerPort);
      config.setZookeeperUrl(zookeeperUrl);
      config.setClusterName(clusterName);
      config.setBrokerUrl(brokerUrl);
      config.setTag(tag);
      config.setControllerConnectionScheme(controllerConnectionScheme);
      config.setName(name);
      return config;
    }
  }

  /**
   * Returns pinot thirdeye datasource config given metadatasource config. There can be only ONE datasource of pinot type
   *
   * @param metadataSourceConfig
   *
   * @return
   */
  public static PinotThirdEyeDataSourceConfig createFromMetadataSourceConfig(MetadataSourceConfig metadataSourceConfig) {
    if (metadataSourceConfig == null || !metadataSourceConfig.getClassName()
        .equals(AutoOnboardPinotMetadataSource.class.getCanonicalName())) {
      throw new IllegalStateException("Metadata source config is not of type pinot " + metadataSourceConfig);
    }
    return createFromProperties(metadataSourceConfig.getProperties());
  }

  /**
   * Returns PinotThirdEyeDataSourceConfig from the given property map.
   *
   * @param properties the properties to setup a PinotThirdEyeDataSourceConfig.
   *
   * @return a PinotThirdEyeDataSourceConfig.
   *
   * @throws IllegalArgumentException is thrown if the property map does not contain all necessary fields, i.e.,
   *                                  controller host and port, cluster name, and the URL to zoo keeper.
   */
  static PinotThirdEyeDataSourceConfig createFromProperties(Map<String, Object> properties) {
    String dataSourceName = MapUtils.getString(properties, PinotThirdeyeDataSourceProperties.NAME.getValue(), PinotThirdEyeDataSource.class.getSimpleName());

    ImmutableMap<String, Object> processedProperties = processPropertyMap(properties);
    Preconditions.checkNotNull(processedProperties, "Invalid properties for data source: %s, properties=%s", dataSourceName, properties);

    String controllerHost = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue());
    int controllerPort = MapUtils.getInteger(processedProperties, PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue());
    String controllerConnectionScheme = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.CONTROLLER_CONNECTION_SCHEME.getValue());
    String zookeeperUrl = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue());
    String clusterName = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue());

    // brokerUrl, tag, and name are optional
    String brokerUrl = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.BROKER_URL.getValue());
    String tag = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.TAG.getValue());
    String name = MapUtils.getString(processedProperties, PinotThirdeyeDataSourceProperties.NAME.getValue());

    Builder builder =
        PinotThirdEyeDataSourceConfig.builder().setControllerHost(controllerHost).setControllerPort(controllerPort)
            .setZookeeperUrl(zookeeperUrl).setClusterName(clusterName);
    if (StringUtils.isNotBlank(brokerUrl)) {
      builder.setBrokerUrl(brokerUrl);
    }
    if (StringUtils.isNotBlank(tag)) {
      builder.setTag(tag);
    }
    if (StringUtils.isNotBlank(name)) {
      builder.setName(name);
    }
    if (StringUtils.isNotBlank(controllerConnectionScheme)) {
      builder.setControllerConnectionScheme(controllerConnectionScheme);
    }

    return builder.build();
  }

  /**
   * Process the input properties and Checks if the given property map could be used to construct a
   * PinotThirdEyeDataSourceConfig. The essential fields are controller host and port, cluster name, and the URL to zoo
   * keeper. This method prints out all missing essential fields before returning a null processed map.
   *
   * @param properties the input properties to be checked.
   *
   * @return a processed property map; null if the given property map cannot be validated successfully.
   */
  static ImmutableMap<String, Object> processPropertyMap(Map<String, Object> properties) {
    if (MapUtils.isEmpty(properties)) {
      LOG.error("PinotThirdEyeDataSource is missing properties {}", properties);
      return null;
    }

    final List<PinotThirdeyeDataSourceProperties> requiredProperties = Arrays
        .asList(PinotThirdeyeDataSourceProperties.CONTROLLER_HOST, PinotThirdeyeDataSourceProperties.CONTROLLER_PORT,
            PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL, PinotThirdeyeDataSourceProperties.CLUSTER_NAME);
    final List<PinotThirdeyeDataSourceProperties> optionalProperties = Arrays
        .asList(PinotThirdeyeDataSourceProperties.CONTROLLER_CONNECTION_SCHEME,
            PinotThirdeyeDataSourceProperties.BROKER_URL, PinotThirdeyeDataSourceProperties.TAG);

    // Validates required properties
    final String className = PinotControllerResponseCacheLoader.class.getSimpleName();
    boolean valid = true;
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (PinotThirdeyeDataSourceProperties requiredProperty : requiredProperties) {
      String propertyString = nullToEmpty(properties.get(requiredProperty.getValue())).trim();
      if (Strings.isNullOrEmpty(propertyString)) {
        valid = false;
        LOG.error("{} is missing required property {}", className, requiredProperty);
      } else {
        builder.put(requiredProperty.getValue(), propertyString);
      }
    }

    if (valid) {
      // Copies optional properties
      for (PinotThirdeyeDataSourceProperties optionalProperty : optionalProperties) {
        String propertyString = nullToEmpty(properties.get(optionalProperty.getValue())).trim();
        if (!Strings.isNullOrEmpty(propertyString)) {
          builder.put(optionalProperty.getValue(), propertyString);
        }
      }

      return builder.build();
    } else {
      return null;
    }
  }

  private static String nullToEmpty(Object value) {
    if (value == null) {
      return "";
    }
    return value.toString();
  }
}
