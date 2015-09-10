package com.linkedin.thirdeye.dashboard;

import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.dashboard.resources.CustomDashboardResource;

public class ThirdEyeDashboardConfiguration extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(CustomDashboardResource.class);

  @NotNull
  private String serverUri; // TODO: Support talking to multiple servers

  private String feedbackEmailAddress;

  private String customDashboardRoot; // directory where all {dashboard}.yml files are saved

  private String funnelConfigRoot;

  private String collectionConfigRoot; // directory where all collection configs are defined

  private AnomalyDatabaseConfig anomalyDatabaseConfig; // information for the anomaly database

  @Valid
  @NotNull
  @JsonProperty
  private HttpClientConfiguration httpClient = new HttpClientConfiguration();

  public HttpClientConfiguration getHttpClient() {
    return httpClient;
  }

  public String getServerUri() {
    return serverUri;
  }

  public String getFeedbackEmailAddress() {
    return feedbackEmailAddress;
  }

  public void setServerUri(String serverUri) {
    this.serverUri = serverUri;
  }

  public String getCustomDashboardRoot() {
    return customDashboardRoot;
  }

  public void setCustomDashboardRoot(String customDashboardRoot) {
    this.customDashboardRoot = customDashboardRoot;
  }

  public String getCollectionConfigRoot() {
    return collectionConfigRoot;
  }

  public void setCollectionConfigRoot(String collectionConfigRoot) {
    this.collectionConfigRoot = collectionConfigRoot;
  }

  public AnomalyDatabaseConfig getAnomalyDatabaseConfig() {
    return anomalyDatabaseConfig;
  }

  public void setAnomalyDatabaseConfig(AnomalyDatabaseConfig anomalyDatabaseConfig) {
    this.anomalyDatabaseConfig = anomalyDatabaseConfig;
  }

  public String getFunnelConfigRoot() {
    return funnelConfigRoot;
  }

  public void setFunnelConfigRoot(String funnelConfigRoot) {
    this.funnelConfigRoot = funnelConfigRoot;
  }

  public String toString() {
    String ret = null;
    try {
      ret = new ObjectMapper().writeValueAsString(this);
    } catch (Exception e) {
      LOG.error("error reading configs", e);
    }

    return ret;
  }

}
