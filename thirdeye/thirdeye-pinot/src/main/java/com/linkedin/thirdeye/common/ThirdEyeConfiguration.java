package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import java.util.ArrayList;
import java.util.List;

import io.dropwizard.Configuration;

public abstract class ThirdEyeConfiguration extends Configuration {
  /**
   * Root directory for all other configuration
   */
  private String rootDir = "";

  private List<String> whitelistDatasets = new ArrayList<>();

  private String dashboardHost;
  private SmtpConfiguration smtpConfiguration;

  private String phantomJsPath = "";
  private String failureFromAddress;
  private String failureToAddress;

  /**
   * allow cross request for local development
   */
  private boolean cors = false;

  public String getDataSourcesPath() {
    return getRootDir() + "/data-sources/data-sources-config.yml";
  }

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

  public List<String> getWhitelistDatasets() {
    return whitelistDatasets;
  }

  public void setWhitelistDatasets(List<String> whitelistDatasets) {
    this.whitelistDatasets = whitelistDatasets;
  }

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }

  //alertFilter.properties format: {alert filter type} = {path to alert filter implementation}
  public String getAlertFilterConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilter.properties";
  }

  //alertFilterAutotune.properties format: {auto tune type} = {path to auto tune implementation}
  public String getFilterAutotuneConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilterAutotune.properties";
  }

  public String getAlertGroupRecipientProviderConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertGroupRecipientProvider.properties";
  }

  public String getAnomalyClassifierConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/anomalyClassifier.properties";
  }

  public void setSmtpConfiguration(SmtpConfiguration smtpConfiguration) {
    this.smtpConfiguration = smtpConfiguration;
  }

  public SmtpConfiguration getSmtpConfiguration(){
    return this.smtpConfiguration;
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

}
