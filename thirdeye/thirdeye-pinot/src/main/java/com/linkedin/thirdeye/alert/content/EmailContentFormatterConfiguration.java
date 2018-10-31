package com.linkedin.thirdeye.alert.content;

import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;


public class EmailContentFormatterConfiguration{
  private String functionConfigPath;
  private String alertFilterConfigPath;

  private SmtpConfiguration smtpConfiguration;
  private String rootDir = "";
  private String dashboardHost;
  private String phantomJsPath = "";
  private String failureFromAddress;
  private String failureToAddress;

  public String getFunctionConfigPath() {
    return functionConfigPath;
  }

  public void setFunctionConfigPath(String functionConfigPath) {
    this.functionConfigPath = functionConfigPath;
  }

  public String getAlertFilterConfigPath() {
    return alertFilterConfigPath;
  }

  public void setAlertFilterConfigPath(String alertFilterConfigPath) {
    this.alertFilterConfigPath = alertFilterConfigPath;
  }

  public SmtpConfiguration getSmtpConfiguration() {
    return smtpConfiguration;
  }

  public void setSmtpConfiguration(SmtpConfiguration smtpConfiguration) {
    this.smtpConfiguration = smtpConfiguration;
  }

  public String getRootDir() {
    return rootDir;
  }

  public void setRootDir(String rootDir) {
    this.rootDir = rootDir;
  }

  public String getDashboardHost() {
    return dashboardHost;
  }

  public void setDashboardHost(String dashboardHost) {
    this.dashboardHost = dashboardHost;
  }

  public String getPhantomJsPath() {
    return phantomJsPath;
  }

  public void setPhantomJsPath(String phantomJsPath) {
    this.phantomJsPath = phantomJsPath;
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

  public static EmailContentFormatterConfiguration fromThirdEyeAnomalyConfiguration(ThirdEyeAnomalyConfiguration thirdeyeConfig) {
    EmailContentFormatterConfiguration emailConfig = new EmailContentFormatterConfiguration();
    emailConfig.setDashboardHost(thirdeyeConfig.getDashboardHost());
    emailConfig.setRootDir(thirdeyeConfig.getRootDir());
    emailConfig.setFailureFromAddress(thirdeyeConfig.getFailureFromAddress());
    emailConfig.setFailureToAddress(thirdeyeConfig.getFailureToAddress());
    emailConfig.setFunctionConfigPath(thirdeyeConfig.getFunctionConfigPath());
    emailConfig.setAlertFilterConfigPath(thirdeyeConfig.getAlertFilterConfigPath());
    emailConfig.setPhantomJsPath(thirdeyeConfig.getPhantomJsPath());
    emailConfig.setSmtpConfiguration(thirdeyeConfig.getSmtpConfiguration());

    return emailConfig;
  }
}
