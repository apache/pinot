package com.linkedin.thirdeye.alert.content;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;


public class EmailContentFormatterConfiguration extends ThirdEyeConfiguration{
  private String functionConfigPath;
  private String alertFilterConfigPath;

  public String getFunctionConfigPath() {
    return functionConfigPath;
  }

  public void setFunctionConfigPath(String functionConfigPath) {
    this.functionConfigPath = functionConfigPath;
  }

  @Override
  public String getAlertFilterConfigPath() {
    return alertFilterConfigPath;
  }

  public void setAlertFilterConfigPath(String alertFilterConfigPath) {
    this.alertFilterConfigPath = alertFilterConfigPath;
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
