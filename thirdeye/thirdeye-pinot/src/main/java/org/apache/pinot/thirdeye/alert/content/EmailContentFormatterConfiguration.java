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

package org.apache.pinot.thirdeye.alert.content;

import org.apache.pinot.thirdeye.anomaly.SmtpConfiguration;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import java.util.List;

import static org.apache.pinot.thirdeye.anomaly.SmtpConfiguration.SMTP_CONFIG_KEY;


public class EmailContentFormatterConfiguration{
  private String functionConfigPath;
  private String alertFilterConfigPath;

  private SmtpConfiguration smtpConfiguration;
  private String rootDir = "";
  private String dashboardHost;
  private String phantomJsPath = "";
  private String failureFromAddress;
  private String failureToAddress;
  private List<String> holidayCountriesWhitelist;

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

  public List<String> getHolidayCountriesWhitelist() {
    return holidayCountriesWhitelist;
  }

  public void setHolidayCountriesWhitelist(List<String> holidayCountriesWhitelist) {
    this.holidayCountriesWhitelist = holidayCountriesWhitelist;
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
    emailConfig.setHolidayCountriesWhitelist(thirdeyeConfig.getHolidayCountriesWhitelist());
    emailConfig.setSmtpConfiguration(
        SmtpConfiguration.createFromProperties(thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)));

    return emailConfig;
  }
}
