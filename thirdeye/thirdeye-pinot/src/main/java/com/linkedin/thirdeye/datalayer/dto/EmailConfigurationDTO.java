package com.linkedin.thirdeye.datalayer.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EmailConfigurationDTO extends AbstractDTO {

  private String collection;

  private String metric;

  private String fromAddress;

  private String toAddresses;

  private String cron;

  private String smtpHost;

  private int smtpPort = 25;

  private String smtpUser;

  private String smtpPassword;

  private int windowSize = 7;

  private TimeUnit windowUnit = TimeUnit.DAYS;

  private boolean isActive;

  private boolean sendZeroAnomalyEmail;

  private String filters;

  private Integer windowDelay;

  private TimeUnit windowDelayUnit;

  public String getCollection() {
    return collection;
  }

  private List<AnomalyFunctionDTO> functions = new ArrayList<>();

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public String getToAddresses() {
    return toAddresses;
  }

  public void setToAddresses(String toAddresses) {
    this.toAddresses = toAddresses;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public String getSmtpHost() {
    return smtpHost;
  }

  public void setSmtpHost(String smtpHost) {
    this.smtpHost = smtpHost;
  }

  public int getSmtpPort() {
    return smtpPort;
  }

  public void setSmtpPort(int smtpPort) {
    this.smtpPort = smtpPort;
  }

  public String getSmtpUser() {
    return smtpUser;
  }

  public void setSmtpUser(String smtpUser) {
    this.smtpUser = smtpUser;
  }

  public String getSmtpPassword() {
    return smtpPassword;
  }

  public void setSmtpPassword(String smtpPassword) {
    this.smtpPassword = smtpPassword;
  }

  public int getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(int windowSize) {
    this.windowSize = windowSize;
  }

  public TimeUnit getWindowUnit() {
    return windowUnit;
  }

  public void setWindowUnit(TimeUnit windowUnit) {
    this.windowUnit = windowUnit;
  }

  public boolean getIsActive() {
    return isActive;
  }

  public void setIsActive(boolean isActive) {
    this.isActive = isActive;
  }

  public boolean getSendZeroAnomalyEmail() {
    return sendZeroAnomalyEmail;
  }

  public void setSendZeroAnomalyEmail(boolean sendZeroAnomalyEmail) {
    this.sendZeroAnomalyEmail = sendZeroAnomalyEmail;
  }

  public String getFilters() {
    return filters;
  }

  @JsonIgnore
  public Multimap<String, String> getFilterSet() {
    return ThirdEyeUtils.getFilterSet(filters);
  }

  public void setFilters(String filters) {
    String sortedFilters = ThirdEyeUtils.getSortedFilters(filters);
    this.filters = sortedFilters;
  }

  public Integer getWindowDelay() {
    return windowDelay;
  }

  public void setWindowDelay(Integer windowDelay) {
    this.windowDelay = windowDelay;
  }

  public TimeUnit getWindowDelayUnit() {
    return windowDelayUnit;
  }

  public void setWindowDelayUnit(TimeUnit windowDelayUnit) {
    this.windowDelayUnit = windowDelayUnit;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("collection", collection).add("metric", metric)
        .add("fromAddress", fromAddress).add("toAddresses", toAddresses).add("cron", cron)
        .add("smtpHost", smtpHost).add("smtpPort", smtpPort).add("smtpUser", smtpUser)
        .add("windowSize", windowSize).add("windowUnit", windowUnit).add("isActive", isActive)
        .add("sendZeroAnomalyEmail", sendZeroAnomalyEmail).add("filters", filters)
        .add("windowDelay", windowDelay).add("windowDelayUnit", windowDelayUnit)
        .add("functions", functions).toString();
  }
}
