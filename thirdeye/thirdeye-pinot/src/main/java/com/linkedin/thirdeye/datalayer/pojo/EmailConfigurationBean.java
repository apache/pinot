package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.Email;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class EmailConfigurationBean extends AbstractBean {

  List<Long> functionIds;

  @Valid
  @NotNull
  private String collection;

  @Valid
  @NotNull
  private String metric;

  @Valid
  @NotNull
  @Email
  private String fromAddress;

  @Valid
  @NotNull
  private String toAddresses;

  @Valid
  @NotNull
  private String cron;

  @Valid
  @NotNull
  private String smtpHost;

  @Valid
  private int smtpPort = 25;

  private String smtpUser;

  private String smtpPassword;

  @Valid
  @NotNull
  private int windowSize = 7;

  @Valid
  @NotNull
  private TimeUnit windowUnit = TimeUnit.DAYS;

  private boolean active;

  private boolean sendZeroAnomalyEmail;

  private String filters;

  private Integer windowDelay;

  private TimeUnit windowDelayUnit;

  public List<Long> getFunctionIds() {
    return functionIds;
  }

  public void setFunctionIds(List<Long> functionIds) {
    this.functionIds = functionIds;
  }

  public String getCollection() {
    return collection;
  }

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

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
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
        .add("windowSize", windowSize).add("windowUnit", windowUnit).add("isActive", active)
        .add("sendZeroAnomalyEmail", sendZeroAnomalyEmail).add("filters", filters)
        .add("windowDelay", windowDelay).add("windowDelayUnit", windowDelayUnit).toString();
  }
}
