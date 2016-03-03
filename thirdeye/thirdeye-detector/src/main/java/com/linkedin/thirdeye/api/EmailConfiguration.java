package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import org.hibernate.validator.constraints.Email;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

@Entity
@Table(name = "email_configurations")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.api.EmailConfiguration#findAll", query = "SELECT c FROM EmailConfiguration c")
})
public class EmailConfiguration {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Valid
  @NotNull
  @Column(name = "collection", nullable = false)
  private String collection;

  @Valid
  @NotNull
  @Column(name = "metric", nullable = false)
  private String metric;

  @Valid
  @NotNull
  @Email
  @Column(name = "from_address", nullable = false)
  private String fromAddress;

  @Valid
  @NotNull
  @Column(name = "to_addresses", nullable = false)
  private String toAddresses;

  @Valid
  @NotNull
  @Column(name = "cron", nullable = false)
  private String cron;

  @Valid
  @NotNull
  @Column(name = "smtp_host", nullable = false)
  private String smtpHost;

  @Valid
  @Column(name = "smtp_port", nullable = false)
  private int smtpPort = 25;

  @Column(name = "smtp_user", nullable = true)
  private String smtpUser;

  @Column(name = "smtp_password", nullable = true)
  private String smtpPassword;

  @Valid
  @NotNull
  @Column(name = "window_size", nullable = false)
  private int windowSize = 7;

  @Valid
  @NotNull
  @Column(name = "window_unit", nullable = false)
  private TimeUnit windowUnit = TimeUnit.DAYS;

  @Column(name = "is_active", nullable = false)
  private boolean isActive;

  @Column(name = "send_zero_anomaly_email", nullable = false)
  private boolean sendZeroAnomalyEmail;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("collection", collection).add("metric", metric)
        .add("fromAddress", fromAddress).add("toAddresses", toAddresses).add("cron", cron)
        .add("smtpHost", smtpHost).add("smtpPort", smtpPort).add("smtpUser", smtpUser)
        .add("windowSize", windowSize).add("windowUnit", windowUnit).add("isActive", isActive)
        .add("sendZeroAnomalyEmail", sendZeroAnomalyEmail)
        .toString();
  }
}
