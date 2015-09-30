package com.linkedin.thirdeye.api;

import com.google.common.base.MoreObjects;
import org.hibernate.validator.constraints.Email;
import org.hibernate.validator.constraints.NotEmpty;

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
    @NamedQuery(
        name = "com.linkedin.thirdeye.api.EmailConfiguration#findAll",
        query = "SELECT c FROM EmailConfiguration c"
    )
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
  @Email
  @Column(name = "from_address", nullable = false)
  private String fromAddress;

  @Valid
  @NotNull
  @Email
  @Column(name = "to_address", nullable = false)
  private String toAddress;

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
  private int windowSize = 1;

  @Valid
  @NotNull
  @Column(name = "window_unit", nullable = false)
  private TimeUnit windowUnit = TimeUnit.DAYS;

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

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public String getToAddress() {
    return toAddress;
  }

  public void setToAddress(String toAddress) {
    this.toAddress = toAddress;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("collection", collection)
        .add("fromAddress", fromAddress)
        .add("toAddress", toAddress)
        .add("cron", cron)
        .add("smtpHost", smtpHost)
        .add("smtpPort", smtpPort)
        .add("smtpUser", smtpUser)
        .add("windowSize", windowSize)
        .add("windowUnit", windowUnit)
        .toString();
  }
}
