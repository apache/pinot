package com.linkedin.thirdeye.detector.driver;

import java.util.Objects;

import com.google.common.base.MoreObjects;

public class FailureEmailConfiguration {
  public static final String FAILURE_EMAIL_CONFIG_KEY = "FAILURE_EMAIL_CONFIG";
  private String fromAddress;
  private String toAddresses;
  private String smtpHost;
  private int smtpPort = 25;
  private String smtpUser;
  private String smtpPassword;

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

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof FailureEmailConfiguration)) {
      return false;
    }
    FailureEmailConfiguration at = (FailureEmailConfiguration) o;
    return Objects.equals(fromAddress, at.getFromAddress())
        && Objects.equals(toAddresses, at.getToAddresses())
        && Objects.equals(smtpHost, at.getSmtpHost())
        && Objects.equals(smtpPort, at.getSmtpPort())
        && Objects.equals(smtpUser, at.getSmtpUser())
        && Objects.equals(smtpPassword, at.getSmtpPassword());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromAddress, toAddresses, smtpHost, smtpPort, smtpUser, smtpPassword);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fromAddress", fromAddress).add("toAddresses", toAddresses)
        .add("smtpHost", smtpHost).add("smtpPort", smtpPort).add("smtpUser", smtpUser).toString();
  }

}
