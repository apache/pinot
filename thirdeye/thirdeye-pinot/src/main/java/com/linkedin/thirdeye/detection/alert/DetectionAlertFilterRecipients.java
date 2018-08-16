package com.linkedin.thirdeye.detection.alert;

import java.util.Objects;
import java.util.Set;


/**
 * Container class for email alert recipients
 */
public class DetectionAlertFilterRecipients {
  Set<String> to;
  Set<String> cc;
  Set<String> bcc;

  public DetectionAlertFilterRecipients(Set<String> to, Set<String> cc, Set<String> bcc) {
    this.to = to;
    this.cc = cc;
    this.bcc = bcc;
  }

  public Set<String> getTo() {
    return to;
  }

  public DetectionAlertFilterRecipients setTo(Set<String> to) {
    this.to = to;
    return this;
  }

  public Set<String> getCc() {
    return cc;
  }

  public DetectionAlertFilterRecipients setCc(Set<String> cc) {
    this.cc = cc;
    return this;
  }

  public Set<String> getBcc() {
    return bcc;
  }

  public DetectionAlertFilterRecipients setBcc(Set<String> bcc) {
    this.bcc = bcc;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DetectionAlertFilterRecipients that = (DetectionAlertFilterRecipients) o;
    return Objects.equals(to, that.to) && Objects.equals(cc, that.cc) && Objects.equals(bcc, that.bcc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(to, cc, bcc);
  }
}
