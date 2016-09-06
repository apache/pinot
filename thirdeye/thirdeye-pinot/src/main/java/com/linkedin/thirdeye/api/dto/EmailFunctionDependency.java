package com.linkedin.thirdeye.api.dto;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.MoreObjects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EmailFunctionDependency {
  private long emailId;

  private long functionId;

  public long getEmailId() {
    return emailId;
  }

  public void setEmailId(long emailId) {
    this.emailId = emailId;
  }

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("emailId", emailId).add("functionId", functionId)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof EmailFunctionDependency)) {
      return false;
    }
    EmailFunctionDependency r = (EmailFunctionDependency) o;
    return Objects.equals(emailId, r.getEmailId()) && Objects.equals(functionId, r.getFunctionId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(emailId, functionId);
  }
}
