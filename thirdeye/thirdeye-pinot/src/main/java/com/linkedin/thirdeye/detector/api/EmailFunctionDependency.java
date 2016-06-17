package com.linkedin.thirdeye.detector.api;

import java.io.Serializable;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;

@Entity
@Table(name = "email_function_dependencies")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.api.EmailFunctionDependency#find", query = "SELECT d FROM EmailFunctionDependency d"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.EmailFunctionDependency#findByEmail", query = "SELECT d FROM EmailFunctionDependency d where d.emailId=:emailId"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.EmailFunctionDependency#findByFunction", query = "SELECT d FROM EmailFunctionDependency d where d.functionId=:functionId"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.EmailFunctionDependency#deleteByEmail", query = "DELETE FROM EmailFunctionDependency d where d.emailId=:emailId"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyFunctionRelation#deleteByFunction", query = "DELETE FROM EmailFunctionDependency d where d.functionId=:functionId")
})
public class EmailFunctionDependency implements Serializable {
  // composite id classes need to be serializable in hibernate
  private static final long serialVersionUID = 1L;

  @Id
  @Column(name = "email_id", nullable = false)
  private long emailId;

  @Id
  @Column(name = "function_id", nullable = false)
  private long functionId;

  public EmailFunctionDependency() {
  }

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
