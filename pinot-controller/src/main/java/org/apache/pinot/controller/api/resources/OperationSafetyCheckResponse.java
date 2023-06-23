package org.apache.pinot.controller.api.resources;

import java.util.List;


public class OperationSafetyCheckResponse {
  private String _instanceName;
  private boolean _safe;
  private List<String> _issues;

  public String getInstanceName() {
    return _instanceName;
  }

  public OperationSafetyCheckResponse setInstanceName(String instanceName) {
    this._instanceName = instanceName;
    return this;
  }

  public boolean isSafe() {
    return _safe;
  }

  public OperationSafetyCheckResponse setSafe(boolean safe) {
    this._safe = safe;
    return this;
  }

  public List<String> getIssues() {
    return _issues;
  }

  public OperationSafetyCheckResponse setIssues(List<String> issues) {
    this._issues = issues;
    return this;
  }
}
