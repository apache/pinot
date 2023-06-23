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
    _instanceName = instanceName;
    return this;
  }

  public boolean isSafe() {
    return _safe;
  }

  public OperationSafetyCheckResponse setSafe(boolean safe) {
    _safe = safe;
    return this;
  }

  public List<String> getIssues() {
    return _issues;
  }

  public OperationSafetyCheckResponse setIssues(List<String> issues) {
    _issues = issues;
    return this;
  }
}
