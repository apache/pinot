package org.apache.pinot.controller.api.resources;

import java.util.Map;

public class ConfigValidationResponse {
  private final String _validationResponse;
  private final Map<String, Object> _unparseableProps;

  public ConfigValidationResponse(String validationResponse, Map<String, Object> unparseableProps) {
    _validationResponse = validationResponse;
    _unparseableProps = unparseableProps;
  }

  public String getValidationResponse() {
    return _validationResponse;
  }

  public Map<String, Object> getUnparseableProps() {
    return _unparseableProps;
  }
}
