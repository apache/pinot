package org.apache.pinot.common.response.server;

import org.apache.pinot.spi.annotations.InterfaceStability;


@InterfaceStability.Unstable
public class ApiErrorResponse {

  private String _errorMsg;
  private String _stacktrace;

  public String getErrorMsg() {
    return _errorMsg;
  }

  public ApiErrorResponse setErrorMsg(String errorMsg) {
    _errorMsg = errorMsg;
    return this;
  }

  public String getStacktrace() {
    return _stacktrace;
  }

  public ApiErrorResponse setStacktrace(String stacktrace) {
    _stacktrace = stacktrace;
    return this;
  }
}
