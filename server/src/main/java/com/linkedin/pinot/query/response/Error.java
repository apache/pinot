package com.linkedin.pinot.query.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class Error {
  private List<Integer> _errorCodeList = new ArrayList<Integer>();
  private List<String> _errorMessageList = new ArrayList<String>();

  public String getErrorMessage(int index) {
    return _errorMessageList.get(index);
  }

  public Collection<Integer> getAllErrorCodes() {
    return _errorCodeList;
  }

  public Collection<String> getAllErrorMessages() {
    return _errorMessageList;
  }

  public void setError(Integer errorCode, String errorMessage) {
    _errorCodeList.add(errorCode);
    _errorMessageList.add(errorMessage);
  }

  public void merge(Error anotherError) {
    _errorCodeList.addAll(anotherError.getAllErrorCodes());
    _errorMessageList.addAll(anotherError.getAllErrorMessages());
  }
}
