package com.linkedin.pinot.query.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TODO: THis class needs work. InstanceError should extend from Throwable or Exception
 * 
 */
public class InstanceError implements Serializable {
  private final List<Integer> _errorCodeList = new ArrayList<Integer>();
  private final List<String> _errorMessageList = new ArrayList<String>();

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

  public void merge(InstanceError anotherError) {
    _errorCodeList.addAll(anotherError.getAllErrorCodes());
    _errorMessageList.addAll(anotherError.getAllErrorMessages());
  }

  @Override
  public String toString() {
    return "InstanceError [_errorCodeList=" + _errorCodeList + ", _errorMessageList=" + _errorMessageList + "]";
  }

}
