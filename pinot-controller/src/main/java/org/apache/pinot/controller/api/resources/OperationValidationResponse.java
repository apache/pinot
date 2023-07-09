/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;


public class OperationValidationResponse {
  private String _instanceName;
  private boolean _safe;
  private final List<ErrorWrapper> _issues = new ArrayList<>();

  @JsonProperty("instanceName")
  public String getInstanceName() {
    return _instanceName;
  }

  public OperationValidationResponse setInstanceName(String instanceName) {
    _instanceName = instanceName;
    return this;
  }

  @JsonProperty("isSafe")
  public boolean isSafe() {
    return _safe;
  }

  public OperationValidationResponse setSafe(boolean safe) {
    _safe = safe;
    return this;
  }

  @JsonProperty("issues")
  public List<ErrorWrapper> getIssues() {
    return _issues;
  }

  public OperationValidationResponse putIssue(ErrorCode code, String... args) {
    _issues.add(new ErrorWrapper(code, args));
    return this;
  }

  public String getIssueMessage(int index) {
    return _issues.get(index).getMessage();
  }

  public static class ErrorWrapper {
    ErrorCode _code;
    String _message;

    public ErrorWrapper(ErrorCode code, String... args) {
      _code = code;
      _message = String.format(code._description, args);
    }

    public ErrorCode getCode() {
      return _code;
    }

    public String getMessage() {
      return _message;
    }
  }

  public enum ErrorCode {
    IS_ALIVE("Instance %s is still live"),
    CONTAINS_RESOURCE("Instance %s exists in ideal state for %s");

    public final String _description;

    ErrorCode(String description) {
      _description = description;
    }
  }
}
