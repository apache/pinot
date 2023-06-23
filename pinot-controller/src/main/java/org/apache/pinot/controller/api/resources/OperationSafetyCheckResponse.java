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
