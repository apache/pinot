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

import java.util.Map;

public final class SuccessResponse {
  private final String _status;
  private Map<String, Object> _unparseableProps;

  public SuccessResponse(String status) {
    _status = status;
  }

  public Map<String, Object> getUnparseableProps() {
    return _unparseableProps;
  }

  public void setUnparseableProps(Map<String, Object> unparseableProps) {
    _unparseableProps = unparseableProps;
  }

  public String getStatus() {
    return _status;
  }
}
