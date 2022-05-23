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
package org.apache.pinot.core.common;

import java.util.Objects;

/**
 * Class to hold the data for a single Explain plan row
 */
public class ExplainPlanRowData {
  private final String _explainPlanString;
  private final int _operatorId;
  private final int _parentId;

  public ExplainPlanRowData(String explainPlanString, int operatorId, int parentId) {
    _explainPlanString = explainPlanString;
    _operatorId = operatorId;
    _parentId = parentId;
  }

  public String getExplainPlanString() {
    return _explainPlanString;
  }

  public int getOperatorId() {
    return _operatorId;
  }

  public int getParentId() {
    return _parentId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExplainPlanRowData that = (ExplainPlanRowData) o;
    return _operatorId == that._operatorId && _parentId == that._parentId
        && _explainPlanString.equals(that._explainPlanString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_explainPlanString, _operatorId, _parentId);
  }
}
