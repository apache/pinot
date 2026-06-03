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
package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.annotation.JsonInclude;


/**
 * Response for the {@code POST /query/sql/validateSyntax} broker endpoint. Reports whether a SQL
 * query was accepted by Pinot's Calcite-based parser, along with an error message when parsing
 * failed.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SqlSyntaxValidationResponse {

  private final boolean _valid;
  private final String _sqlType;
  private final String _errorMessage;

  public SqlSyntaxValidationResponse(boolean valid, String sqlType, String errorMessage) {
    _valid = valid;
    _sqlType = sqlType;
    _errorMessage = errorMessage;
  }

  public static SqlSyntaxValidationResponse valid(String sqlType) {
    return new SqlSyntaxValidationResponse(true, sqlType, null);
  }

  public static SqlSyntaxValidationResponse invalid(String errorMessage) {
    return new SqlSyntaxValidationResponse(false, null, errorMessage);
  }

  public boolean isValid() {
    return _valid;
  }

  public String getSqlType() {
    return _sqlType;
  }

  public String getErrorMessage() {
    return _errorMessage;
  }
}
