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
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.apache.pinot.query.validate.GroupByDiagnostics;
import org.apache.pinot.spi.exception.QueryErrorCode;

/// Per-query response item for {@code POST /query/sql/validateQueryCompilation}. Reports whether
/// query compilation succeeded and, when it did not, surfaces the corresponding Pinot query error
/// code and message along with the input SQL used for compilation.
/// Thread-safety: immutable and thread-safe.
public class SqlCompilationValidationResponse {
  private final boolean _compiledSuccessfully;
  private final String _errorMessage;
  private final QueryErrorCode _errorCode;
  private final String _sql;
  private final GroupByDiagnostics _groupByDiagnostics;

  public SqlCompilationValidationResponse(boolean compiledSuccessfully, @Nullable String errorMessage,
      @Nullable QueryErrorCode errorCode, @Nullable String sql) {
    this(compiledSuccessfully, errorMessage, errorCode, sql, null);
  }

  public SqlCompilationValidationResponse(boolean compiledSuccessfully, @Nullable String errorMessage,
      @Nullable QueryErrorCode errorCode, @Nullable String sql, @Nullable GroupByDiagnostics groupByDiagnostics) {
    _compiledSuccessfully = compiledSuccessfully;
    _errorMessage = errorMessage;
    _errorCode = errorCode;
    _sql = sql;
    _groupByDiagnostics = groupByDiagnostics;
  }

  @JsonProperty("compiledSuccessfully")
  public boolean isCompiledSuccessfully() {
    return _compiledSuccessfully;
  }

  @Nullable
  @JsonProperty("errorMessage")
  public String getErrorMessage() {
    return _errorMessage;
  }

  @Nullable
  @JsonProperty("errorCode")
  public QueryErrorCode getErrorCode() {
    return _errorCode;
  }

  @Nullable
  @JsonProperty("sql")
  public String getSql() {
    return _sql;
  }

  @Nullable
  @JsonProperty("groupByDiagnostics")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public GroupByDiagnostics getGroupByDiagnostics() {
    return _groupByDiagnostics;
  }
}
