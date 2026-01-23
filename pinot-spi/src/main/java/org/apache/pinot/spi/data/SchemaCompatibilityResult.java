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
package org.apache.pinot.spi.data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Result class for schema backward compatibility checking that provides detailed information
 * about incompatibilities to help developers understand and fix schema evolution issues.
 */
public class SchemaCompatibilityResult {

  public enum ErrorCode {
    MISSING_COLUMN("MISSING_COLUMN", "Column '%s' exists in old schema but is missing in new schema"),
    DATA_TYPE_MISMATCH("DATA_TYPE_MISMATCH", "Column '%s' has data type mismatch: old=%s, new=%s"),
    FIELD_TYPE_MISMATCH("FIELD_TYPE_MISMATCH", "Column '%s' has field type mismatch: old=%s, new=%s"),
    SINGLE_MULTI_VALUE_MISMATCH("SINGLE_MULTI_VALUE_MISMATCH",
        "Column '%s' has single/multi-value mismatch: old=%s, new=%s");

    private final String _code;
    private final String _messageTemplate;

    ErrorCode(String code, String messageTemplate) {
      _code = code;
      _messageTemplate = messageTemplate;
    }

    public String getCode() {
      return _code;
    }

    public String getMessageTemplate() {
      return _messageTemplate;
    }
  }

  public static class IncompatibilityIssue {
    private final ErrorCode _errorCode;
    private final String _columnName;
    private final String _message;
    private final String _suggestion;

    public IncompatibilityIssue(ErrorCode errorCode, String columnName, String message, String suggestion) {
      _errorCode = errorCode;
      _columnName = columnName;
      _message = message;
      _suggestion = suggestion;
    }

    public ErrorCode getErrorCode() {
      return _errorCode;
    }

    public String getColumnName() {
      return _columnName;
    }

    public String getMessage() {
      return _message;
    }

    public String getSuggestion() {
      return _suggestion;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("❌ ").append(_message);
      if (_suggestion != null && !_suggestion.isEmpty()) {
        sb.append("\n   💡 Suggestion: ").append(_suggestion);
      }
      return sb.toString();
    }
  }

  private final boolean _isCompatible;
  private final List<IncompatibilityIssue> _issues;

  private SchemaCompatibilityResult(boolean isCompatible, List<IncompatibilityIssue> issues) {
    _isCompatible = isCompatible;
    _issues = issues != null ? issues : new ArrayList<>();
  }

  /**
   * Creates a compatible result with no issues
   */
  public static SchemaCompatibilityResult compatible() {
    return new SchemaCompatibilityResult(true, new ArrayList<>());
  }

  /**
   * Creates an incompatible result with issues
   */
  public static SchemaCompatibilityResult incompatible(List<IncompatibilityIssue> issues) {
    return new SchemaCompatibilityResult(false, issues);
  }

  public boolean isCompatible() {
    return _isCompatible;
  }

  public List<IncompatibilityIssue> getIssues() {
    return _issues;
  }

  /**
   * Returns a formatted error message suitable for exceptions or logging
   */
  public String getDetailedErrorMessage() {
    if (_isCompatible) {
      return "Schema is backward compatible";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Schema is not backward compatible. Found ").append(_issues.size()).append(" issue(s):\n\n");

    for (int i = 0; i < _issues.size(); i++) {
      sb.append(i + 1).append(". ").append(_issues.get(i).toString());
      if (i < _issues.size() - 1) {
        sb.append("\n\n");
      }
    }

    return sb.toString();
  }

  /**
   * Returns a concise summary of all issues
   */
  public String getSummaryErrorMessage() {
    if (_isCompatible) {
      return "Schema is backward compatible";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Schema is not backward compatible: ");

    // Group issues by type
    List<String> missingColumns = _issues.stream()
        .filter(issue -> issue.getErrorCode() == ErrorCode.MISSING_COLUMN)
        .map(IncompatibilityIssue::getColumnName)
        .collect(Collectors.toList());

    List<String> dataTypeMismatches = _issues.stream()
        .filter(issue -> issue.getErrorCode() == ErrorCode.DATA_TYPE_MISMATCH)
        .map(IncompatibilityIssue::getColumnName)
        .collect(Collectors.toList());

    List<String> fieldTypeMismatches = _issues.stream()
        .filter(issue -> issue.getErrorCode() == ErrorCode.FIELD_TYPE_MISMATCH)
        .map(IncompatibilityIssue::getColumnName)
        .collect(Collectors.toList());

    List<String> singleMultiMismatches = _issues.stream()
        .filter(issue -> issue.getErrorCode() == ErrorCode.SINGLE_MULTI_VALUE_MISMATCH)
        .map(IncompatibilityIssue::getColumnName)
        .collect(Collectors.toList());

    List<String> parts = new ArrayList<>();
    if (!missingColumns.isEmpty()) {
      parts.add("missing columns [" + String.join(", ", missingColumns) + "]");
    }
    if (!dataTypeMismatches.isEmpty()) {
      parts.add("data type mismatches [" + String.join(", ", dataTypeMismatches) + "]");
    }
    if (!fieldTypeMismatches.isEmpty()) {
      parts.add("field type mismatches [" + String.join(", ", fieldTypeMismatches) + "]");
    }
    if (!singleMultiMismatches.isEmpty()) {
      parts.add("single/multi-value mismatches [" + String.join(", ", singleMultiMismatches) + "]");
    }

    sb.append(String.join(", ", parts));
    return sb.toString();
  }
}
