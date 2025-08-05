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
package org.apache.pinot.udf.test;

import javax.annotation.Nullable;
import org.apache.pinot.core.udf.UdfExample;

/// The object used to return results from the UDF test execution framework for a single UDF example.
///
/// An execution may be successful, in which case error message will be null, or it may fail,
/// in which case the error message will be set. Remember that the actual and expected results may be null even on
/// success.
public class UdfExampleResult {

  private final UdfExample _test;
  /// The result of the test execution in case it was successful.
  ///
  /// Remember this value can be null even on success.
  ///
  /// The type of the result is determined by [UdfTestScenario] that was performed. While most of the time it will
  /// be the returned call value, in predicate executions it may be a boolean indicating whether the value matched the
  /// expected result or not
  @Nullable
  private final Object _actualResult;
  /// The expected result of the test execution.
  ///
  /// Remember this value can be null even on success.
  ///
  /// The type of the result is determined by [UdfTestScenario] that was performed. While most of the time it will
  /// be the returned call value, in predicate executions it may be a boolean indicating whether the value matched the
  /// expected result or not
  @Nullable
  private final Object _expectedResult;
  /// The error message in case the test execution failed.
  /// If null, the test execution was successful.
  @Nullable
  private final String _errorMessage;

  private UdfExampleResult(
      UdfExample test,
      @Nullable Object actualResult,
      @Nullable Object expectedResult,
      @Nullable String errorMessage) {
    _test = test;
    _actualResult = actualResult;
    _expectedResult = expectedResult;
    _errorMessage = errorMessage;
  }

  /// Creates a test result indicating a successful test execution.
  public static UdfExampleResult success(
      UdfExample test,
      @Nullable Object actualResult,
      @Nullable Object expectedResult) {
    return new UdfExampleResult(test, actualResult, expectedResult, null);
  }

  /// Creates a test result indicating an error in the test execution.
  public static UdfExampleResult error(UdfExample test, String errorMessage) {
    return new UdfExampleResult(test, null, null, errorMessage);
  }

  public UdfExample getTest() {
    return _test;
  }

  @Nullable
  public Object getActualResult() {
    return _actualResult;
  }

  @Nullable
  public Object getExpectedResult() {
    return _expectedResult;
  }

  @Nullable
  public String getErrorMessage() {
    return _errorMessage;
  }
}
