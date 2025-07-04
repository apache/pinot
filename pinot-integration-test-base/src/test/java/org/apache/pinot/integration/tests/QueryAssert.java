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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Locale;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractSoftAssertions;

/// A custom AssertJ assertion class for query responses that provides a fluent API for asserting on query responses.
///
/// The current implementation is partial and we should be adding more methods to support more use cases as more tests
/// are migrated to this class instead of TestNG's Assert class.
public class QueryAssert extends AbstractAssert<QueryAssert, JsonNode> {
  public QueryAssert(JsonNode jsonNode) {
    super(jsonNode, QueryAssert.class);
  }

  public static QueryAssert assertThat(JsonNode actual) {
    return new QueryAssert(actual);
  }

  public QueryAssert hasNoExceptions() {
    isNotNull();
    if (actual.has("exceptions") && !actual.get("exceptions").isEmpty()) {
      failWithMessage("Expected no exceptions but found <%s>", actual.get("exceptions"));
    }
    return this;
  }

  /// Obtains the first exception in the query response, returning it as a [QueryErrorAssert] object.
  ///
  /// It fails if there are no exceptions in the query response.
  public QueryErrorAssert firstException() {
    isNotNull();
    if (!actual.has("exceptions")) {
      failWithMessage("No exceptions found in query response");
    }
    JsonNode exceptions = actual.get("exceptions");
    if (exceptions.isEmpty()) {
      failWithMessage("No exceptions found in query response");
    }
    return new QueryErrorAssert(actual.get("exceptions").get(0));
  }


  /// Obtains the first exception in the query response, returning it as a [QueryErrorAssert.Soft] object.
  ///
  /// It fails if there are no exceptions in the query response.
  ///
  /// Unlike [#firstException], this method returns a [QueryErrorAssert.Soft] object, which allows for multiple
  /// assertions to be made before failing.
  ///
  /// See [Soft Assertions in AssertJ docs](https://assertj.github.io/doc/#assertj-core-soft-assertions)
  public QueryErrorAssert.Soft softFirstException() {
    isNotNull();
    if (!actual.has("exceptions")) {
      failWithMessage("No exceptions found in query response");
    }
    JsonNode exceptions = actual.get("exceptions");
    if (exceptions.isEmpty()) {
      failWithMessage("No exceptions found in query response");
    }

    return new QueryErrorAssert.Soft(actual.get("exceptions").get(0));
  }

  public static class Soft extends AbstractSoftAssertions implements AutoCloseable {

    public QueryAssert assertThat(JsonNode actual) {
      return proxy(QueryAssert.class, JsonNode.class, actual);
    }

    /// Closes this object, asserting that all soft assertions have passed.
    @Override
    public void close() {
      super.assertAll();
    }
  }


  public static class QueryErrorAssert extends AbstractAssert<QueryErrorAssert, JsonNode> {
    public QueryErrorAssert(JsonNode jsonNode) {
      super(jsonNode, QueryErrorAssert.class);
    }

    /// Asserts that the error code in the exception matches the given error code.
    ///
    /// In case of a mismatch, it fails with a message indicating the expected and actual error codes in both numeric
    /// and enum form.
    public QueryErrorAssert hasErrorCode(QueryErrorCode errorCode) {
      isNotNull();
      int actualId = actual.get("errorCode").intValue();
      if (actualId != errorCode.getId()) {
        String actualValueDescription;
        try {
          actualValueDescription = QueryErrorCode.fromErrorCode(actualId) + " (" + actualId + ")";
        } catch (IllegalArgumentException e) {
          actualValueDescription = "*INVALID ERROR CODE*" + " (" + actualId + ")";
        }
        failWithMessage("Expected error code <%s (%s)> but was <%s>. Error message is %s",
            errorCode, errorCode.getId(), actualValueDescription, actual.get("message").asText());
      }
      return this;
    }

    /// Asserts that the message in the exception contains the given message (ignoring case).
    public QueryErrorAssert containsMessage(String message) {
      isNotNull();
      if (actual == null || !actual.has("message")) {
        failWithMessage("Expected message %s but no message found in exception", message);
      } else {
        String actualMessage = actual.get("message").asText();
        if (!actualMessage.toLowerCase(Locale.US).contains(message.toLowerCase(Locale.US))) {
          failWithMessage("Expected message to contain <%s> but was <%s>", message, actualMessage);
        }
      }
      return this;
    }

    public static class Soft extends AbstractSoftAssertions implements AutoCloseable {
      private final QueryErrorAssert _assert;

      public Soft(JsonNode exception) {
        _assert = proxy(QueryErrorAssert.class, JsonNode.class, exception);
      }

      public QueryErrorAssert hasErrorCode(QueryErrorCode errorCode) {
        return _assert.hasErrorCode(errorCode);
      }

      public QueryErrorAssert containsMessage(String message) {
        return _assert.containsMessage(message);
      }

      @Override
      public void close() {
        super.assertAll();
      }
    }
  }
}
