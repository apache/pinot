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
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractSoftAssertions;

public class QueryAssert extends AbstractAssert<QueryAssert, JsonNode> {
  public QueryAssert(JsonNode jsonNode) {
    super(jsonNode, QueryAssert.class);
  }

  public static QueryAssert assertThat(JsonNode actual) {
    return new QueryAssert(actual);
  }

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

    @Override
    public void close()
        throws Exception {
      super.assertAll();
    }
  }


  public static class QueryErrorAssert extends AbstractAssert<QueryErrorAssert, JsonNode> {
    public QueryErrorAssert(JsonNode jsonNode) {
      super(jsonNode, QueryErrorAssert.class);
    }

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
        failWithMessage("Expected error code <%s (%s)> but was <%s>",
            errorCode, errorCode.getId(), actualValueDescription);
      }
      return this;
    }

    public QueryErrorAssert containsMessage(String message) {
      isNotNull();
      if (actual == null || !actual.has("message")) {
        failWithMessage("Expected message %s but no message found in exception", message);
      }
      String actualMessage = actual.get("message").asText();
      if (!actualMessage.contains(message)) {
        failWithMessage("Expected message to contain <%s> but was <%s>", message, actualMessage);
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
      public void close()
          throws Exception {
        super.assertAll();
      }
    }
  }
}
