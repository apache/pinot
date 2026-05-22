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
package org.apache.pinot.compat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests retry classification for broker responses returned while compatibility tests roll components.
public class StreamOpTest {
  private static final String QUERY = "SELECT count(*) FROM testTable";

  @Test
  public void testRetryableExceptionArrayWithNumericErrorCode()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":"
        + QueryErrorCode.BROKER_INSTANCE_MISSING.getId() + ",\"message\":\"No routing table\"}]}");

    assertTrue(StreamOp.hasOnlyRetryableQueryExceptions(response.get("exceptions")));
  }

  @Test
  public void testRetryableExceptionArrayWithBrokerSegmentUnavailableCode()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":"
        + QueryErrorCode.BROKER_SEGMENT_UNAVAILABLE.getId() + ",\"message\":\"Segment unavailable\"}]}");

    assertTrue(StreamOp.hasOnlyRetryableQueryExceptions(response.get("exceptions")));
  }

  @Test
  public void testRetryableExceptionArrayWithShutdownCode()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":"
        + QueryErrorCode.SERVER_SHUTTING_DOWN.getId() + ",\"message\":\"Server shutting down\"}]}");

    assertTrue(StreamOp.hasOnlyRetryableQueryExceptions(response.get("exceptions")));
  }

  @Test
  public void testNonRetryableExceptionArray()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":"
        + QueryErrorCode.SQL_PARSING.getId() + ",\"message\":\"Bad query\"}]}");

    assertFalse(StreamOp.hasOnlyRetryableQueryExceptions(response.get("exceptions")));
  }

  @Test
  public void testExceptionWithoutErrorCodeIsNotRetryable()
      throws Exception {
    JsonNode responseWithArray = JsonUtils.stringToJsonNode("{\"exceptions\":[{\"code\":\""
        + QueryErrorCode.BROKER_INSTANCE_MISSING.name() + "\",\"message\":\"No routing table\"}]}");
    JsonNode responseWithObject = JsonUtils.stringToJsonNode("{\"exceptions\":{\"code\":\""
        + QueryErrorCode.BROKER_INSTANCE_MISSING.name() + "\",\"message\":\"No routing table\"}}");

    assertFalse(StreamOp.hasOnlyRetryableQueryExceptions(responseWithArray.get("exceptions")));
    assertFalse(StreamOp.hasOnlyRetryableQueryExceptions(responseWithObject.get("exceptions")));
    RuntimeException thrown = expectThrows(RuntimeException.class,
        () -> StreamOp.extractTotalDocs(QUERY, responseWithArray));
    assertTrue(thrown.getMessage().contains(QueryErrorCode.BROKER_INSTANCE_MISSING.name()));
  }

  @Test
  public void testMixedExceptionArrayIsNotRetryable()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":"
        + QueryErrorCode.BROKER_INSTANCE_MISSING.getId() + ",\"message\":\""
        + QueryErrorCode.BROKER_INSTANCE_MISSING.getDefaultMessage() + "\"},{\"errorCode\":"
        + QueryErrorCode.SQL_PARSING.getId() + ",\"message\":\""
        + QueryErrorCode.SQL_PARSING.getDefaultMessage() + "\"}]}");

    assertFalse(StreamOp.hasOnlyRetryableQueryExceptions(response.get("exceptions")));
    RuntimeException thrown = expectThrows(RuntimeException.class, () -> StreamOp.extractTotalDocs(QUERY, response));
    assertTrue(thrown.getMessage().contains(QueryErrorCode.SQL_PARSING.getDefaultMessage()));
  }

  @Test
  public void testWaitForDocsRetriesTransientFailures()
      throws Exception {
    List<JsonNode> responses = List.of(
        responseWithErrorCodeObject(QueryErrorCode.BROKER_INSTANCE_MISSING),
        responseWithErrorCodeArray(QueryErrorCode.QUERY_SCHEDULING_TIMEOUT),
        responseWithErrorCodeArray(QueryErrorCode.SERVER_NOT_RESPONDING),
        partialResultResponse(),
        totalDocsResponse(12));
    AtomicInteger attempts = new AtomicInteger();

    new StreamOp().waitForDocs("testTable", 1_000L, 0L, docs -> docs == 12, () -> "Failed to load docs", () -> {
      int attempt = attempts.getAndIncrement();
      return StreamOp.extractTotalDocs(QUERY, responses.get(attempt));
    });

    assertEquals(attempts.get(), responses.size());
  }

  @Test
  public void testWaitForDocsRetriesThrownQueryException()
      throws Exception {
    AtomicInteger attempts = new AtomicInteger();

    new StreamOp().waitForDocs("testTable", 1_000L, 0L, docs -> docs == 12, () -> "Failed to load docs", () -> {
      if (attempts.getAndIncrement() == 0) {
        throw new StreamOp.RetryableQueryException("Broker unavailable");
      }
      return 12;
    });

    assertEquals(attempts.get(), 2);
  }

  @Test
  public void testWaitForDocsFailsFastOnNonRetryableException()
      throws Exception {
    AtomicInteger attempts = new AtomicInteger();

    RuntimeException thrown = expectThrows(RuntimeException.class,
        () -> new StreamOp().waitForDocs("testTable", 1_000L, 0L, docs -> true, () -> "Failed to load docs", () -> {
          attempts.incrementAndGet();
          return StreamOp.extractTotalDocs(QUERY, responseWithErrorCodeArray(QueryErrorCode.SQL_PARSING));
        }));

    assertTrue(thrown.getMessage().contains(QueryErrorCode.SQL_PARSING.getDefaultMessage()));
    assertEquals(attempts.get(), 1);
  }

  private static JsonNode responseWithErrorCodeArray(QueryErrorCode queryErrorCode)
      throws Exception {
    return JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":" + queryErrorCode.getId()
        + ",\"message\":\"" + queryErrorCode.getDefaultMessage() + "\"}]}");
  }

  private static JsonNode responseWithErrorCodeObject(QueryErrorCode queryErrorCode)
      throws Exception {
    return JsonUtils.stringToJsonNode("{\"exceptions\":{\"errorCode\":" + queryErrorCode.getId()
        + ",\"message\":\"" + queryErrorCode.getDefaultMessage() + "\"}}");
  }

  private static JsonNode partialResultResponse()
      throws Exception {
    return JsonUtils.stringToJsonNode("{\"numServersQueried\":2,\"numServersResponded\":1,\"totalDocs\":10}");
  }

  private static JsonNode totalDocsResponse(long totalDocs)
      throws Exception {
    return JsonUtils.stringToJsonNode("{\"numServersQueried\":1,\"numServersResponded\":1,\"totalDocs\":" + totalDocs
        + "}");
  }
}
