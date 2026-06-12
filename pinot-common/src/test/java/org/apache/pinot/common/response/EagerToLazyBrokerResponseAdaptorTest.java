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
package org.apache.pinot.common.response;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Tests for [EagerToLazyBrokerResponseAdaptor].
public class EagerToLazyBrokerResponseAdaptorTest {

  /// Creates a minimal [BrokerResponse] stub that has no result table.
  private static BrokerResponse errorResponse(String requestId, List<QueryProcessingException> exceptions) {
    StreamingBrokerResponse.Metainfo metainfo = new StreamingBrokerResponse.Metainfo.Error(exceptions);
    // Build an eager response with no resultTable by consuming an EarlyResponse
    StreamingBrokerResponse earlyResponse = new StreamingBrokerResponse.EarlyResponse(metainfo);
    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(earlyResponse);
    eager.setRequestId(requestId);
    return eager;
  }

  /// Creates a [BrokerResponse] stub with a result table containing the given rows.
  private static BrokerResponse responseWithRows(DataSchema schema, List<Object[]> rows) {
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        schema, new StreamingBrokerResponse.Metainfo.Error(List.of()), rows);
    return LazyToEagerBrokerResponseAdaptor.of(streaming);
  }

  // ---- getDataSchema -----------------------------------------------------------------------

  @Test
  public void testGetDataSchemaIsNullWhenNoResultTable() {
    BrokerResponse eager = errorResponse("req-1", List.of());
    StreamingBrokerResponse lazy = new EagerToLazyBrokerResponseAdaptor(eager);

    assertNull(lazy.getDataSchema(),
        "getDataSchema() must return null when the eager response has no resultTable");
  }

  @Test
  public void testGetDataSchemaMatchesResultTableSchema() {
    DataSchema schema = new DataSchema(
        new String[]{"id", "name"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, "one"});
    BrokerResponse eager = responseWithRows(schema, rows);

    StreamingBrokerResponse lazy = new EagerToLazyBrokerResponseAdaptor(eager);
    DataSchema actual = lazy.getDataSchema();

    assertNotNull(actual, "getDataSchema() must not be null when the eager response has a resultTable");
    assertEquals(actual, schema);
  }

  // ---- consumeData emits rows exactly once -------------------------------------------------

  @Test
  public void testConsumeDataEmitsRowsExactlyOnce()
      throws InterruptedException {
    DataSchema schema = new DataSchema(
        new String[]{"x"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{10});
    rows.add(new Object[]{20});
    BrokerResponse eager = responseWithRows(schema, rows);

    EagerToLazyBrokerResponseAdaptor lazy = new EagerToLazyBrokerResponseAdaptor(eager);

    AtomicInteger firstCallRowCount = new AtomicInteger(0);
    lazy.consumeData(data -> {
      while (data.next()) {
        firstCallRowCount.incrementAndGet();
      }
    });
    assertEquals(firstCallRowCount.get(), 2, "First consumeData call must emit all rows");

    // Second call must not re-emit rows — it should return metainfo but consume nothing.
    AtomicInteger secondCallRowCount = new AtomicInteger(0);
    lazy.consumeData(data -> {
      while (data.next()) {
        secondCallRowCount.incrementAndGet();
      }
    });
    assertEquals(secondCallRowCount.get(), 0,
        "Second consumeData call must not re-emit rows (idempotent after first call)");
  }

  @Test
  public void testConsumeDataReturnsMetainfoOnSecondCall()
      throws InterruptedException {
    DataSchema schema = new DataSchema(
        new String[]{"v"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG}
    );
    List<QueryProcessingException> exceptions =
        List.of(new QueryProcessingException(507, "test error"));
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        schema, new StreamingBrokerResponse.Metainfo.Error(exceptions), List.of());
    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);

    EagerToLazyBrokerResponseAdaptor lazy = new EagerToLazyBrokerResponseAdaptor(eager);

    // First call
    StreamingBrokerResponse.Metainfo first = lazy.consumeData(data -> {
    });
    assertNotNull(first, "First consumeData must return non-null metainfo");

    // Second call must also return metainfo (not throw, not return null).
    StreamingBrokerResponse.Metainfo second = lazy.consumeData(data -> {
    });
    assertNotNull(second, "Second consumeData must also return non-null metainfo");
    assertEquals(second.getExceptions().size(), 1,
        "Metainfo from second call must still carry exceptions from the eager response");
  }

  // ---- asJson excludes resultTable ---------------------------------------------------------

  @Test
  public void testAsJsonExcludesResultTable() {
    DataSchema schema = new DataSchema(
        new String[]{"col"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"hello"});
    BrokerResponse eager = responseWithRows(schema, rows);

    // Verify getResultTable() is non-null so we know there is actually a resultTable to exclude.
    assertNotNull(eager.getResultTable(), "Precondition: eager response must have a resultTable");

    EagerToLazyBrokerResponseAdaptor lazy = new EagerToLazyBrokerResponseAdaptor(eager);
    ObjectNode json = lazy.getMetaInfo().asJson();

    assertNull(json.get("resultTable"),
        "asJson() must exclude resultTable from the metainfo JSON (Jackson mixin must be active)");
  }

  @Test
  public void testAsJsonPreservesRequestIdAndExceptions() {
    QueryProcessingException ex = new QueryProcessingException(400, "bad query");
    BrokerResponse eager = errorResponse("req-42", List.of(ex));

    EagerToLazyBrokerResponseAdaptor lazy = new EagerToLazyBrokerResponseAdaptor(eager);
    ObjectNode json = lazy.getMetaInfo().asJson();

    // requestId
    assertNotNull(json.get("requestId"), "requestId must be present in metainfo JSON");
    assertEquals(json.get("requestId").asText(), "req-42");

    // exceptions
    assertNotNull(json.get("exceptions"), "exceptions must be present in metainfo JSON");
    assertFalse(json.get("exceptions").isEmpty(), "exceptions array must be non-empty");
    assertEquals(json.get("exceptions").get(0).get("errorCode").asInt(), 400);
  }

  /// The Jackson mixin in [EagerToLazyBrokerResponseAdaptor.EagerBrokerResponseToMetainfo] adds
  /// {@code @JsonIgnore} on {@code getResultTable()}.  If the mixin is not applied (e.g. the wrong
  /// ObjectMapper is used), the resultTable will appear in the JSON and this test will fail.
  @Test
  public void testMixinIsApplied_failsIfResultTableAppearsInJson() {
    DataSchema schema = new DataSchema(
        new String[]{"a", "b"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 2L});
    BrokerResponse eager = responseWithRows(schema, rows);
    assertNotNull(eager.getResultTable(), "Precondition: eager response must have a resultTable");

    EagerToLazyBrokerResponseAdaptor lazy = new EagerToLazyBrokerResponseAdaptor(eager);
    ObjectNode json = lazy.getMetaInfo().asJson();

    // This assertion directly tests that the mixin is active.
    assertNull(json.get("resultTable"),
        "The StreamingBrokerMetainfoMixing @JsonIgnore must suppress resultTable from asJson(); "
            + "if resultTable appears, the mixin was not applied correctly.");

    // Cross-check: other fields are still present (proves we serialized something meaningful).
    boolean hasAnyField = false;
    Iterator<String> fieldNames = json.fieldNames();
    while (fieldNames.hasNext()) {
      fieldNames.next();
      hasAnyField = true;
      break;
    }
    assertFalse(!hasAnyField, "asJson() must produce a non-empty JSON object");
  }

  // ---- ResultTable in lazy adaptor ---------------------------------------------------------

  @Test
  public void testGetDataSchemaReturnedFromResultTable() {
    DataSchema schema = new DataSchema(
        new String[]{"p", "q"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.FLOAT}
    );

    // Build an eager BrokerResponse that has a ResultTable directly.
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        schema, new StreamingBrokerResponse.Metainfo.Error(List.of()),
        List.<Object[]>of(new Object[]{1.0, 2.0f}));
    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);

    EagerToLazyBrokerResponseAdaptor lazy = new EagerToLazyBrokerResponseAdaptor(eager);

    DataSchema actual = lazy.getDataSchema();
    assertNotNull(actual);
    assertEquals(actual.getColumnNames(), schema.getColumnNames());
    assertEquals(actual.getColumnDataTypes(), schema.getColumnDataTypes());
  }
}
