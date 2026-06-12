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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LazyToEagerBrokerResponseAdaptorTest {

  @Test
  public void testOfConsumesAllRows() {
    DataSchema dataSchema = new DataSchema(
        new String[]{"id", "name"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, "one"});
    rows.add(new Object[]{2, "two"});

    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(List.of()), rows);

    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);
    ResultTable resultTable = eager.getResultTable();
    assertNotNull(resultTable);
    assertEquals(resultTable.getRows().size(), 2);
    assertEquals(resultTable.getRows().get(0)[0], 1);
    assertEquals(resultTable.getRows().get(0)[1], "one");
    assertEquals(resultTable.getRows().get(1)[0], 2);
    assertEquals(resultTable.getRows().get(1)[1], "two");
  }

  @Test
  public void testGetResultTableNullWhenNullSchema()
      throws IOException {
    // Early validation errors produce a response with no data schema.
    StreamingBrokerResponse streaming = StreamingBrokerResponse.error(
        QueryErrorCode.QUERY_VALIDATION, "mismatched row sizes");
    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);

    assertNull(eager.getResultTable(), "resultTable must be null for error responses with no schema");

    JsonNode json = JsonUtils.stringToJsonNode(eager.toJsonString());
    assertNull(json.get("resultTable"), "resultTable must be absent from JSON for error responses");
  }

  @Test
  public void testGetResultTableNullWhenExceptionAndNoRows()
      throws IOException {
    // Execution errors (e.g. NUM_GROUPS_LIMIT with error_on=true) produce a response with a schema
    // but no rows and at least one exception.
    DataSchema dataSchema = new DataSchema(
        new String[]{"i", "j", "cnt"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG
        }
    );
    List<QueryProcessingException> exceptions =
        List.of(new QueryProcessingException(507, "NUM_GROUPS_LIMIT has been reached"));
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(exceptions), List.of());

    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);

    assertNull(eager.getResultTable(), "resultTable must be null when schema set but execution raised an exception");

    JsonNode json = JsonUtils.stringToJsonNode(eager.toJsonString());
    assertNull(json.get("resultTable"), "resultTable must be absent from JSON");
    assertNotNull(json.get("exceptions"), "exceptions must be present in JSON");
  }

  @Test
  public void testGetResultTableNonNullWhenEmptyRowsNoException() {
    // A valid query that matches no rows should still return an empty ResultTable (not null).
    DataSchema dataSchema = new DataSchema(
        new String[]{"id"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
    );
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(List.of()), List.of());

    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);

    ResultTable resultTable = eager.getResultTable();
    assertNotNull(resultTable, "resultTable must be non-null for a valid empty result set");
    assertEquals(resultTable.getRows().size(), 0);
  }

  @Test
  public void testSetRequestId() {
    DataSchema dataSchema = new DataSchema(
        new String[]{"id"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1});
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(List.of()), rows);
    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);
    eager.setRequestId("cursor-request-id");
    assertEquals(eager.getRequestId(), "cursor-request-id");
  }

  /// Regression test for Issue 2: isPartialResult() must read the "partialResult" key (singular),
  /// not "partialResults" (plural).  BrokerResponseNativeV2 serializes isPartialResult() as "partialResult".
  @Test
  public void testIsPartialResultKeyIsCorrect() {
    DataSchema dataSchema = new DataSchema(
        new String[]{"id"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
    );
    List<QueryProcessingException> exceptions =
        List.of(new QueryProcessingException(507, "NUM_GROUPS_LIMIT"));
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(exceptions), List.of());

    // Drive the response through BrokerResponseNativeV2 to produce an ObjectNode with the
    // canonical "partialResult" field.
    BrokerResponseNativeV2 nativeV2 = new BrokerResponseNativeV2();
    nativeV2.addException(new QueryProcessingException(507, "NUM_GROUPS_LIMIT"));
    ObjectNode nativeJson = (ObjectNode) JsonUtils.objectToJsonNode(nativeV2);
    assertTrue(nativeJson.has("partialResult"),
        "BrokerResponseNativeV2 must serialize isPartialResult() as \"partialResult\" (singular)");
    assertFalse(nativeJson.has("partialResults"),
        "BrokerResponseNativeV2 must NOT serialize as \"partialResults\" (plural)");

    // Build an adaptor whose _json contains "partialResult" (singular) from a BrokerResponseNativeV2.
    StreamingBrokerResponse eagerV2Streaming = nativeV2.toStreamingResponse();
    BrokerResponse adaptor = LazyToEagerBrokerResponseAdaptor.of(eagerV2Streaming);
    assertTrue(adaptor.isPartialResult(), "isPartialResult() must return true when partialResult=true in metainfo");
  }

  /// JSON-parity test: serializing a fully-populated [BrokerResponseNativeV2] directly must produce
  /// the same field set as serializing the same response after an eager-to-lazy-to-eager round-trip.
  @Test
  public void testJsonParityBetweenNativeV2AndRoundTrip()
      throws IOException {
    DataSchema schema = new DataSchema(
        new String[]{"id", "name"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}
    );

    // Populate a BrokerResponseNativeV2 with realistic data.
    BrokerResponseNativeV2 source = new BrokerResponseNativeV2();
    source.setRequestId("test-req-1");
    source.setResultTable(new ResultTable(schema, List.of(new Object[]{1, "alpha"}, new Object[]{2, "beta"})));
    source.setTimeUsedMs(42L);
    source.addException(new QueryProcessingException(200, "partial warning"));
    source.setNumServersQueried(3);
    source.setNumServersResponded(3);
    source.setRLSFiltersApplied(true);

    // Serialize the source directly to JSON.
    JsonNode directJson = JsonUtils.objectToJsonNode(source);

    // Round-trip: wrap source as a StreamingBrokerResponse, then eagerize.
    StreamingBrokerResponse lazy = source.toStreamingResponse();
    BrokerResponse roundTripped = LazyToEagerBrokerResponseAdaptor.of(lazy);
    // Use toJsonString() here — LazyToEagerBrokerResponseAdaptor overrides it to emit the
    // metainfo ObjectNode plus resultTable rather than relying on reflective bean serialization.
    JsonNode roundTrippedJson = JsonUtils.stringToJsonNode(roundTripped.toJsonString());

    // Assert field-by-field parity (ignore order, compare values).
    assertJsonFieldSetEquals(directJson, roundTrippedJson);
  }

  /// Asserts that two JSON objects have equal field names and equal values for every shared key.
  private static void assertJsonFieldSetEquals(JsonNode expected, JsonNode actual) {
    Iterator<String> expectedFields = expected.fieldNames();
    while (expectedFields.hasNext()) {
      String field = expectedFields.next();
      JsonNode expectedValue = expected.get(field);
      JsonNode actualValue = actual.get(field);
      assertNotNull(actualValue, "Field \"" + field + "\" present in source but missing in round-tripped JSON");
      assertEquals(actualValue, expectedValue,
          "Field \"" + field + "\" value mismatch between source and round-tripped JSON");
    }
    // Also verify no extra fields appear in the round-tripped JSON that weren't in the source.
    Iterator<String> actualFields = actual.fieldNames();
    while (actualFields.hasNext()) {
      String field = actualFields.next();
      assertNotNull(expected.get(field),
          "Field \"" + field + "\" present in round-tripped JSON but missing in source");
    }
  }

  /// Regression test: StdMetainfoDecorator.decorateMetainfoJson must return the merged {@code json} node,
  /// not the intermediate {@code statsJson} node. Returning {@code statsJson} caused asJson() to drop all
  /// original metainfo fields (exceptions, requestId, timeUsedMs, etc.).
  @Test
  public void testStdMetainfoDecoratorPreservesOriginalMetainfoFields() {
    QueryProcessingException exception =
        new QueryProcessingException(QueryErrorCode.QUERY_VALIDATION.getId(), "test error");
    StreamingBrokerResponse.Metainfo base =
        new StreamingBrokerResponse.Metainfo.Error(List.of(exception));

    StreamingBrokerResponse.EarlyResponse earlyResponse = new StreamingBrokerResponse.EarlyResponse(base);
    StreamingBrokerResponse decorated = earlyResponse.withPostMetainfo(
        stats -> stats.merge(StreamingBrokerResponse.StdMetaField.NUM_DOCS_SCANNED, 42L));

    // getMetaInfo() returns the PostDecorator-wrapped metainfo; asJson() must include BOTH
    // the original fields from the base metainfo AND the stat fields added by the decorator.
    ObjectNode json = decorated.getMetaInfo().asJson();

    // Original metainfo field: exceptions array must be present and non-empty.
    JsonNode exceptionsNode = json.get("exceptions");
    assertNotNull(exceptionsNode, "exceptions field must be present in merged JSON");
    assertTrue(exceptionsNode.isArray(), "exceptions must be an array");
    assertFalse(exceptionsNode.isEmpty(), "exceptions array must be non-empty");

    // Stat field added by the decorator must also be present.
    JsonNode numDocsScanned = json.get("numDocsScanned");
    assertNotNull(numDocsScanned, "numDocsScanned stat field must be present in merged JSON");
    assertEquals(numDocsScanned.longValue(), 42L);
  }
}
