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
package org.apache.pinot.core.query.request;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.TableSegmentsInfo;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests how a request answers whether it has segments to read, which depends on which of its two mutually
/// exclusive segment representations is populated, and how SQL submitted directly to a server defers aggregation
/// construction until the executor supplies the table schema.
public class ServerQueryRequestTest {

  @Test
  public void testDirectSqlDefersAggregationsUntilSchemaIsKnown()
      throws Exception {
    QueryContext query = directSql("SELECT count(*), mode(ts) FROM tbl_OFFLINE WHERE 1 = 0");
    assertThrows(IllegalStateException.class, query::getAggregationFunctions);
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("ts", DataType.TIMESTAMP).build();
    query.setSchema(schema);
    AggregationFunction<?, ?>[] functions = query.getAggregationFunctions();
    assertEquals(functions.length, 2);
    assertEquals(functions[0].getFinalResultColumnType(), ColumnDataType.LONG);
    assertEquals(functions[1].getFinalResultColumnType(), ColumnDataType.DOUBLE);
    query.setSchema(schema);
    assertSame(query.getAggregationFunctions(), functions, "Aggregations should be constructed once");
  }

  @Test
  public void testDirectSqlResolvesBuiltInVirtualColumns()
      throws Exception {
    // A server's table schema does not declare the built-in virtual columns, but a call over them must still resolve
    // its input types. Record the types the per-call binding rule receives.
    List<ColumnDataType> resolved = new ArrayList<>();
    try (MockedStatic<AggregationFunctionTypeResolver> resolver = mockStatic(AggregationFunctionTypeResolver.class)) {
      resolver.when(() -> AggregationFunctionTypeResolver.bind(any(), anyList(), any())).thenAnswer(invocation -> {
        IntFunction<ColumnDataType> argumentTypes = invocation.getArgument(2);
        resolved.add(argumentTypes.apply(0));
        return null;
      });
      QueryContext query = directSql("SELECT mode($docId), mode($segmentName) FROM tbl_OFFLINE");
      query.setSchema(new Schema.SchemaBuilder().addSingleValueDimension("ts", DataType.TIMESTAMP).build());
      assertEquals(query.getAggregationFunctions().length, 2);
    }
    assertEquals(resolved, List.of(ColumnDataType.INT, ColumnDataType.STRING));
  }

  private static QueryContext directSql(String sql)
      throws Exception {
    Server.ServerRequest request =
        Server.ServerRequest.newBuilder().putMetadata(Request.MetadataKeys.REQUEST_ID, "1").setSql(sql).build();
    return new ServerQueryRequest(request, ServerMetrics.get()).getQueryContext();
  }

  @Test
  public void shouldHaveSegmentsWhenTheFlatListIsPopulated() {
    assertTrue(request(List.of("segment1"), null).hasSegmentsToQuery());
  }

  @Test
  public void shouldNotHaveSegmentsWhenTheFlatListIsEmpty() {
    assertFalse(request(List.of(), null).hasSegmentsToQuery());
  }

  /// A logical table sets the per-table lists instead and leaves the flat one unset, which Thrift reports as null.
  /// Reading it without checking would throw, so this pins both that it stays null and that the question is still
  /// answered from the other representation.
  @Test
  public void shouldHaveSegmentsWhenOnlyThePerTableListsArePopulated() {
    ServerQueryRequest request =
        request(null, List.of(new TableSegmentsInfo("tbl_OFFLINE", List.of("segment1"))));

    assertNull(request.getSegmentsToQuery(), "expected the flat list to be null on the logical table path");
    assertTrue(request.hasSegmentsToQuery());
  }

  @Test
  public void shouldNotHaveSegmentsWhenThePerTableListsAreAllEmpty() {
    ServerQueryRequest request = request(null,
        List.of(new TableSegmentsInfo("tbl1_OFFLINE", List.of()), new TableSegmentsInfo("tbl2_OFFLINE", List.of())));

    assertFalse(request.hasSegmentsToQuery());
  }

  @Test
  public void shouldHaveSegmentsWhenOnlyOneOfThePerTableListsIsPopulated() {
    assertTrue(request(null,
        List.of(new TableSegmentsInfo("tbl1_OFFLINE", List.of()),
            new TableSegmentsInfo("tbl2_OFFLINE", List.of("segment1")))).hasSegmentsToQuery());
  }

  /// Exactly one of the two arguments is set on a real request, mirroring
  /// `ServerPlanRequestUtils.compileInstanceRequest`.
  private static ServerQueryRequest request(@Nullable List<String> searchSegments,
      @Nullable List<TableSegmentsInfo> tableSegmentsInfoList) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(1);
    instanceRequest.setBrokerId("broker");
    instanceRequest.setQuery(CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM tbl"));
    if (searchSegments != null) {
      instanceRequest.setSearchSegments(searchSegments);
    }
    if (tableSegmentsInfoList != null) {
      instanceRequest.setTableSegmentsInfoList(tableSegmentsInfoList);
    }
    return new ServerQueryRequest(instanceRequest, ServerMetrics.get(), System.currentTimeMillis());
  }
}
