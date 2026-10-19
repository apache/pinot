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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.NativeQueryIdentity;
import org.apache.pinot.common.request.TableSegmentsInfo;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryContext.ProcessIdentity;
import org.apache.pinot.common.request.context.NativeQueryContext.Segment;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests how a request answers whether it has segments to read, which depends on which of its two mutually
/// exclusive segment representations is populated.
public class ServerQueryRequestTest {

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

  @Test
  public void testNativeIdentityTransportParityAndImmutableCapture()
      throws Exception {
    NativeQueryContext context = nativeContext();
    InstanceRequest thrift = nativeRequest(context);
    ServerQueryRequest decodedThrift = thriftRoundTrip(thrift);
    ServerQueryRequest decodedProto = protoRoundTrip(nativeRequestProto(context));
    assertEquals(decodedThrift.getNativeQueryContext().toProto(), context.toProto());
    assertEquals(decodedProto.getNativeQueryContext().toProto(), context.toProto());
    assertEquals(decodedThrift.getSegmentsToQuery(), context.requiredNames());
    assertEquals(decodedProto.getOptionalSegments(), context.optionalNames());
    assertEquals(decodedThrift.getOptionalSegments(), decodedProto.getOptionalSegments());

    ServerQueryRequest captured = new ServerQueryRequest(thrift, ServerMetrics.get(), 0);
    thrift.getNativeQueryContext().getRequiredSegments().get(0).setName("changed");
    thrift.setSearchSegments(List.of("changed"));
    assertEquals(captured.getNativeQueryContext().toProto(), context.toProto());
    assertEquals(captured.getSegmentsToQuery(), context.requiredNames());
    assertThrows(UnsupportedOperationException.class, () -> captured.getSegmentsToQuery().add("changed"));
    assertThrows(UnsupportedOperationException.class, () -> context.requiredNames().clear());

    NativeQueryContext optionalOnly = NativeQueryContext.fromProto(context.toProto().toBuilder()
        .clearRequiredSegments().build());
    InstanceRequest optionalRequest = nativeRequest(optionalOnly);
    optionalRequest.unsetSearchSegments();
    ServerQueryRequest decodedOptional = thriftRoundTrip(optionalRequest);
    assertTrue(decodedOptional.getSegmentsToQuery().isEmpty());
    assertEquals(decodedOptional.getOptionalSegments(), optionalOnly.optionalNames());
    assertEquals(decodedOptional.getSegmentsToQuery(),
        protoRoundTrip(nativeRequestProto(optionalOnly)).getSegmentsToQuery());
  }

  @Test
  public void testRejectsMalformedNativeIdentityOnBothTransports() {
    NativeQueryContext context = nativeContext();
    NativeQueryIdentity version = context.toThrift();
    version.setVersion(2);
    assertInvalidIdentity(version, context.toProto().toBuilder().setVersion(2).build());

    NativeQueryIdentity epoch = context.toThrift();
    epoch.getServer().setRegistrationEpoch(0);
    assertInvalidIdentity(epoch, context.toProto().toBuilder()
        .setServer(context.toProto().getServer().toBuilder().setRegistrationEpoch(0)).build());

    NativeQueryIdentity uuid = context.toThrift();
    uuid.setHistoryId("1-1-1-1-1");
    assertInvalidIdentity(uuid, context.toProto().toBuilder().setHistoryId("1-1-1-1-1").build());

    NativeQueryIdentity content = context.toThrift();
    content.getRequiredSegments().get(0).setContentSha256("invalid");
    assertInvalidIdentity(content, context.toProto().toBuilder().setRequiredSegments(0,
        context.toProto().getRequiredSegments(0).toBuilder().setContentSha256("invalid")).build());

    NativeQueryIdentity duplicate = context.toThrift();
    duplicate.setOptionalSegments(duplicate.getRequiredSegments());
    assertInvalidIdentity(duplicate, context.toProto().toBuilder().clearOptionalSegments()
        .addAllOptionalSegments(context.toProto().getRequiredSegmentsList()).build());

    NativeQueryIdentity count = context.toThrift();
    count.setRequiredSegments(Collections.nCopies(NativeQueryContext.MAX_SEGMENTS + 1,
        context.toThrift().getRequiredSegments().get(0)));
    assertInvalidIdentity(count, context.toProto().toBuilder().clearRequiredSegments()
        .addAllRequiredSegments(Collections.nCopies(NativeQueryContext.MAX_SEGMENTS + 1,
            context.toProto().getRequiredSegments(0))).build());
  }

  @Test
  public void testRejectsDifferentNamesAndLogicalFormsOnBothTransports() {
    NativeQueryContext context = nativeContext();
    InstanceRequest thrift = nativeRequest(context);
    thrift.setSearchSegments(List.of("other"));
    assertInvalidRequest(thrift, nativeRequestProto(context).toBuilder().clearSegments().addSegments("other").build());

    thrift = nativeRequest(context);
    thrift.setOptionalSegments(List.of());
    assertInvalidRequest(thrift, nativeRequestProto(context).toBuilder().clearOptionalSegments().build());

    thrift = nativeRequest(context);
    thrift.setBrokerId("other");
    assertInvalidRequest(thrift,
        nativeRequestProto(context).toBuilder().putMetadata(MetadataKeys.BROKER_ID, "other").build());

    thrift = nativeRequest(context);
    thrift.setQuery(CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM other_OFFLINE"));
    assertInvalidRequest(thrift,
        nativeRequestProto(context).toBuilder().setSql("SELECT * FROM other_OFFLINE").build());

    thrift = nativeRequest(context);
    thrift.setTableSegmentsInfoList(List.of(new TableSegmentsInfo(context.tableName(), context.requiredNames())));
    assertInvalidRequest(thrift, nativeRequestProto(context).toBuilder().addTableSegmentsInfo(
        Server.TableSegmentsInfo.newBuilder().setTableName(context.tableName())
            .addAllSegments(context.requiredNames())).build());
  }

  @Test
  public void testLegacyTransportRequestsRemainUnfenced()
      throws Exception {
    NativeQueryContext context = nativeContext();
    InstanceRequest thrift = nativeRequest(context);
    thrift.unsetNativeQueryContext();
    ServerQueryRequest decodedThrift = thriftRoundTrip(thrift);
    ServerQueryRequest decodedProto = protoRoundTrip(nativeRequestProto(context).toBuilder()
        .clearNativeQueryContext().build());
    assertNull(decodedThrift.getNativeQueryContext());
    assertNull(decodedProto.getNativeQueryContext());
    assertEquals(decodedThrift.getOptionalSegments(), context.optionalNames());
    assertNull(decodedProto.getOptionalSegments(), "Legacy gRPC optional-segment behavior must remain unchanged");
    assertEquals(decodedThrift.getSegmentsToQuery(), decodedProto.getSegmentsToQuery());
  }

  private static void assertInvalidIdentity(NativeQueryIdentity thrift, Server.NativeQueryIdentity proto) {
    InstanceRequest request = nativeRequest(nativeContext());
    request.setNativeQueryContext(thrift);
    assertInvalidRequest(request, nativeRequestProto(nativeContext()).toBuilder().setNativeQueryContext(proto).build());
  }

  private static void assertInvalidRequest(InstanceRequest thrift, Server.ServerRequest proto) {
    assertEquals(expectThrows(QueryException.class, () -> thriftRoundTrip(thrift)).getErrorCode(),
        QueryErrorCode.QUERY_VALIDATION);
    assertEquals(expectThrows(QueryException.class, () -> protoRoundTrip(proto)).getErrorCode(),
        QueryErrorCode.QUERY_VALIDATION);
  }

  private static ServerQueryRequest thriftRoundTrip(InstanceRequest request)
      throws Exception {
    byte[] bytes = new TSerializer(new TCompactProtocol.Factory()).serialize(request);
    InstanceRequest decoded = new InstanceRequest();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(decoded, bytes);
    return new ServerQueryRequest(decoded, ServerMetrics.get(), 0);
  }

  private static ServerQueryRequest protoRoundTrip(Server.ServerRequest request)
      throws Exception {
    return new ServerQueryRequest(Server.ServerRequest.parseFrom(request.toByteArray()), ServerMetrics.get());
  }

  private static InstanceRequest nativeRequest(NativeQueryContext context) {
    InstanceRequest request = new InstanceRequest();
    request.setRequestId(17);
    request.setBrokerId(context.broker().participantId());
    request.setQuery(CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + context.tableName()));
    request.setSearchSegments(context.requiredNames());
    request.setOptionalSegments(context.optionalNames());
    request.setNativeQueryContext(context.toThrift());
    return request;
  }

  private static Server.ServerRequest nativeRequestProto(NativeQueryContext context) {
    return Server.ServerRequest.newBuilder().putMetadata(MetadataKeys.REQUEST_ID, "17")
        .putMetadata(MetadataKeys.BROKER_ID, context.broker().participantId())
        .setSql("SELECT * FROM " + context.tableName()).addAllSegments(context.requiredNames())
        .addAllOptionalSegments(context.optionalNames()).setNativeQueryContext(context.toProto()).build();
  }

  private static NativeQueryContext nativeContext() {
    return new NativeQueryContext(1, UUID.randomUUID(), new ProcessIdentity("broker", UUID.randomUUID(), 2),
        new Participant("server", Role.SERVER, UUID.randomUUID(), 3), UUID.randomUUID(), UUID.randomUUID(),
        "tbl_OFFLINE", object(), "tbl", object(),
        List.of(new Segment("required", object(), "a".repeat(64), object(), UUID.randomUUID())),
        List.of(new Segment("optional", object(), "b".repeat(64), object(), UUID.randomUUID())));
  }

  private static ObjectVersion object() {
    return new ObjectVersion(UUID.randomUUID(), 1);
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
