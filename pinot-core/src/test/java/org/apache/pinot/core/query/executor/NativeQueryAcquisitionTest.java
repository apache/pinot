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
package org.apache.pinot.core.query.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryContext.ProcessIdentity;
import org.apache.pinot.common.request.context.NativeQueryContext.Segment;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.NativeImmutableSegmentDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TableSegmentsContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.data.BuiltInVirtualColumnDefinitions;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises identity checks at the normal query acquisition boundary, including reference release and schema pinning.
public class NativeQueryAcquisitionTest {
  private static final String TABLE = "testTable_OFFLINE";
  private static final String SCHEMA = "testTable";

  @Test
  public void testPinsExactContentAndSchemaWithAbsentOptionalSegment() throws Exception {
    Fixture fixture = new Fixture();
    NativeQueryContext context = fixture.context(List.of("a"), List.of("b"));
    NativeImmutableSegmentDataManager a = fixture._installed.get("a");
    fixture._installed.remove("b");
    SingleTableExecutionInfo info = fixture.acquire(context);
    assertSame(info.getSegmentDataManagers().get(0), a);
    assertEquals(a.getReferenceCount(), 2);
    assertTrue(info.getMissingSegments().isEmpty());
    fixture._schema.setSchemaName("changed");
    assertEquals(info.getSchema().getSchemaName(), SCHEMA);
    assertTrue(info.getSchema().getColumnNames().containsAll(BuiltInVirtualColumnDefinitions.NAMES));
    info.getSchema().setSchemaName("query-local");
    assertEquals(a.getQuerySchema().getSchemaName(), SCHEMA);
    verify(fixture._table, never()).getCachedTableConfigAndSchema();
    info.releaseSegmentDataManagers();
    assertEquals(a.getReferenceCount(), 1);
  }

  @Test
  public void testMismatchInOptionalSegmentReleasesEveryPinnedHandle() {
    Fixture fixture = new Fixture();
    NativeQueryContext context = fixture.context(List.of("a"), List.of("b"));
    fixture.replaceContent("b");
    QueryException failure = expectThrows(QueryException.class, () -> fixture.acquire(context));
    assertEquals(failure.getErrorCode(), QueryErrorCode.SERVER_QUERY_IDENTITY_MISMATCH);
    fixture.assertReleased();
    verify(fixture._table, never()).getCachedTableConfigAndSchema();
  }

  @Test
  public void testRequiredAbsenceAndEmptyRequestsCannotBecomeSuccessfulEmptyScans() {
    Fixture fixture = new Fixture();
    NativeQueryContext context = fixture.context(List.of("a", "b"), List.of());
    fixture._installed.remove("b");
    when(fixture._table.isSegmentDeletedRecently("b")).thenReturn(true);
    QueryException failure = expectThrows(QueryException.class, () -> fixture.acquire(context));
    assertEquals(failure.getErrorCode(), QueryErrorCode.SERVER_SEGMENT_MISSING);
    fixture.assertReleased();
    NativeQueryContext empty = fixture.context(List.of(), List.of());
    assertEquals(expectThrows(QueryException.class, () -> fixture.acquire(empty)).getErrorCode(),
        QueryErrorCode.SERVER_SEGMENT_MISSING);
  }

  @Test
  public void testModeAndNameChecksRunBeforeAcquisition() {
    Fixture fixture = new Fixture();
    assertEquals(expectThrows(QueryException.class,
        () -> SingleTableExecutionInfo.create(fixture._instance, TABLE, List.of(), null, mock(QueryContext.class)))
        .getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    NativeQueryContext context = fixture.context(List.of("a"), List.of());
    assertEquals(expectThrows(QueryException.class,
        () -> SingleTableExecutionInfo.create(fixture._instance, TABLE, List.of("b"), null,
            mock(QueryContext.class), context)).getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    when(fixture._table.requiresQueryIdentity()).thenReturn(false);
    assertEquals(expectThrows(QueryException.class, () -> fixture.acquire(context)).getErrorCode(),
        QueryErrorCode.QUERY_VALIDATION);
    verify(fixture._table, never()).acquireSegments(anyList(), any(), anyList());
  }

  @Test
  public void testNormalExecutorReturnsIdentityErrorBeforePlanning() throws Exception {
    Fixture fixture = new Fixture();
    NativeQueryContext context = fixture.context(List.of("a", "b"), List.of());
    fixture.replaceContent("b");
    ServerMetrics metrics = mock(ServerMetrics.class);
    ServerQueryExecutorV1Impl executor = new ServerQueryExecutorV1Impl();
    executor.init(new PinotConfiguration(), fixture._instance, metrics);
    InstanceRequest request = new InstanceRequest(1, CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT COUNT(*) FROM " + TABLE));
    request.setBrokerId(context.broker().participantId());
    request.setSearchSegments(context.requiredNames());
    request.setNativeQueryContext(context.toThrift());
    // Even an explicit missing-segment option cannot turn an identity mismatch into a successful partial scan.
    request.getQuery().getPinotQuery().putToQueryOptions("ignoreMissingSegments", "true");
    ExecutorService executorService = mock(ExecutorService.class);
    try {
      ServerQueryRequest query = new ServerQueryRequest(request, metrics, System.currentTimeMillis());
      InstanceResponseBlock response = executor.execute(query, executorService);
      assertEquals(response.getExceptions().size(), 1);
      assertTrue(response.getExceptions().containsKey(QueryErrorCode.SERVER_QUERY_IDENTITY_MISMATCH.getId()));
      assertNull(response.getResultsBlock());
      fixture.assertReleased();
    } finally {
      executor.shutDown();
    }
  }

  @Test
  public void testLogicalFailureReleasesPreviouslyPinnedLegacyTable() {
    Fixture fixture = new Fixture();
    TableDataManager legacy = mock(TableDataManager.class);
    SegmentDataManager segment = mock(SegmentDataManager.class);
    when(segment.getSegment()).thenReturn(mock(ImmutableSegment.class));
    when(legacy.acquireSegments(anyList(), any(), anyList())).thenReturn(List.of(segment));
    when(fixture._instance.getTableDataManager("legacy_OFFLINE")).thenReturn(legacy);
    ServerQueryRequest query = mock(ServerQueryRequest.class);
    when(query.getTableSegmentsContexts()).thenReturn(List.of(
        new TableSegmentsContext("legacy_OFFLINE", List.of("legacy"), null),
        new TableSegmentsContext(TABLE, List.of("a"), null)));
    QueryException failure = expectThrows(QueryException.class,
        () -> LogicalTableExecutionInfo.create(fixture._instance, query, mock(QueryContext.class)));
    assertEquals(failure.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    verify(legacy).releaseSegment(segment);
  }

  private static final class Fixture {
    private final UUID _history = UUID.randomUUID();
    private final ObjectVersion _tableId = new ObjectVersion(UUID.randomUUID(), 1);
    private final ObjectVersion _schemaId = new ObjectVersion(UUID.randomUUID(), 1);
    private final Participant _server = new Participant("Server_test", Role.SERVER, UUID.randomUUID(), 1);
    private final Schema _schema = new Schema.SchemaBuilder().setSchemaName(SCHEMA)
        .addSingleValueDimension("value", DataType.INT).build();
    private final Map<String, Identity> _identities = new HashMap<>();
    private final Map<String, NativeImmutableSegmentDataManager> _installed = new HashMap<>();
    private final TableDataManager _table = mock(TableDataManager.class);
    private final InstanceDataManager _instance = mock(InstanceDataManager.class);

    private Fixture() {
      for (String name : List.of("a", "b")) {
        Identity identity = identity(name);
        _identities.put(name, identity);
        install(identity);
      }
      when(_instance.getTableDataManager(TABLE)).thenReturn(_table);
      when(_table.requiresQueryIdentity()).thenReturn(true);
      when(_table.acquireSegments(anyList(), any(), anyList())).thenAnswer(call -> {
        List<String> required = call.getArgument(0);
        List<String> optional = call.getArgument(1);
        List<String> missing = call.getArgument(2);
        List<SegmentDataManager> result = new ArrayList<>();
        for (String name : required) {
          NativeImmutableSegmentDataManager manager = _installed.get(name);
          if (manager == null) {
            missing.add(name);
          } else {
            assertTrue(manager.increaseReferenceCount());
            result.add(manager);
          }
        }
        if (optional != null) {
          for (String name : optional) {
            NativeImmutableSegmentDataManager manager = _installed.get(name);
            if (manager != null) {
              assertTrue(manager.increaseReferenceCount());
              result.add(manager);
            }
          }
        }
        return result;
      });
      doAnswer(call -> {
        ((SegmentDataManager) call.getArgument(0)).decreaseReferenceCount();
        return null;
      }).when(_table).releaseSegment(any());
    }

    private Identity identity(String name) {
      Participant controller = new Participant("Controller_test", Role.CONTROLLER, UUID.randomUUID(), 1);
      return new Identity(_history, TABLE, _tableId, SCHEMA, _schemaId, name,
          new ObjectVersion(UUID.randomUUID(), 1), "a".repeat(64), _server,
          new ObjectVersion(UUID.randomUUID(), 1), UUID.randomUUID(),
          new Operation(UUID.randomUUID(), 1, new Owner(1, 0, 1, controller)));
    }

    private void install(Identity identity) {
      ImmutableSegment segment = mock(ImmutableSegment.class);
      when(segment.getSegmentName()).thenReturn(identity.segmentName());
      _installed.put(identity.segmentName(), new NativeImmutableSegmentDataManager(segment, identity, _schema));
    }

    private void replaceContent(String name) {
      install(identity(name));
    }

    private NativeQueryContext context(List<String> required, List<String> optional) {
      return new NativeQueryContext(NativeQueryContext.VERSION, _history,
          new ProcessIdentity("Broker_test", UUID.randomUUID(), 1), _server, UUID.randomUUID(), UUID.randomUUID(),
          TABLE, _tableId, SCHEMA, _schemaId, required.stream().map(this::expected).toList(),
          optional.stream().map(this::expected).toList());
    }

    private Segment expected(String name) {
      Identity identity = _identities.get(name);
      return new Segment(name, identity.content(), identity.contentSha256(), identity.assignment(),
          identity.materializationId());
    }

    private SingleTableExecutionInfo acquire(NativeQueryContext context) throws Exception {
      return SingleTableExecutionInfo.create(_instance, TABLE, context.requiredNames(), context.optionalNames(),
          mock(QueryContext.class), context);
    }

    private void assertReleased() {
      for (NativeImmutableSegmentDataManager manager : _installed.values()) {
        assertEquals(manager.getReferenceCount(), 1);
      }
    }
  }
}
