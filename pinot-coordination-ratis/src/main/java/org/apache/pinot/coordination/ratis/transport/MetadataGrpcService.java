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
package org.apache.pinot.coordination.ratis.transport;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangeTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataServiceGrpc;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy.ControllerIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Controller-only metadata RPC implementation. Snapshot handles retain immutable records within explicit serialized
/// byte/count/time budgets. All materialization runs outside Raft application; journal gaps always require resnapshot.
public final class MetadataGrpcService extends MetadataServiceGrpc.MetadataServiceImplBase implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataGrpcService.class);
  private final MetadataAccess _access;
  private final ControllerAccessPolicy _policy;
  private final MetadataAudit _audit;
  private final Limits _limits;
  private final Semaphore _calls;
  private final Semaphore _snapshotSlots;
  private final AtomicLong _retainedBytes = new AtomicLong();
  private final Map<String, SnapshotHandle> _snapshots = new ConcurrentHashMap<>();
  private final Object _lifecycleLock = new Object();
  private volatile boolean _closed;

  /// Bounds serialized payload residency rather than Java heap overhead. These limits are supplied by component
  /// configuration at construction; there are no environment or JVM-property fallbacks.
  public record Limits(int maxSnapshots, int maxSnapshotBytes, long maxRetainedBytes, int maxSnapshotRecords,
                       int maxPageBytes, int maxPageRecords, long snapshotTtlMillis, long maxRpcMillis,
                       int maxConcurrentCalls) {
    public Limits {
      if (maxSnapshots < 1 || maxSnapshots > 1024 || maxSnapshotBytes < 1024
          || maxSnapshotBytes > MetadataProtocol.MAX_IMAGE_BYTES || maxRetainedBytes < maxSnapshotBytes
          || maxRetainedBytes > 1_073_741_824L || maxSnapshotRecords < 1
          || maxSnapshotRecords > MetadataProtocol.MAX_IMAGE_RECORDS || maxPageBytes < 1024
          || maxPageBytes > MetadataProtocol.MAX_CHANGE_BYTES || maxPageRecords < 1
          || maxPageRecords > MetadataProtocol.MAX_CHANGE_RECORDS || snapshotTtlMillis < 1
          || snapshotTtlMillis > 300_000 || maxRpcMillis < 1 || maxRpcMillis > 30_000
          || maxConcurrentCalls < 1 || maxConcurrentCalls > 128) {
        throw new IllegalArgumentException("Invalid metadata transport bounds");
      }
    }

    public static Limits defaults() {
      return new Limits(16, 8 * 1024 * 1024, 32 * 1024 * 1024L, 10_000, MetadataProtocol.MAX_CHANGE_BYTES,
          MetadataProtocol.MAX_CHANGE_RECORDS, 30_000, 15_000, 16);
    }
  }

  MetadataGrpcService(MetadataAccess access, ControllerAccessPolicy policy, MetadataAudit audit, Limits limits) {
    _access = Objects.requireNonNull(access);
    _policy = Objects.requireNonNull(policy);
    _audit = Objects.requireNonNull(audit);
    _limits = Objects.requireNonNull(limits);
    _calls = new Semaphore(limits.maxConcurrentCalls());
    _snapshotSlots = new Semaphore(limits.maxSnapshots());
  }

  @Override
  public void execute(MetadataRpc.CommandRequest request, StreamObserver<MetadataRpc.CommandResult> observer) {
    respond("Execute", request, observer, identity -> {
      MetadataProtocol.Command command;
      try {
        command = MetadataRpcCodec.command(request, identity.clientId());
      } catch (IOException e) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid typed metadata value").asRuntimeException();
      }
      return MetadataRpcCodec.result(_access.execute(command, remainingMillis()));
    });
  }

  @Override
  public void read(MetadataRpc.ReadRequest request, StreamObserver<MetadataRpc.CommandResult> observer) {
    respond("Read", request, observer, identity -> MetadataRpcCodec.result(
        _access.query(MetadataRpcCodec.query(request, identity.clientId()), remainingMillis())));
  }

  @Override
  public void openSnapshot(MetadataRpc.OpenSnapshotRequest request,
      StreamObserver<MetadataRpc.OpenSnapshotResult> observer) {
    respond("OpenSnapshot", request, observer, identity -> {
      FeedScope scope = scope(request.getKindsList(), identity);
      UUID historyId = UUID.fromString(request.getHistoryId());
      if (!_snapshotSlots.tryAcquire()) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Snapshot handle limit reached").asRuntimeException();
      }
      boolean reserved = false;
      boolean published = false;
      try {
        reserveBytes();
        reserved = true;
        long expiresAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(_limits.snapshotTtlMillis());
        CatalogImage image;
        try {
          image = _access.image(scope, _limits.maxSnapshotRecords(), _limits.maxSnapshotBytes(), remainingMillis());
        } catch (MetadataProtocol.ResponseTooLargeException e) {
          return MetadataRpc.OpenSnapshotResult.newBuilder().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
              .build();
        }
        if (!historyId.equals(image.cursor().historyId())) {
          return MetadataRpc.OpenSnapshotResult.newBuilder().setStatus(MetadataRpc.ResultStatus.HISTORY_MISMATCH)
              .build();
        }
        if (image.records().size() > _limits.maxSnapshotRecords()) {
          throw Status.RESOURCE_EXHAUSTED.withDescription("Snapshot record limit exceeded").asRuntimeException();
        }
        List<MetadataRpc.CatalogRecord> records = new ArrayList<>(image.records().size());
        long bytes = 0;
        for (MetadataProtocol.CatalogRecord record : image.records()) {
          MetadataRpc.CatalogRecord encoded = MetadataRpcCodec.record(record);
          bytes += encoded.getSerializedSize();
          if (bytes > _limits.maxSnapshotBytes()) {
            return MetadataRpc.OpenSnapshotResult.newBuilder().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
                .build();
          }
          records.add(encoded);
        }
        if (expired(scope, image.cursor()) || expiresAt - System.nanoTime() <= 0) {
          return MetadataRpc.OpenSnapshotResult.newBuilder().setStatus(MetadataRpc.ResultStatus.CURSOR_EXPIRED).build();
        }
        reauthorize(identity);
        String token = UUID.randomUUID().toString();
        synchronized (_lifecycleLock) {
          if (_closed) {
            throw Status.UNAVAILABLE.withDescription("Metadata service closed").asRuntimeException();
          }
          _snapshots.put(token, new SnapshotHandle(identity, scope, image.cursor(), List.copyOf(records), bytes,
              expiresAt));
          _retainedBytes.addAndGet(bytes - _limits.maxSnapshotBytes());
          published = true;
        }
        return MetadataRpc.OpenSnapshotResult.newBuilder().setStatus(MetadataRpc.ResultStatus.OK)
            .setSnapshotHandle(token).setCursor(MetadataRpcCodec.cursor(image.cursor())).setRecordCount(records.size())
            .build();
      } finally {
        if (!published) {
          _snapshotSlots.release();
          if (reserved) {
            _retainedBytes.addAndGet(-_limits.maxSnapshotBytes());
          }
        }
      }
    });
  }

  @Override
  public void getSnapshotPage(MetadataRpc.SnapshotPageRequest request,
      StreamObserver<MetadataRpc.SnapshotPage> observer) {
    respond("GetSnapshotPage", request, observer, identity -> {
      int maxRecords = pageRecords(request.getMaxRecords());
      int maxBytes = pageBytes(request.getMaxBytes());
      SnapshotHandle handle = handle(request.getSnapshotHandle(), identity);
      if (handle == null) {
        return expiredPage();
      }
      if (expired(handle._scope, handle._cursor)) {
        release(request.getSnapshotHandle(), handle);
        return expiredPage();
      }
      int offset = request.getOffset();
      if (offset < 0 || offset > handle._records.size()) {
        throw new IllegalArgumentException("Invalid snapshot page offset");
      }
      MetadataRpc.SnapshotPage.Builder page = MetadataRpc.SnapshotPage.newBuilder()
          .setStatus(MetadataRpc.ResultStatus.OK).setCursor(MetadataRpcCodec.cursor(handle._cursor));
      int bytes = page.build().getSerializedSize() + 32;
      int next = offset;
      while (next < handle._records.size() && next - offset < maxRecords) {
        MetadataRpc.CatalogRecord record = handle._records.get(next);
        int recordBytes = CodedOutputStream.computeMessageSize(2, record);
        if ((long) bytes + recordBytes > maxBytes) {
          break;
        }
        page.addRecords(record);
        bytes += recordBytes;
        next++;
      }
      if (next == offset && next < handle._records.size()) {
        return MetadataRpc.SnapshotPage.newBuilder().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
            .setCursor(MetadataRpcCodec.cursor(handle._cursor)).setNextOffset(offset).build();
      }
      // Validate the original image cursor again after materialization, including the final handoff to the feed.
      if (expired(handle._scope, handle._cursor) || handle._expiresAt - System.nanoTime() <= 0) {
        release(request.getSnapshotHandle(), handle);
        return expiredPage();
      }
      return page.setNextOffset(next).setComplete(next == handle._records.size()).build();
    });
  }

  @Override
  public void getChanges(MetadataRpc.ChangesRequest request, StreamObserver<MetadataRpc.ChangePage> observer) {
    respond("GetChanges", request, observer, identity -> {
      if (!request.hasCursor() || request.getMaxTransactions() < 1
          || request.getMaxTransactions() > MetadataProtocol.MAX_CHANGE_TRANSACTIONS) {
        throw new IllegalArgumentException("Invalid change request");
      }
      FeedScope scope = scope(request.getKindsList(), identity);
      FeedCursor cursor = MetadataRpcCodec.cursor(request.getCursor());
      if (!scope.scopeId().equals(cursor.scopeId())) {
        throw Status.PERMISSION_DENIED.withDescription("Metadata cursor scope differs from current authorization")
            .asRuntimeException();
      }
      int maxBytes = pageBytes(request.getMaxBytes());
      ChangePage page = _access.changes(scope, cursor, request.getMaxTransactions(),
          pageRecords(request.getMaxRecords()), maxBytes, remainingMillis());
      MetadataRpc.ChangePage.Builder response = MetadataRpc.ChangePage.newBuilder()
          .setStatus(MetadataRpc.ResultStatus.valueOf(page.status().name()))
          .setFrom(MetadataRpcCodec.cursor(page.from()))
          .setNext(MetadataRpcCodec.cursor(page.next())).setRetentionFloor(page.retentionFloor())
          .setHasMore(page.hasMore());
      for (ChangeTransaction transaction : page.transactions()) {
        MetadataRpc.ChangeTransaction.Builder encoded = MetadataRpc.ChangeTransaction.newBuilder()
            .setPreviousIndex(transaction.previousIndex()).setIndex(transaction.index());
        for (MetadataProtocol.CatalogRecord record : transaction.records()) {
          encoded.addRecords(MetadataRpcCodec.record(record));
        }
        response.addTransactions(encoded);
      }
      MetadataRpc.ChangePage result = response.build();
      if (result.getSerializedSize() > maxBytes) {
        return response.clearTransactions().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
            .setNext(MetadataRpcCodec.cursor(cursor)).setHasMore(true).build();
      }
      return result;
    });
  }

  @Override
  public void closeSnapshot(MetadataRpc.CloseSnapshotRequest request, StreamObserver<MetadataRpc.Empty> observer) {
    respond("CloseSnapshot", request, observer, identity -> {
      SnapshotHandle handle = handle(request.getSnapshotHandle(), identity);
      if (handle != null) {
        release(request.getSnapshotHandle(), handle);
      }
      return MetadataRpc.Empty.getDefaultInstance();
    });
  }

  private boolean expired(FeedScope scope, FeedCursor cursor)
      throws IOException {
    ChangePage page = _access.changes(scope, cursor, 1, MetadataProtocol.MAX_CHANGE_RECORDS,
        MetadataProtocol.MAX_CHANGE_BYTES, remainingMillis());
    if (page.status() == MetadataProtocol.Status.CURSOR_EXPIRED
        || page.status() == MetadataProtocol.Status.HISTORY_MISMATCH || page.retentionFloor() > cursor.index()) {
      return true;
    }
    if (page.status() != MetadataProtocol.Status.OK && page.status() != MetadataProtocol.Status.RESPONSE_TOO_LARGE) {
      throw Status.UNAVAILABLE.withDescription("Cannot certify snapshot change retention").asRuntimeException();
    }
    return false;
  }

  private SnapshotHandle handle(String token, ControllerIdentity identity) {
    SnapshotHandle handle = _snapshots.get(token);
    if (handle != null && !handle._identity.equals(identity)) {
      throw Status.PERMISSION_DENIED.withDescription("Snapshot belongs to another authorization scope")
          .asRuntimeException();
    }
    if (handle != null && handle._expiresAt - System.nanoTime() <= 0) {
      release(token, handle);
      return null;
    }
    return handle;
  }

  private void reserveBytes() {
    while (true) {
      long current = _retainedBytes.get();
      if (current + _limits.maxSnapshotBytes() > _limits.maxRetainedBytes()) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Snapshot memory budget reached").asRuntimeException();
      }
      if (_retainedBytes.compareAndSet(current, current + _limits.maxSnapshotBytes())) {
        return;
      }
    }
  }

  private void release(String token, SnapshotHandle handle) {
    if (_snapshots.remove(token, handle)) {
      _retainedBytes.addAndGet(-handle._bytes);
      _snapshotSlots.release();
    }
  }

  private void expireSnapshots() {
    long now = System.nanoTime();
    _snapshots.forEach((token, handle) -> {
      if (handle._expiresAt - now <= 0) {
        release(token, handle);
      }
    });
  }

  private FeedScope scope(List<MetadataRpc.RecordKind> kinds, ControllerIdentity identity) {
    EnumSet<RecordKind> selected = EnumSet.noneOf(RecordKind.class);
    kinds.forEach(kind -> selected.add(MetadataRpcCodec.kind(kind)));
    // Length prefixes make the scope unambiguous even if IDs contain punctuation.
    String scope = identity.clientId().length() + ":" + identity.clientId() + identity.accessScopeId();
    return new FeedScope(selected, scope);
  }

  private int pageRecords(int records) {
    if (records < 1 || records > _limits.maxPageRecords()) {
      throw new IllegalArgumentException("Invalid page record limit");
    }
    return records;
  }

  private int pageBytes(int bytes) {
    if (bytes < 1024 || bytes > _limits.maxPageBytes()) {
      throw new IllegalArgumentException("Invalid page byte limit");
    }
    return bytes;
  }

  private long remainingMillis() {
    if (Context.current().isCancelled()) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Metadata request deadline expired").asRuntimeException();
    }
    long remaining = Context.current().getDeadline() == null ? _limits.maxRpcMillis()
        : Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    if (remaining <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Metadata request deadline expired").asRuntimeException();
    }
    return Math.min(remaining, _limits.maxRpcMillis());
  }

  private ControllerIdentity authorize() {
    String fingerprint = MetadataGrpcServer.PEER_FINGERPRINT.get();
    ControllerIdentity identity = fingerprint == null ? null : _policy.authorize(fingerprint);
    if (identity == null) {
      throw Status.PERMISSION_DENIED.withDescription("Controller metadata access is not authorized")
          .asRuntimeException();
    }
    return identity;
  }

  private void reauthorize(ControllerIdentity identity) {
    if (!identity.equals(authorize())) {
      throw Status.PERMISSION_DENIED.withDescription("Metadata authorization changed during request")
          .asRuntimeException();
    }
  }

  private <T extends Message> void respond(String operation, Message request, StreamObserver<T> observer,
      Handler<T> handler) {
    ControllerIdentity identity = null;
    String outcome = "UNAVAILABLE";
    boolean acquired = false;
    try {
      identity = authorize();
      remainingMillis();
      if (_closed) {
        throw Status.UNAVAILABLE.withDescription("Metadata service closed").asRuntimeException();
      }
      if (!_calls.tryAcquire()) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Metadata concurrency limit reached").asRuntimeException();
      }
      acquired = true;
      MetadataRpcCodec.rejectUnknown(request);
      expireSnapshots();
      T response = handler.handle(identity);
      remainingMillis();
      reauthorize(identity);
      FieldDescriptor status = response.getDescriptorForType().findFieldByName("status");
      outcome = status == null ? "OK" : response.getField(status).toString();
      observer.onNext(response);
      observer.onCompleted();
    } catch (StatusRuntimeException e) {
      outcome = e.getStatus().getCode().name();
      observer.onError(e);
    } catch (IllegalArgumentException e) {
      outcome = "INVALID_ARGUMENT";
      observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid metadata request").asRuntimeException());
    } catch (IOException e) {
      outcome = "UNAVAILABLE";
      observer.onError(Status.UNAVAILABLE.withDescription("Metadata unavailable; a write outcome may be unknown")
          .asRuntimeException());
    } catch (RuntimeException e) {
      outcome = "INTERNAL";
      observer.onError(Status.INTERNAL.withDescription("Metadata request failed").asRuntimeException());
    } finally {
      if (acquired) {
        _calls.release();
      }
      audit(_audit, identity == null ? "" : identity.clientId(), operation, outcome);
    }
  }

  static void audit(MetadataAudit audit, String clientId, String operation, String outcome) {
    try {
      audit.record(clientId, operation, outcome);
    } catch (RuntimeException e) {
      LOGGER.error("Metadata audit sink failed");
    }
  }

  private static MetadataRpc.SnapshotPage expiredPage() {
    return MetadataRpc.SnapshotPage.newBuilder().setStatus(MetadataRpc.ResultStatus.CURSOR_EXPIRED).build();
  }

  @Override
  public void close() {
    synchronized (_lifecycleLock) {
      _closed = true;
      _snapshots.forEach(this::release);
    }
  }

  private interface Handler<T> {
    T handle(ControllerIdentity identity) throws IOException;
  }

  /// Immutable handle, scoped to one authenticated principal and one authorization generation.
  private record SnapshotHandle(ControllerIdentity _identity, FeedScope _scope, FeedCursor _cursor,
                                List<MetadataRpc.CatalogRecord> _records, long _bytes, long _expiresAt) {
  }
}
