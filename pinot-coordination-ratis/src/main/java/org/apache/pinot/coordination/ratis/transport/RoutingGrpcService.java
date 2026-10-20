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

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineRoutingProtocol;
import org.apache.pinot.coordination.ratis.rpc.BrokerRoutingServiceGrpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// BROKER-only routing reads on an independently bounded lane. Certificate authorization is rechecked at delivery;
/// committed tenant policy and exact participant sessions are checked in the coherent catalog projection.
final class RoutingGrpcService extends BrokerRoutingServiceGrpc.BrokerRoutingServiceImplBase {
  static final int MAX_REQUEST_BYTES = 64 * 1024;
  private final RoutingAccess _routing;
  private final ParticipantAccessPolicy _policy;
  private final MetadataAudit _audit;
  private final long _maxRpcMillis;
  private final Semaphore _calls = new Semaphore(2);

  RoutingGrpcService(RoutingAccess routing, ParticipantAccessPolicy policy, MetadataAudit audit, long maxRpcMillis) {
    _routing = Objects.requireNonNull(routing);
    _policy = Objects.requireNonNull(policy);
    _audit = Objects.requireNonNull(audit);
    _maxRpcMillis = maxRpcMillis;
  }

  @Override
  public void snapshot(MetadataRpc.RoutingSnapshotRequest request,
      StreamObserver<MetadataRpc.RoutingSnapshot> observer) {
    ParticipantIdentity identity = null;
    String outcome = "UNAVAILABLE";
    boolean acquired = false;
    try {
      identity = authorize();
      remainingMillis();
      if (request.getSerializedSize() > MAX_REQUEST_BYTES || !_calls.tryAcquire()) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Routing request exceeds admission bounds")
            .asRuntimeException();
      }
      acquired = true;
      var broker = RoutingRpcCodec.broker(request, identity);
      var snapshot = _routing.routingSnapshot(broker, request.getTableName(), request.getMaxRecords(),
          request.getMaxBytes(), remainingMillis());
      if (snapshot.status() == MetadataProtocol.Status.OK) {
        if (snapshot.participant() == null || !broker.equals(snapshot.participant().participant())
            || snapshot.control() == null || !broker.historyId().equals(snapshot.revision().historyId())
            || request.getTableName().isEmpty() != (snapshot.table() == null)
            || snapshot.table() != null && !request.getTableName().equals(snapshot.table().tableName())) {
          throw Status.INTERNAL.withDescription("Routing projection identity differs").asRuntimeException();
        }
      } else if (snapshot.participant() != null || snapshot.control() != null || snapshot.table() != null
          || !snapshot.tables().isEmpty()) {
        throw Status.INTERNAL.withDescription("Failed routing projection contains data").asRuntimeException();
      }
      MetadataRpc.RoutingSnapshot response = RoutingRpcCodec.snapshot(snapshot);
      if (response.getSerializedSize() > request.getMaxBytes()
          || response.getSerializedSize() > OfflineRoutingProtocol.MAX_SNAPSHOT_BYTES) {
        response = MetadataRpc.RoutingSnapshot.newBuilder().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
            .setRevision(response.getRevision()).setDetail("Routing projection exceeds its wire byte bound").build();
      }
      remainingMillis();
      if (!identity.equals(authorize())) {
        throw Status.PERMISSION_DENIED.withDescription("Broker authorization changed during routing request")
            .asRuntimeException();
      }
      outcome = response.getStatus().name();
      observer.onNext(response);
      observer.onCompleted();
    } catch (StatusRuntimeException e) {
      outcome = e.getStatus().getCode().name();
      observer.onError(e);
    } catch (IllegalArgumentException e) {
      outcome = "INVALID_ARGUMENT";
      observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid routing request").asRuntimeException());
    } catch (IOException e) {
      outcome = "UNAVAILABLE";
      observer.onError(Status.UNAVAILABLE.withDescription("Routing projection is unavailable").asRuntimeException());
    } catch (RuntimeException e) {
      outcome = "INTERNAL";
      observer.onError(Status.INTERNAL.withDescription("Routing projection failed").asRuntimeException());
    } finally {
      if (acquired) {
        _calls.release();
      }
      MetadataGrpcService.audit(_audit, identity == null ? "" : identity.participantId(), "RoutingSnapshot", outcome);
    }
  }

  private ParticipantIdentity authorize() {
    String fingerprint = MetadataGrpcServer.PEER_FINGERPRINT.get();
    ParticipantIdentity identity = fingerprint == null ? null : _policy.authorize(fingerprint);
    if (identity == null || identity.role() != Role.BROKER) {
      throw Status.PERMISSION_DENIED.withDescription("Broker routing access is not authorized").asRuntimeException();
    }
    return identity;
  }

  private long remainingMillis() {
    Context context = Context.current();
    long remaining = context.getDeadline() == null ? _maxRpcMillis
        : context.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    if (context.isCancelled() || remaining <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Routing request deadline expired").asRuntimeException();
    }
    return Math.min(remaining, _maxRpcMillis);
  }
}
