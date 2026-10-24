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

import com.google.protobuf.Message;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetInstallationWork;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.InstallationSnapshot;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.WorkResult;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.ServerInstallationServiceGrpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// SERVER-only installation work and durable outcomes. The distinct executor and admission budget keep potentially
/// larger work reads away from membership renewals. Timeouts never imply cancellation of an accepted durable report.
final class InstallationGrpcService extends ServerInstallationServiceGrpc.ServerInstallationServiceImplBase {
  static final int MAX_REQUEST_BYTES = 64 * 1024;
  private final MetadataAccess _metadata;
  private final InstallationAccess _installation;
  private final ParticipantAccessPolicy _policy;
  private final MetadataAudit _audit;
  private final long _maxRpcMillis;
  private final Semaphore _calls = new Semaphore(2);

  InstallationGrpcService(MetadataAccess metadata, InstallationAccess installation, ParticipantAccessPolicy policy,
      MetadataAudit audit, long maxRpcMillis) {
    _metadata = Objects.requireNonNull(metadata);
    _installation = Objects.requireNonNull(installation);
    _policy = Objects.requireNonNull(policy);
    _audit = Objects.requireNonNull(audit);
    _maxRpcMillis = maxRpcMillis;
  }

  @Override
  public void execute(MetadataRpc.InstallationCommandRequest request,
      StreamObserver<MetadataRpc.CommandResult> observer) {
    respond("InstallationExecute", request, observer, identity -> MetadataRpcCodec.result(
        _metadata.execute(InstallationRpcCodec.command(request, identity), remainingMillis())));
  }

  @Override
  public void lookup(MetadataRpc.InstallationLookupRequest request,
      StreamObserver<MetadataRpc.InstallationWorkResult> observer) {
    respond("InstallationLookup", request, observer, identity -> {
      GetInstallationWork query = InstallationRpcCodec.lookup(request, identity);
      WorkResult result = _installation.query(query, remainingMillis());
      if (result.work() != null && (!query.server().equals(result.work().target().server())
          || !query.operationId().equals(result.work().operationId()))) {
        throw Status.INTERNAL.withDescription("Installation work identity differs").asRuntimeException();
      }
      return InstallationRpcCodec.work(result);
    });
  }

  @Override
  public void outcome(MetadataRpc.InstallationOutcomeRequest request,
      StreamObserver<MetadataRpc.CommandResult> observer) {
    respond("InstallationOutcome", request, observer, identity -> MetadataRpcCodec.result(
        _metadata.query(InstallationRpcCodec.outcome(request, identity), remainingMillis())));
  }

  @Override
  public void snapshot(MetadataRpc.InstallationSnapshotRequest request,
      StreamObserver<MetadataRpc.InstallationSnapshot> observer) {
    respond("InstallationSnapshot", request, observer, identity -> {
      ParticipantToken server = InstallationRpcCodec.snapshot(request, identity);
      InstallationSnapshot snapshot = _installation.installationSnapshot(server, request.getMaxRecords(),
          request.getMaxBytes(), remainingMillis());
      if (snapshot.status() == MetadataProtocol.Status.OK && (snapshot.participant() == null
          || !snapshot.participant().participant().equals(server) || snapshot.control() == null)) {
        throw Status.INTERNAL.withDescription("Installation snapshot identity differs").asRuntimeException();
      }
      MetadataRpc.InstallationSnapshot response = InstallationRpcCodec.snapshot(snapshot);
      if (response.getSerializedSize() > request.getMaxBytes()) {
        return MetadataRpc.InstallationSnapshot.newBuilder().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
            .setRevision(response.getRevision()).setDetail("Installation snapshot exceeds its wire byte bound")
            .build();
      }
      return response;
    });
  }

  private <T extends Message> void respond(String operation, Message request, StreamObserver<T> observer,
      Handler<T> handler) {
    ParticipantIdentity identity = null;
    String outcome = "UNAVAILABLE";
    boolean acquired = false;
    try {
      identity = authorize();
      remainingMillis();
      if (request.getSerializedSize() > MAX_REQUEST_BYTES || !_calls.tryAcquire()) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Installation request exceeds admission bounds")
            .asRuntimeException();
      }
      acquired = true;
      MetadataRpcCodec.rejectUnknown(request);
      T response = handler.handle(identity);
      remainingMillis();
      if (!identity.equals(authorize())) {
        throw Status.PERMISSION_DENIED.withDescription("Server authorization changed during installation request")
            .asRuntimeException();
      }
      if (response.getSerializedSize() > MetadataProtocol.MAX_CHANGE_BYTES) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Installation response exceeds transport bounds")
            .asRuntimeException();
      }
      outcome = response.getField(response.getDescriptorForType().findFieldByName("status")).toString();
      observer.onNext(response);
      observer.onCompleted();
    } catch (StatusRuntimeException e) {
      outcome = e.getStatus().getCode().name();
      observer.onError(e);
    } catch (IllegalArgumentException e) {
      outcome = "INVALID_ARGUMENT";
      observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid installation request").asRuntimeException());
    } catch (IOException e) {
      outcome = "UNAVAILABLE";
      observer.onError(Status.UNAVAILABLE.withDescription("Installation unavailable; write outcome may be unknown")
          .asRuntimeException());
    } catch (RuntimeException e) {
      outcome = "INTERNAL";
      observer.onError(Status.INTERNAL.withDescription("Installation request failed").asRuntimeException());
    } finally {
      if (acquired) {
        _calls.release();
      }
      MetadataGrpcService.audit(_audit, identity == null ? "" : identity.participantId(), operation, outcome);
    }
  }

  private ParticipantIdentity authorize() {
    String fingerprint = MetadataGrpcServer.PEER_FINGERPRINT.get();
    ParticipantIdentity identity = fingerprint == null ? null : _policy.authorize(fingerprint);
    if (identity == null || identity.role() != Role.SERVER) {
      throw Status.PERMISSION_DENIED.withDescription("Server installation access is not authorized")
          .asRuntimeException();
    }
    return identity;
  }

  private long remainingMillis() {
    Context context = Context.current();
    long remaining = context.getDeadline() == null ? _maxRpcMillis
        : context.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    if (context.isCancelled() || remaining <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Installation request deadline expired").asRuntimeException();
    }
    return Math.min(remaining, _maxRpcMillis);
  }

  private interface Handler<T> {
    T handle(ParticipantIdentity identity) throws IOException;
  }
}
