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
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.WorkSnapshot;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MinionTaskServiceGrpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// MINION-only task work and durable outcomes. A separate executor and admission budget protect membership
/// renewals from task reads and reports. An RPC timeout never cancels an accepted durable command.
final class TaskGrpcService extends MinionTaskServiceGrpc.MinionTaskServiceImplBase {
  static final int MAX_REQUEST_BYTES = 64 * 1024;
  private final MetadataAccess _metadata;
  private final TaskAccess _tasks;
  private final ParticipantAccessPolicy _policy;
  private final MetadataAudit _audit;
  private final long _maxRpcMillis;
  private final Semaphore _calls = new Semaphore(2);

  TaskGrpcService(MetadataAccess metadata, TaskAccess tasks, ParticipantAccessPolicy policy,
      MetadataAudit audit, long maxRpcMillis) {
    _metadata = Objects.requireNonNull(metadata);
    _tasks = Objects.requireNonNull(tasks);
    _policy = Objects.requireNonNull(policy);
    _audit = Objects.requireNonNull(audit);
    _maxRpcMillis = maxRpcMillis;
  }

  @Override
  public void execute(MetadataRpc.TaskCommandRequest request,
      StreamObserver<MetadataRpc.CommandResult> observer) {
    respond("TaskExecute", request, observer, identity -> MetadataRpcCodec.result(
        _metadata.execute(TaskRpcCodec.command(request, identity), remainingMillis())));
  }

  @Override
  public void outcome(MetadataRpc.TaskOutcomeRequest request,
      StreamObserver<MetadataRpc.CommandResult> observer) {
    respond("TaskOutcome", request, observer, identity -> MetadataRpcCodec.result(
        _metadata.query(TaskRpcCodec.outcome(request, identity), remainingMillis())));
  }

  @Override
  public void work(MetadataRpc.TaskWorkRequest request, StreamObserver<MetadataRpc.TaskWorkResult> observer) {
    respond("TaskWork", request, observer, identity -> {
      ParticipantToken minion = TaskRpcCodec.work(request, identity);
      WorkSnapshot snapshot = _tasks.taskSnapshot(minion, request.getMaxRecords(), request.getMaxBytes(),
          remainingMillis());
      if (snapshot.status() == MetadataProtocol.Status.OK && (snapshot.participant() == null
          || !snapshot.participant().participant().equals(minion) || snapshot.control() == null)) {
        throw Status.INTERNAL.withDescription("Task work identity differs").asRuntimeException();
      }
      MetadataRpc.TaskWorkResult response = TaskRpcCodec.work(snapshot);
      if (response.getSerializedSize() > request.getMaxBytes()) {
        return MetadataRpc.TaskWorkResult.newBuilder().setStatus(MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE)
            .setRevision(response.getRevision()).setDetail("Task projection exceeds its wire byte bound").build();
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
        throw Status.RESOURCE_EXHAUSTED.withDescription("Task request exceeds admission bounds")
            .asRuntimeException();
      }
      acquired = true;
      MetadataRpcCodec.rejectUnknown(request);
      T response = handler.handle(identity);
      remainingMillis();
      if (!identity.equals(authorize())) {
        throw Status.PERMISSION_DENIED.withDescription("Minion authorization changed during task request")
            .asRuntimeException();
      }
      if (response.getSerializedSize() > MetadataProtocol.MAX_CHANGE_BYTES) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Task response exceeds transport bounds")
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
      observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid task request").asRuntimeException());
    } catch (IOException e) {
      outcome = "UNAVAILABLE";
      observer.onError(Status.UNAVAILABLE.withDescription("Task unavailable; write outcome may be unknown")
          .asRuntimeException());
    } catch (RuntimeException e) {
      outcome = "INTERNAL";
      observer.onError(Status.INTERNAL.withDescription("Task request failed").asRuntimeException());
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
    if (identity == null || identity.role() != Role.MINION
        || !identity.capabilities().contains(Capability.MINION_TASK)) {
      throw Status.PERMISSION_DENIED.withDescription("Minion task access is not authorized")
          .asRuntimeException();
    }
    return identity;
  }

  private long remainingMillis() {
    Context context = Context.current();
    long remaining = context.getDeadline() == null ? _maxRpcMillis
        : context.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    if (context.isCancelled() || remaining <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Task request deadline expired").asRuntimeException();
    }
    return Math.min(remaining, _maxRpcMillis);
  }

  private interface Handler<T> {
    T handle(ParticipantIdentity identity) throws IOException;
  }
}
