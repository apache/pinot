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
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.ParticipantMembershipServiceGrpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// Authenticated own-participant membership access. Authorization is independent from controller catalog access;
/// requests cannot name another participant, choose capabilities or encode internal clock/control commands.
final class MembershipGrpcService extends ParticipantMembershipServiceGrpc.ParticipantMembershipServiceImplBase {
  static final int MAX_MESSAGE_BYTES = 64 * 1024;
  private final MembershipAccess _access;
  private final ParticipantAccessPolicy _policy;
  private final MetadataAudit _audit;
  private final long _maxRpcMillis;
  private final Semaphore _calls = new Semaphore(2);

  MembershipGrpcService(MembershipAccess access, ParticipantAccessPolicy policy, MetadataAudit audit,
      long maxRpcMillis) {
    _access = Objects.requireNonNull(access);
    _policy = Objects.requireNonNull(policy);
    _audit = Objects.requireNonNull(audit);
    _maxRpcMillis = maxRpcMillis;
  }

  @Override
  public void execute(MetadataRpc.MembershipCommandRequest request,
      StreamObserver<MetadataRpc.MembershipResponse> observer) {
    respond("MembershipExecute", request, observer,
        identity -> _access.execute(MembershipRpcCodec.command(request, identity), remainingMillis()));
  }

  @Override
  public void read(MetadataRpc.MembershipReadRequest request, StreamObserver<MetadataRpc.MembershipResponse> observer) {
    respond("MembershipRead", request, observer,
        identity -> _access.query(MembershipRpcCodec.query(request, identity), remainingMillis()));
  }

  private void respond(String operation, Message request, StreamObserver<MetadataRpc.MembershipResponse> observer,
      Handler handler) {
    ParticipantIdentity identity = null;
    String outcome = "UNAVAILABLE";
    boolean acquired = false;
    try {
      identity = authorize();
      remainingMillis();
      if (request.getSerializedSize() > MAX_MESSAGE_BYTES || !_calls.tryAcquire()) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Membership request exceeds admission bounds")
            .asRuntimeException();
      }
      acquired = true;
      MetadataRpcCodec.rejectUnknown(request);
      MetadataRpc.MembershipResponse response = MembershipRpcCodec.response(handler.handle(identity));
      remainingMillis();
      if (!identity.equals(authorize())) {
        throw Status.PERMISSION_DENIED.withDescription("Participant authorization changed during request")
            .asRuntimeException();
      }
      if (response.getSerializedSize() > MAX_MESSAGE_BYTES) {
        throw Status.RESOURCE_EXHAUSTED.withDescription("Membership response exceeds transport bounds")
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
      observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid participant request").asRuntimeException());
    } catch (IOException e) {
      outcome = "UNAVAILABLE";
      observer.onError(Status.UNAVAILABLE.withDescription("Membership unavailable; write outcome may be unknown")
          .asRuntimeException());
    } catch (RuntimeException e) {
      outcome = "INTERNAL";
      observer.onError(Status.INTERNAL.withDescription("Membership request failed").asRuntimeException());
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
    if (identity == null) {
      throw Status.PERMISSION_DENIED.withDescription("Participant membership access is not authorized")
          .asRuntimeException();
    }
    return identity;
  }

  private long remainingMillis() {
    Context context = Context.current();
    long remaining = context.getDeadline() == null ? _maxRpcMillis
        : context.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    if (context.isCancelled() || remaining <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Membership request deadline expired").asRuntimeException();
    }
    return Math.min(remaining, _maxRpcMillis);
  }

  private interface Handler {
    Response handle(ParticipantIdentity identity) throws IOException;
  }
}
