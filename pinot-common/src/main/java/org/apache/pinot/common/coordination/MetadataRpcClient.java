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
package org.apache.pinot.common.coordination;

import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataServiceGrpc;
import org.apache.pinot.coordination.ratis.rpc.MinionTaskServiceGrpc;
import org.apache.pinot.coordination.ratis.rpc.ParticipantMembershipServiceGrpc;
import org.apache.pinot.coordination.ratis.rpc.ServerInstallationServiceGrpc;


/// Bounded mutually authenticated metadata client. It never invents a new transaction identity or retries an unknown
/// write outcome; callers resolve/retry the same client epoch, stream and sequence. Server hostname verification stays
/// enabled and there is no authority override or trust-all path. Instances own one channel and are thread-safe; use
/// a separate instance for membership to isolate it from bulk HTTP/2 stream admission. This client depends only on
/// the closed protobuf wire vocabulary, not on a coordination backend or its metadata model.
public class MetadataRpcClient implements AutoCloseable {
  public static final int MAX_COMMAND_BYTES = 1024 * 1024;
  public static final int MAX_RESPONSE_BYTES = 2 * MAX_COMMAND_BYTES;
  public static final int MAX_MEMBERSHIP_BYTES = 64 * 1024;
  public static final int MAX_INSTALLATION_REQUEST_BYTES = 64 * 1024;
  public static final int MAX_TASK_REQUEST_BYTES = 64 * 1024;
  private final ManagedChannel _channel;
  private final MetadataServiceGrpc.MetadataServiceBlockingStub _stub;
  private final ParticipantMembershipServiceGrpc.ParticipantMembershipServiceBlockingStub _membership;
  private final ServerInstallationServiceGrpc.ServerInstallationServiceBlockingStub _installation;
  private final MinionTaskServiceGrpc.MinionTaskServiceBlockingStub _task;
  private final long _timeoutMillis;

  public MetadataRpcClient(String host, int port, Path certificateChain, Path privateKey, Path trustCertificates,
      long timeoutMillis)
      throws IOException {
    if (timeoutMillis < 1 || timeoutMillis > 30_000) {
      throw new IllegalArgumentException("Metadata RPC timeout must be between 1 and 30000 milliseconds");
    }
    _timeoutMillis = timeoutMillis;
    _channel = NettyChannelBuilder.forAddress(host, port)
        .sslContext(GrpcSslContexts.forClient().keyManager(certificateChain.toFile(), privateKey.toFile())
            .trustManager(trustCertificates.toFile()).build()).maxInboundMessageSize(MAX_RESPONSE_BYTES)
        .disableRetry().build();
    _stub = MetadataServiceGrpc.newBlockingStub(_channel);
    _membership = ParticipantMembershipServiceGrpc.newBlockingStub(_channel);
    _installation = ServerInstallationServiceGrpc.newBlockingStub(_channel);
    _task = MinionTaskServiceGrpc.newBlockingStub(_channel);
  }

  public MetadataRpc.CommandResult taskExecute(MetadataRpc.TaskCommandRequest request, long timeoutMillis) {
    return taskCall(request, timeoutMillis, stub -> stub.execute(request));
  }

  public MetadataRpc.TaskWorkResult taskWork(MetadataRpc.TaskWorkRequest request, long timeoutMillis) {
    return taskCall(request, timeoutMillis, stub -> stub.work(request));
  }

  public MetadataRpc.CommandResult taskOutcome(MetadataRpc.TaskOutcomeRequest request, long timeoutMillis) {
    return taskCall(request, timeoutMillis, stub -> stub.outcome(request));
  }

  private <T> T taskCall(Message request, long timeoutMillis,
      Function<MinionTaskServiceGrpc.MinionTaskServiceBlockingStub, T> operation) {
    if (request == null || request.getSerializedSize() > MAX_TASK_REQUEST_BYTES) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Task request exceeds transport byte limit")
          .asRuntimeException();
    }
    if (timeoutMillis <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Task request deadline expired").asRuntimeException();
    }
    return operation.apply(_task.withDeadlineAfter(Math.min(_timeoutMillis, timeoutMillis), TimeUnit.MILLISECONDS));
  }

  public MetadataRpc.CommandResult installationExecute(MetadataRpc.InstallationCommandRequest request,
      long timeoutMillis) {
    return installationCall(request, timeoutMillis, stub -> stub.execute(request));
  }

  public MetadataRpc.InstallationWorkResult installationLookup(MetadataRpc.InstallationLookupRequest request,
      long timeoutMillis) {
    return installationCall(request, timeoutMillis, stub -> stub.lookup(request));
  }

  public MetadataRpc.CommandResult installationOutcome(MetadataRpc.InstallationOutcomeRequest request,
      long timeoutMillis) {
    return installationCall(request, timeoutMillis, stub -> stub.outcome(request));
  }

  /// Returns the complete bounded metadata projection for this server registration, including before readiness.
  public MetadataRpc.InstallationSnapshot installationSnapshot(MetadataRpc.InstallationSnapshotRequest request,
      long timeoutMillis) {
    return installationCall(request, timeoutMillis, stub -> stub.snapshot(request));
  }

  private <T> T installationCall(Message request, long timeoutMillis,
      Function<ServerInstallationServiceGrpc.ServerInstallationServiceBlockingStub, T> operation) {
    if (request == null || request.getSerializedSize() > MAX_INSTALLATION_REQUEST_BYTES) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Installation request exceeds transport byte limit")
          .asRuntimeException();
    }
    if (timeoutMillis <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Installation request deadline expired").asRuntimeException();
    }
    return operation.apply(_installation.withDeadlineAfter(Math.min(_timeoutMillis, timeoutMillis),
        TimeUnit.MILLISECONDS));
  }

  public MetadataRpc.MembershipResponse membershipExecute(MetadataRpc.MembershipCommandRequest request) {
    return membershipExecute(request, _timeoutMillis);
  }

  public MetadataRpc.MembershipResponse membershipExecute(MetadataRpc.MembershipCommandRequest request,
      long timeoutMillis) {
    return membershipCall(request, timeoutMillis, stub -> stub.execute(request));
  }

  public MetadataRpc.MembershipResponse membershipRead(MetadataRpc.MembershipReadRequest request) {
    return membershipRead(request, _timeoutMillis);
  }

  public MetadataRpc.MembershipResponse membershipRead(MetadataRpc.MembershipReadRequest request, long timeoutMillis) {
    return membershipCall(request, timeoutMillis, stub -> stub.read(request));
  }

  private <T> T membershipCall(Message request, long timeoutMillis,
      Function<ParticipantMembershipServiceGrpc.ParticipantMembershipServiceBlockingStub, T> operation) {
    if (request == null || request.getSerializedSize() > MAX_MEMBERSHIP_BYTES) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Membership request exceeds the transport byte limit")
          .asRuntimeException();
    }
    if (timeoutMillis <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Membership request deadline expired").asRuntimeException();
    }
    return operation.apply(_membership.withDeadlineAfter(Math.min(_timeoutMillis, timeoutMillis),
        TimeUnit.MILLISECONDS));
  }

  public MetadataRpc.CommandResult execute(MetadataRpc.CommandRequest request) {
    return execute(request, _timeoutMillis);
  }

  public MetadataRpc.CommandResult execute(MetadataRpc.CommandRequest request, long timeoutMillis) {
    return call(request, timeoutMillis, stub -> stub.execute(request));
  }

  public MetadataRpc.CommandResult read(MetadataRpc.ReadRequest request) {
    return read(request, _timeoutMillis);
  }

  public MetadataRpc.CommandResult read(MetadataRpc.ReadRequest request, long timeoutMillis) {
    return call(request, timeoutMillis, stub -> stub.read(request));
  }

  public MetadataRpc.OpenSnapshotResult openSnapshot(MetadataRpc.OpenSnapshotRequest request) {
    return openSnapshot(request, _timeoutMillis);
  }

  public MetadataRpc.OpenSnapshotResult openSnapshot(MetadataRpc.OpenSnapshotRequest request, long timeoutMillis) {
    return call(request, timeoutMillis, stub -> stub.openSnapshot(request));
  }

  public MetadataRpc.SnapshotPage page(MetadataRpc.SnapshotPageRequest request) {
    return page(request, _timeoutMillis);
  }

  public MetadataRpc.SnapshotPage page(MetadataRpc.SnapshotPageRequest request, long timeoutMillis) {
    return call(request, timeoutMillis, stub -> stub.getSnapshotPage(request));
  }

  public MetadataRpc.ChangePage changes(MetadataRpc.ChangesRequest request) {
    return changes(request, _timeoutMillis);
  }

  public MetadataRpc.ChangePage changes(MetadataRpc.ChangesRequest request, long timeoutMillis) {
    return call(request, timeoutMillis, stub -> stub.getChanges(request));
  }

  public void closeSnapshot(String handle) {
    closeSnapshot(handle, _timeoutMillis);
  }

  public void closeSnapshot(String handle, long timeoutMillis) {
    MetadataRpc.CloseSnapshotRequest request = MetadataRpc.CloseSnapshotRequest.newBuilder().setSnapshotHandle(handle)
        .build();
    call(request, timeoutMillis, stub -> stub.closeSnapshot(request));
  }

  private <T> T call(Message request, long timeoutMillis,
      Function<MetadataServiceGrpc.MetadataServiceBlockingStub, T> operation) {
    if (request == null || request.getSerializedSize() > MAX_COMMAND_BYTES) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Metadata request exceeds transport byte limit")
          .asRuntimeException();
    }
    if (timeoutMillis <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Metadata request deadline expired").asRuntimeException();
    }
    return operation.apply(_stub.withDeadlineAfter(Math.min(_timeoutMillis, timeoutMillis), TimeUnit.MILLISECONDS));
  }

  @Override
  public void close() {
    _channel.shutdownNow();
    try {
      _channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
