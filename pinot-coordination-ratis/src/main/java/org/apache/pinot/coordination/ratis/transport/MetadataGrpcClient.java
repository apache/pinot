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
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataServiceGrpc;
import org.apache.pinot.coordination.ratis.rpc.ParticipantMembershipServiceGrpc;


/// Bounded mutually authenticated metadata client. It never invents a new transaction identity or retries an unknown
/// write outcome; callers resolve/retry the same client epoch, stream and sequence. Server hostname verification stays
/// enabled and there is no authority override or trust-all path.
public final class MetadataGrpcClient implements AutoCloseable {
  private final ManagedChannel _channel;
  private final MetadataServiceGrpc.MetadataServiceBlockingStub _stub;
  private final ParticipantMembershipServiceGrpc.ParticipantMembershipServiceBlockingStub _membership;
  private final long _timeoutMillis;

  public MetadataGrpcClient(String host, int port, Path certificateChain, Path privateKey, Path trustCertificates,
      long timeoutMillis)
      throws IOException {
    if (timeoutMillis < 1 || timeoutMillis > 30_000) {
      throw new IllegalArgumentException("Metadata RPC timeout must be between 1 and 30000 milliseconds");
    }
    _timeoutMillis = timeoutMillis;
    _channel = NettyChannelBuilder.forAddress(host, port)
        .sslContext(GrpcSslContexts.forClient().keyManager(certificateChain.toFile(), privateKey.toFile())
            .trustManager(trustCertificates.toFile()).build()).maxInboundMessageSize(MetadataProtocol.MAX_CHANGE_BYTES)
        .disableRetry().build();
    _stub = MetadataServiceGrpc.newBlockingStub(_channel);
    _membership = ParticipantMembershipServiceGrpc.newBlockingStub(_channel);
  }

  public MetadataRpc.MembershipResponse membershipExecute(MetadataRpc.MembershipCommandRequest request) {
    checkMembershipRequest(request);
    return _membership.withDeadlineAfter(_timeoutMillis, TimeUnit.MILLISECONDS).execute(request);
  }

  public MetadataRpc.MembershipResponse membershipRead(MetadataRpc.MembershipReadRequest request) {
    checkMembershipRequest(request);
    return _membership.withDeadlineAfter(_timeoutMillis, TimeUnit.MILLISECONDS).read(request);
  }

  public static MembershipProtocol.Response decodeMembershipResponse(MetadataRpc.MembershipResponse response)
      throws IOException {
    if (response == null || response.getSerializedSize() > MembershipGrpcService.MAX_MESSAGE_BYTES) {
      throw new IOException("Membership response exceeds the transport byte limit");
    }
    try {
      return MembershipRpcCodec.response(response);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid membership response", e);
    }
  }

  private static void checkMembershipRequest(Message request) {
    if (request == null || request.getSerializedSize() > MembershipGrpcService.MAX_MESSAGE_BYTES) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Membership request exceeds the transport byte limit")
          .asRuntimeException();
    }
  }

  public MetadataRpc.CommandResult execute(MetadataRpc.CommandRequest request) {
    return call(request, stub -> stub.execute(request));
  }

  public MetadataRpc.CommandResult read(MetadataRpc.ReadRequest request) {
    return call(request, stub -> stub.read(request));
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

  private <T> T call(Message request, Function<MetadataServiceGrpc.MetadataServiceBlockingStub, T> operation) {
    return call(request, _timeoutMillis, operation);
  }

  private <T> T call(Message request, long timeoutMillis,
      Function<MetadataServiceGrpc.MetadataServiceBlockingStub, T> operation) {
    if (request == null || request.getSerializedSize() > MetadataProtocol.MAX_COMMAND_BYTES) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Metadata request exceeds transport byte limit")
          .asRuntimeException();
    }
    if (timeoutMillis <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Metadata request deadline expired").asRuntimeException();
    }
    return operation.apply(_stub.withDeadlineAfter(Math.min(_timeoutMillis, timeoutMillis), TimeUnit.MILLISECONDS));
  }

  public static MetadataProtocol.CatalogRecord decodeRecord(MetadataRpc.CatalogRecord record)
      throws IOException {
    validateSize(record);
    try {
      return MetadataRpcCodec.record(record);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata record response", e);
    }
  }

  public static MetadataProtocol.FeedCursor decodeCursor(MetadataRpc.Cursor cursor)
      throws IOException {
    validateSize(cursor);
    try {
      return MetadataRpcCodec.cursor(cursor);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata cursor response", e);
    }
  }

  public static MetadataProtocol.ChangePage decodeChanges(MetadataRpc.ChangePage page)
      throws IOException {
    validateSize(page);
    try {
      return MetadataRpcCodec.changes(page);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata change response", e);
    }
  }

  private static void validateSize(Message response)
      throws IOException {
    if (response == null || response.getSerializedSize() > MetadataProtocol.MAX_CHANGE_BYTES) {
      throw new IOException("Metadata response exceeds the transport byte limit");
    }
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
