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
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLSession;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.rpc.ParticipantMembershipServiceGrpc;


/// Mutually authenticated metadata and own-participant endpoint. The certificate trust store authenticates peers;
/// separate current policies authorize full controller metadata and participant membership access. There is no
/// plaintext or trust-all construction path.
public final class MetadataGrpcServer implements AutoCloseable {
  static final Context.Key<String> PEER_FINGERPRINT = Context.key("pinot-metadata-peer-fingerprint");
  private final Server _server;
  private final MetadataGrpcService _service;
  private final ThreadPoolExecutor _executor;
  private final ThreadPoolExecutor _membershipExecutor;
  private final ThreadPoolExecutor _dispatchExecutor;
  private final ScheduledThreadPoolExecutor _deadlines;

  public MetadataGrpcServer(InetSocketAddress address, Path certificateChain, Path privateKey, Path trustCertificates,
      MetadataAccess access, ControllerAccessPolicy policy, MetadataAudit audit, MetadataGrpcService.Limits limits)
      throws IOException {
    this(address, certificateChain, privateKey, trustCertificates, access, policy, null, null, audit, limits);
  }

  public MetadataGrpcServer(InetSocketAddress address, Path certificateChain, Path privateKey, Path trustCertificates,
      MetadataAccess access, ControllerAccessPolicy policy, @Nullable MembershipAccess membershipAccess,
      @Nullable ParticipantAccessPolicy participantPolicy, MetadataAudit audit, MetadataGrpcService.Limits limits)
      throws IOException {
    if ((membershipAccess == null) != (participantPolicy == null)) {
      throw new IllegalArgumentException("Membership backend and participant policy must both be supplied");
    }
    _executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(32),
        Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
    _membershipExecutor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16),
        Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
    // gRPC performs method lookup before callExecutor selection. Keep that short dispatch off both handler pools;
    // its SerializingExecutor switches to the selected pool before starting the application call.
    _dispatchExecutor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(64),
        Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
    _deadlines = new ScheduledThreadPoolExecutor(1);
    _deadlines.setRemoveOnCancelPolicy(true);
    _service = new MetadataGrpcService(access, policy, audit, limits);
    Server server = null;
    try {
      NettyServerBuilder builder = NettyServerBuilder.forAddress(address)
          .sslContext(GrpcSslContexts.forServer(certificateChain.toFile(),
              privateKey.toFile()).trustManager(trustCertificates.toFile()).clientAuth(ClientAuth.REQUIRE).build())
          .executor(_dispatchExecutor).callExecutor(new ServerCallExecutorSupplier() {
            @Override
            public <REQ, RESP> Executor getExecutor(ServerCall<REQ, RESP> call, Metadata headers) {
              return ParticipantMembershipServiceGrpc.SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())
                  ? _membershipExecutor : _executor;
            }
          }).maxInboundMessageSize(MetadataProtocol.MAX_COMMAND_BYTES).maxInboundMetadataSize(8192)
          .maxConcurrentCallsPerConnection(limits.maxConcurrentCalls())
          .addService(ServerInterceptors.intercept(_service, new PeerInterceptor(limits.maxRpcMillis(), audit)));
      if (membershipAccess != null) {
        builder.addService(ServerInterceptors.intercept(
            new MembershipGrpcService(membershipAccess, participantPolicy, audit, limits.maxRpcMillis()),
            new PeerInterceptor(limits.maxRpcMillis(), audit)));
      }
      server = builder.build().start();
      _server = server;
    } catch (IOException | RuntimeException | Error e) {
      if (server != null) {
        server.shutdownNow();
      }
      _executor.shutdownNow();
      _membershipExecutor.shutdownNow();
      _dispatchExecutor.shutdownNow();
      _deadlines.shutdownNow();
      throw e;
    }
  }

  public int port() {
    return _server.getPort();
  }

  @Override
  public void close() {
    _server.shutdownNow();
    _service.close();
    _executor.shutdownNow();
    _membershipExecutor.shutdownNow();
    _dispatchExecutor.shutdownNow();
    _deadlines.shutdownNow();
    try {
      _server.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /// Extracts only authenticated TLS session identity; RPC headers and message fields never select a principal.
  private final class PeerInterceptor implements ServerInterceptor {
    private final long _maxRpcMillis;
    private final MetadataAudit _audit;

    private PeerInterceptor(long maxRpcMillis, MetadataAudit audit) {
      _maxRpcMillis = maxRpcMillis;
      _audit = audit;
    }

    @Override
    public <REQ, RESP> ServerCall.Listener<REQ> interceptCall(ServerCall<REQ, RESP> call, Metadata headers,
        ServerCallHandler<REQ, RESP> next) {
      String fingerprint;
      try {
        SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        if (session == null || !(session.getPeerCertificates()[0] instanceof X509Certificate certificate)) {
          throw new IllegalArgumentException("Authenticated TLS peer required");
        }
        certificate.checkValidity();
        fingerprint = ControllerAccessPolicy.fingerprint(certificate);
      } catch (Exception e) {
        call.close(Status.UNAUTHENTICATED.withDescription("Valid mutual TLS identity is required"), new Metadata());
        MetadataGrpcService.audit(_audit, "", call.getMethodDescriptor().getBareMethodName(), "UNAUTHENTICATED");
        return new ServerCall.Listener<>() { };
      }
      Context.CancellableContext context = Context.current().withValue(PEER_FINGERPRINT, fingerprint)
          .withDeadlineAfter(_maxRpcMillis, TimeUnit.MILLISECONDS, _deadlines);
      DeadlineCall<REQ, RESP> boundedCall = new DeadlineCall<>(call);
      // A child Context deadline does not close the underlying RPC. Close from its cancellation thread so a blocked
      // application handler cannot delay the server deadline until it returns to gRPC's serialized executor.
      context.addListener(cancelled -> {
        Status status = Contexts.statusFromCancelled(cancelled);
        if (status != null) {
          boundedCall.close(status, new Metadata());
        }
      }, Runnable::run);
      try {
        ServerCall.Listener<REQ> listener = Contexts.interceptCall(context, boundedCall, headers, next);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
          @Override
          public void onComplete() {
            try {
              super.onComplete();
            } finally {
              context.cancel(null);
            }
          }

          @Override
          public void onCancel() {
            try {
              super.onCancel();
            } finally {
              context.cancel(null);
            }
          }
        };
      } catch (RuntimeException | Error e) {
        context.cancel(e);
        throw e;
      }
    }
  }

  /// Serializes deadline closure with response writes; ServerCall itself is not thread safe.
  private static final class DeadlineCall<REQ, RESP>
      extends ForwardingServerCall.SimpleForwardingServerCall<REQ, RESP> {
    private boolean _closed;

    private DeadlineCall(ServerCall<REQ, RESP> delegate) {
      super(delegate);
    }

    @Override
    public synchronized void sendHeaders(Metadata headers) {
      if (!_closed) {
        super.sendHeaders(headers);
      }
    }

    @Override
    public synchronized void sendMessage(RESP message) {
      if (!_closed) {
        super.sendMessage(message);
      }
    }

    @Override
    public synchronized void close(Status status, Metadata trailers) {
      if (!_closed) {
        _closed = true;
        super.close(status, trailers);
      }
    }
  }
}
