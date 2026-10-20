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
package org.apache.pinot.broker.transport;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Deadline;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import javax.net.ssl.SSLSession;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server.ServerRequest;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.common.request.context.NativeQueryContext;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/// Strict native query transport with fixed, explicitly scoped PEM credentials and one owned channel per call.
/// There is no plaintext, authority override, credential renewal, retry, or global TLS/channel cache. Per-call
/// channels trade connection reuse for bounded ownership in this experimental path. Closing a call cancels its
/// RPC; closing the client cancels every outstanding call. Callers still enforce query admission and response
/// identities: transport authentication alone does not authorize a query or establish complete query coverage.
public final class NativeGrpcQueryClient implements AutoCloseable {
  private static final int MAX_CREDENTIAL_BYTES = 1024 * 1024;
  private final SslContext _tls;
  private final Limits _limits;
  private final Clock _certificateClock;
  private final Semaphore _slots;
  private final Object _lifecycle = new Object();
  private final Set<OwnedCall> _calls = ConcurrentHashMap.newKeySet();
  private final Set<ManagedChannel> _channels = ConcurrentHashMap.newKeySet();
  private final ScheduledExecutorService _reaper = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-query-channel-cleanup"));
  private final AtomicBoolean _closed = new AtomicBoolean();

  /// Hard bounds apply independently to each call; the active channel count cannot exceed maxConcurrentCalls.
  public record Limits(int maxConcurrentCalls, int maxRequestBytes, int maxFrameBytes, int maxFrames,
                       long maxResponseBytes, long maxCallMillis) {
    public Limits {
      checkArgument(maxConcurrentCalls > 0 && maxConcurrentCalls <= 128
          && maxRequestBytes > 0 && maxRequestBytes <= 8 * 1024 * 1024
          && maxFrameBytes > 0 && maxFrameBytes <= 16 * 1024 * 1024
          && maxFrames > 0 && maxFrames <= 65_536 && maxResponseBytes >= maxFrameBytes
          && maxResponseBytes <= 256L * 1024 * 1024 && maxCallMillis > 0 && maxCallMillis <= 300_000,
          "Invalid bounded native query transport configuration");
    }
  }

  /// An owned, cancellable server stream. The iterator has one consumer; cancellation may run on another thread.
  public interface Call extends AutoCloseable {
    Iterator<ServerResponse> responses();

    void cancel();

    @Override
    void close();
  }

  public NativeGrpcQueryClient(Path certificateChain, Path privateKey, Path trustCertificates, Limits limits)
      throws IOException {
    this(certificateChain, privateKey, trustCertificates, limits, Clock.systemUTC());
  }

  @VisibleForTesting
  NativeGrpcQueryClient(Path certificateChain, Path privateKey, Path trustCertificates, Limits limits,
      Clock certificateClock) throws IOException {
    _limits = requireNonNull(limits);
    _certificateClock = requireNonNull(certificateClock);
    _slots = new Semaphore(limits.maxConcurrentCalls());
    byte[] certificate = readCredential(certificateChain);
    byte[] key = readCredential(privateKey);
    try {
      byte[] trust = readCredential(trustCertificates);
      _tls = GrpcSslContexts.configure(SslContextBuilder.forClient(), SslProvider.JDK)
          .keyManager(new ByteArrayInputStream(certificate), new ByteArrayInputStream(key))
          .trustManager(new ByteArrayInputStream(trust)).build();
    } finally {
      Arrays.fill(key, (byte) 0);
    }
  }

  /// The supplier must return the remaining original qualified query budget, not a fresh timeout. It must be local
  /// and nonblocking; it is checked before connection/request admission and on both sides of iterator delivery.
  public Call submit(URI endpoint, ServerRequest request, LongSupplier remainingMillis) {
    validateEndpoint(endpoint);
    requireNonNull(request);
    requireNonNull(remainingMillis);
    checkArgument(request.hasNativeQueryContext()
        && NativeQueryContext.fromProto(request.getNativeQueryContext()).deadline() != null,
        "Native query transport requires an identity envelope and original deadline");
    if (request.getSerializedSize() > _limits.maxRequestBytes()) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Native query request exceeds its byte bound")
          .asRuntimeException();
    }
    if (_closed.get()) {
      throw Status.CANCELLED.withDescription("Native query client is closed").asRuntimeException();
    }
    Deadline anchor = Deadline.after(0, TimeUnit.NANOSECONDS);
    long remaining = remaining(remainingMillis);
    Deadline deadline = anchor.offset(Math.min(remaining, _limits.maxCallMillis()), TimeUnit.MILLISECONDS);
    if (!_slots.tryAcquire()) {
      throw Status.RESOURCE_EXHAUSTED.withDescription("Native query call/channel limit reached").asRuntimeException();
    }
    ManagedChannel channel = null;
    OwnedCall call = null;
    try {
      synchronized (_lifecycle) {
        if (_closed.get()) {
          throw Status.CANCELLED.withDescription("Native query client is closed").asRuntimeException();
        }
        channel = NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).sslContext(_tls)
            .maxInboundMessageSize(_limits.maxFrameBytes()).maxInboundMetadataSize(16 * 1024)
            .disableRetry().build();
        _channels.add(channel);
        call = new OwnedCall(endpoint, channel, remainingMillis, deadline);
        _calls.add(call);
      }
      call.start(request);
      return call;
    } catch (RuntimeException | Error e) {
      if (call != null) {
        call.cancel();
      } else {
        if (channel != null) {
          retire(channel, null);
        } else {
          _slots.release();
        }
      }
      throw e;
    }
  }

  @Override
  public void close() {
    synchronized (_lifecycle) {
      _closed.set(true);
    }
    _calls.forEach(OwnedCall::cancel);
    if (_channels.isEmpty()) {
      _reaper.shutdown();
    }
  }

  @VisibleForTesting
  int channelCount() {
    return _channels.size();
  }

  private void retire(ManagedChannel channel, @Nullable OwnedCall call) {
    channel.shutdownNow();
    _reaper.execute(() -> reap(channel, call));
  }

  private void reap(ManagedChannel channel, @Nullable OwnedCall call) {
    if (!channel.isTerminated()) {
      _reaper.schedule(() -> reap(channel, call), 10, TimeUnit.MILLISECONDS);
      return;
    }
    if (call != null) {
      _calls.remove(call);
    }
    _slots.release();
    _channels.remove(channel);
    if (_closed.get() && _channels.isEmpty()) {
      _reaper.shutdown();
    }
  }

  private final class OwnedCall implements Call {
    private final URI _endpoint;
    private final ManagedChannel _channel;
    private final LongSupplier _remainingMillis;
    private final Deadline _deadline;
    private final AtomicBoolean _released = new AtomicBoolean();
    private final AtomicReference<StatusRuntimeException> _failure = new AtomicReference<>();
    private final ClientCall<ServerRequest, ServerResponse> _rpc;
    @Nullable
    private volatile SSLSession _session;
    @Nullable
    private Iterator<ServerResponse> _responses;

    private OwnedCall(URI endpoint, ManagedChannel channel, LongSupplier remainingMillis, Deadline deadline) {
      _endpoint = endpoint;
      _channel = channel;
      _remainingMillis = remainingMillis;
      _deadline = deadline;
      _rpc = channel.newCall(PinotQueryServerGrpc.getSubmitMethod(), CallOptions.DEFAULT.withDeadline(deadline)
          .withCallCredentials(new PeerCredentials()));
    }

    private void start(ServerRequest request) {
      checkBudget();
      ClientCall<ServerRequest, ServerResponse> guarded =
          new SimpleForwardingClientCall<>(_rpc) {
            @Override
            public void start(Listener<ServerResponse> listener, Metadata headers) {
              super.start(new SimpleForwardingClientCallListener<>(listener) {
                private int _frames;
                private long _bytes;

                @Override
                public void onMessage(ServerResponse response) {
                  try {
                    checkDelivery();
                    int bytes = response.getSerializedSize();
                    if (++_frames > _limits.maxFrames() || bytes > _limits.maxFrameBytes()
                        || bytes > _limits.maxResponseBytes() - _bytes) {
                      throw Status.RESOURCE_EXHAUSTED.withDescription("Native query response exceeds its bounds")
                          .asRuntimeException();
                    }
                    _bytes += bytes;
                    super.onMessage(response);
                  } catch (RuntimeException e) {
                    fail(e);
                  }
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                  try {
                    if (status.isOk()) {
                      try {
                        checkDelivery();
                      } catch (RuntimeException e) {
                        _failure.compareAndSet(null, statusException(e));
                      }
                    }
                    StatusRuntimeException failure = _failure.get();
                    super.onClose(failure == null ? status : failure.getStatus(), trailers);
                  } finally {
                    release();
                  }
                }
              }, headers);
            }
          };
      Iterator<ServerResponse> iterator = ClientCalls.blockingServerStreamingCall(guarded, request);
      _responses = new Iterator<>() {
        @Override
        public boolean hasNext() {
          try {
            checkWaiting();
            boolean next = iterator.hasNext();
            checkDelivery();
            return next;
          } catch (RuntimeException e) {
            fail(e);
            throw e;
          }
        }

        @Override
        public ServerResponse next() {
          try {
            checkWaiting();
            ServerResponse response = iterator.next();
            checkDelivery();
            return response;
          } catch (RuntimeException e) {
            fail(e);
            throw e;
          }
        }
      };
    }

    @Override
    public Iterator<ServerResponse> responses() {
      return requireNonNull(_responses, "Native query call has not started");
    }

    @Override
    public void cancel() {
      fail(Status.CANCELLED.withDescription("Native query call was cancelled").asRuntimeException());
    }

    @Override
    public void close() {
      cancel();
    }

    private void fail(RuntimeException failure) {
      _failure.compareAndSet(null, statusException(failure));
      _rpc.cancel("Native query call authority or transport failed", failure);
      release();
    }

    private void release() {
      if (_released.compareAndSet(false, true)) {
        retire(_channel, this);
      }
    }

    private void checkBudget() {
      StatusRuntimeException failure = _failure.get();
      if (failure != null) {
        throw failure;
      }
      if (_closed.get()) {
        throw Status.CANCELLED.withDescription("Native query client is closed").asRuntimeException();
      }
      remaining(_remainingMillis);
      if (_deadline.isExpired()) {
        throw Status.DEADLINE_EXCEEDED.withDescription("Native query call deadline expired").asRuntimeException();
      }
    }

    private void checkDelivery() {
      checkBudget();
      validatePeer(requireNonNull(_session, "Native query transport has no authenticated peer"), _endpoint.getHost());
    }

    private void checkWaiting() {
      checkBudget();
      // Prior to receiving the first frame, the iterator can wait for negotiation. Request admission itself is
      // blocked by PeerCredentials until TLS identity has been verified.
      SSLSession session = _session;
      if (session != null) {
        validatePeer(session, _endpoint.getHost());
      }
    }

    private final class PeerCredentials extends CallCredentials {
      @Override
      public void applyRequestMetadata(RequestInfo info, Executor executor, MetadataApplier applier) {
        try {
          checkBudget();
        } catch (RuntimeException e) {
          applier.fail(statusException(e).getStatus());
          return;
        }
        try {
          SSLSession session = requireNonNull(info.getTransportAttrs().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
          validatePeer(session, _endpoint.getHost());
          _session = session;
          applier.apply(new Metadata());
        } catch (Exception e) {
          applier.fail(Status.UNAUTHENTICATED.withDescription("Native query requires a current verified TLS peer")
              .withCause(e));
        }
      }
    }
  }

  private void validatePeer(SSLSession session, String hostname) {
    try {
      Certificate[] chain = session.getPeerCertificates();
      checkArgument(chain.length > 0 && chain[0] instanceof X509Certificate, "Missing TLS server identity");
      Date now = Date.from(_certificateClock.instant());
      for (Certificate certificate : chain) {
        ((X509Certificate) certificate).checkValidity(now);
      }
      Certificate[] own = session.getLocalCertificates();
      checkArgument(own != null && own.length > 0, "Missing TLS client identity");
      for (Certificate certificate : own) {
        ((X509Certificate) certificate).checkValidity(now);
      }
      new DefaultHostnameVerifier().verify(hostname, (X509Certificate) chain[0]);
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED.withDescription("Native query TLS identity is absent, expired or mismatched")
          .withCause(e).asRuntimeException();
    }
  }

  private static long remaining(LongSupplier supplier) {
    long remaining = supplier.getAsLong();
    if (remaining <= 0) {
      throw Status.DEADLINE_EXCEEDED.withDescription("Original native query deadline expired").asRuntimeException();
    }
    return remaining;
  }

  private static StatusRuntimeException statusException(RuntimeException failure) {
    return failure instanceof StatusRuntimeException status ? status
        : Status.FAILED_PRECONDITION.withDescription("Native query call validation failed").withCause(failure)
            .asRuntimeException();
  }

  private static byte[] readCredential(Path path) throws IOException {
    try (var input = Files.newInputStream(requireNonNull(path))) {
      byte[] value = input.readNBytes(MAX_CREDENTIAL_BYTES + 1);
      if (value.length == 0 || value.length > MAX_CREDENTIAL_BYTES) {
        throw new IOException("Native TLS credential exceeds its byte bound");
      }
      return value;
    }
  }

  private static void validateEndpoint(URI endpoint) {
    requireNonNull(endpoint);
    checkArgument(endpoint.toString().length() <= 2048 && "grpcs".equals(endpoint.getScheme())
        && endpoint.getHost() != null && endpoint.getHost().equals(endpoint.getHost().toLowerCase(Locale.ROOT))
        && endpoint.getPort() > 0 && endpoint.getPort() <= 65_535 && endpoint.getRawUserInfo() == null
        && (endpoint.getRawPath() == null || endpoint.getRawPath().isEmpty())
        && endpoint.getRawQuery() == null && endpoint.getRawFragment() == null,
        "Native query endpoint must be a canonical grpcs authority");
    try {
      checkArgument(endpoint.toString().equals(
          new URI("grpcs", null, endpoint.getHost(), endpoint.getPort(), null, null, null).toASCIIString()),
          "Native query endpoint is not canonical");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid native query endpoint", e);
    }
  }
}
