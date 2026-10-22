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
package org.apache.pinot.core.transport.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import javax.net.ssl.SSLSession;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryGate;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Binds admission to the TLS peer and serializes all streamed/final result callbacks through one serving ticket.
/// The surrounding listener must require verified client certificates; no request header can set the peer context.
final class NativeGrpcQueryGuard implements ServerInterceptor {
  private static final Context.Key<String> PEER_FINGERPRINT = Context.key("native-query-peer-certificate");
  private static final Context.Key<X509Certificate> PEER_CERTIFICATE = Context.key("native-query-peer-x509");

  @Override
  public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers,
      ServerCallHandler<Q, S> next) {
    String fingerprint;
    X509Certificate certificate;
    try {
      SSLSession session = requireNonNull(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
      Certificate[] peer = session.getPeerCertificates();
      checkState(peer.length > 0 && peer[0] instanceof X509Certificate,
          "Native query TLS peer certificate is absent");
      certificate = (X509Certificate) peer[0];
      validateCertificate(certificate);
      fingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(peer[0].getEncoded()));
    } catch (Exception e) {
      call.close(Status.UNAUTHENTICATED.withDescription("Native queries require a verified TLS peer"), new Metadata());
      return new ServerCall.Listener<>() { };
    }
    return Contexts.interceptCall(Context.current().withValues(PEER_FINGERPRINT, fingerprint,
        PEER_CERTIFICATE, certificate), call, headers, next);
  }

  static StreamObserver<ServerResponse> admit(NativeQueryGate gate, NativeQueryContext query,
      StreamObserver<ServerResponse> observer) {
    String peer = PEER_FINGERPRINT.get();
    checkState(peer != null && !Context.current().isCancelled(), "Native query transport is not authenticated or live");
    X509Certificate certificate = requireNonNull(PEER_CERTIFICATE.get());
    validateCertificate(certificate);
    return new GuardedObserver(observer, requireNonNull(gate.admit(query, peer)), Context.current(), certificate);
  }

  private static void validateCertificate(X509Certificate certificate) {
    try {
      certificate.checkValidity();
    } catch (CertificateException e) {
      throw Status.UNAUTHENTICATED.withDescription("Native query peer certificate is not currently valid")
          .asRuntimeException();
    }
  }

  /// Every onNext and the successful terminal event consume the same authority. Failure terminates exactly once;
  /// throwing from a rejected callback also stops a streaming producer instead of letting it continue silently.
  private static final class GuardedObserver implements StreamObserver<ServerResponse> {
    private final StreamObserver<ServerResponse> _observer;
    private final NativeQueryGate.Delivery _delivery;
    private final Context _context;
    private final X509Certificate _certificate;
    private boolean _terminal;

    private GuardedObserver(StreamObserver<ServerResponse> observer, NativeQueryGate.Delivery delivery,
        Context context, X509Certificate certificate) {
      _observer = observer;
      _delivery = delivery;
      _context = context;
      _certificate = certificate;
    }

    @Override
    public synchronized void onNext(ServerResponse value) {
      deliver(() -> _observer.onNext(value), false);
    }

    @Override
    public synchronized void onCompleted() {
      deliver(_observer::onCompleted, true);
    }

    @Override
    public synchronized void onError(Throwable error) {
      if (!_terminal) {
        _terminal = true;
        _observer.onError(error);
      }
    }

    private void deliver(Runnable callback, boolean terminal) {
      checkState(!_terminal, "Native query response is already terminated");
      try {
        _delivery.deliver(() -> {
          validateCertificate(_certificate);
          if (_context.isCancelled()) {
            throw Status.CANCELLED.withDescription("Native query RPC is no longer live").asRuntimeException();
          }
          _terminal = terminal;
          callback.run();
        });
      } catch (RuntimeException e) {
        onError(Status.FAILED_PRECONDITION.withDescription("Native query delivery authority is unavailable")
            .asRuntimeException());
        throw e;
      }
    }
  }
}
