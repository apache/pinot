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
package org.apache.pinot.query.grpc;

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Rejects requests that are addressed to another instance than this one. See [ReceiverIdentity].
///
/// The check runs on the request headers, before the service sees the request, so a rejected mailbox stream never
/// creates a receiving mailbox and a rejected stage plan never runs. The call is closed with
/// [Status.Code#FAILED_PRECONDITION] and the [ReceiverIdentity#MISROUTED_TO_KEY] trailer, which is how the sender tells
/// this rejection from any other error.
///
/// Thread-safe if the `onRejected` callback is.
public class ReceiverIdentityInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverIdentityInterceptor.class);

  private final ReceiverIdentity _identity;
  @Nullable
  private final Metadata.Key<String> _requestIdKey;
  private final Runnable _onRejected;

  /// @param identity the identity of this receiver
  /// @param requestIdKey a header that identifies the request in logs, or `null` if there is none
  /// @param onRejected called for each rejected request
  public ReceiverIdentityInterceptor(ReceiverIdentity identity, @Nullable Metadata.Key<String> requestIdKey,
      Runnable onRejected) {
    _identity = identity;
    _requestIdKey = requestIdKey;
    _onRejected = onRejected;
  }

  @Override
  public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> call, Metadata headers,
      ServerCallHandler<T, R> next) {
    String mismatch = _identity.checkTarget(headers);
    if (mismatch == null) {
      return next.startCall(call, headers);
    }
    _onRejected.run();
    LOGGER.warn("Rejecting {}{} from {}: {}. The connection of the sender to {} is stale and leads to this instance.",
        call.getMethodDescriptor().getBareMethodName(),
        _requestIdKey != null ? " " + headers.get(_requestIdKey) : "",
        call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR), mismatch, call.getAuthority());
    Metadata trailers = new Metadata();
    trailers.put(ReceiverIdentity.MISROUTED_TO_KEY, _identity.toString());
    call.close(Status.FAILED_PRECONDITION.withDescription(mismatch), trailers);
    // Skip any future operations since we have closed the call
    return new ServerCall.Listener<T>() {
    };
  }
}
