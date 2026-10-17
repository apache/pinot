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

import io.grpc.Metadata;
import io.grpc.Status;
import javax.annotation.Nullable;
import org.apache.pinot.query.routing.QueryServerInstance;


/// The identity of a multi-stage engine receiver (a mailbox server or a query server), and the request headers through
/// which a sender says which instance it means.
///
/// Senders reach receivers by hostname and port, and that address can lead to another instance than the one a plan
/// was made for. Pod IP addresses are reused, and every server can listen on the same port. A sender whose connection,
/// or DNS answer, is older than such a change reaches whichever instance now holds the old address, and that instance
/// used to accept the request as its own. So a sender puts the instance it means in the request headers, and an
/// instance that is not that one rejects the request (see [ReceiverIdentityInterceptor]). The sender then knows that
/// its connection is stale, resets it, and fails fast instead of leaving the intended receiver waiting.
///
/// The check uses the instance id, which never changes for an instance, so stale cluster metadata cannot make a
/// receiver reject requests that are meant for it. Requests carry the hash of the id (see
/// [QueryServerInstance#hashInstanceId(String)]), as hexadecimal text: mailbox plans carry the hash rather than the id
/// to stay small, and the text of a hash is ASCII, which a header needs, whatever characters the id has. A request
/// without the hash, as sent by older versions, is accepted.
///
/// Immutable and thread-safe.
public final class ReceiverIdentity {
  /// The hash of the id of the instance the request is meant for, in hexadecimal.
  public static final Metadata.Key<String> TARGET_INSTANCE_ID_HASH_KEY =
      Metadata.Key.of("pinot-target-instance-id-hash", Metadata.ASCII_STRING_MARSHALLER);
  /// Trailer of a rejected request. Its value describes the receiver that rejected it.
  public static final Metadata.Key<String> MISROUTED_TO_KEY =
      Metadata.Key.of("pinot-misrouted-to", Metadata.ASCII_STRING_MARSHALLER);

  private final long _instanceIdHash;
  private final String _description;

  /// @param instanceId the id of this instance
  /// @param address the address senders use for this receiver, used in messages only
  public ReceiverIdentity(String instanceId, String address) {
    _instanceIdHash = QueryServerInstance.hashInstanceId(instanceId);
    _description = instanceId + " at " + address;
  }

  /// Adds the hash of the id of the instance the request is meant for to the request headers, unless it is 0, which
  /// stands for an unknown instance.
  public static void addTargetInstanceIdHash(Metadata headers, long instanceIdHash) {
    if (instanceIdHash != 0) {
      headers.put(TARGET_INSTANCE_ID_HASH_KEY, Long.toHexString(instanceIdHash));
    }
  }

  /// Returns why a request with these headers is not meant for this receiver, or `null` if it is meant for it or does
  /// not say.
  @Nullable
  public String checkTarget(Metadata headers) {
    String targetInstanceIdHash = headers.get(TARGET_INSTANCE_ID_HASH_KEY);
    if (targetInstanceIdHash == null) {
      return null;
    }
    long hash;
    try {
      hash = Long.parseUnsignedLong(targetInstanceIdHash, 16);
    } catch (NumberFormatException e) {
      // Not from a Pinot sender: nothing to check against
      return null;
    }
    if (hash == _instanceIdHash) {
      return null;
    }
    return "The request is for the instance whose id hashes to " + targetInstanceIdHash + " but reached "
        + _description;
  }

  /// Returns whether `t` is the rejection of a request that reached another instance than the one it was meant for.
  public static boolean isMisrouted(@Nullable Throwable t) {
    return getMisroutedTo(t) != null;
  }

  /// Returns the description of the receiver that rejected the request as not meant for it, or `null` if `t` is not
  /// such a rejection.
  @Nullable
  public static String getMisroutedTo(@Nullable Throwable t) {
    if (t == null) {
      return null;
    }
    Metadata trailers = Status.trailersFromThrowable(t);
    return trailers != null ? trailers.get(MISROUTED_TO_KEY) : null;
  }

  @Override
  public String toString() {
    return _description;
  }
}
