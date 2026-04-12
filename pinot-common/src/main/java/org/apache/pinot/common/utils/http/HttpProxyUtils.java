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
package org.apache.pinot.common.utils.http;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;


/**
 * Utilities for proxying/forwarding HTTP requests between Pinot components (broker → controller, controller → broker).
 *
 * <p>The main concern is hop-by-hop and transport-specific headers that must not be forwarded verbatim to a downstream
 * request whose body may be completely different from the original (e.g. a SQL query body vs. an AdhocTaskConfig body).
 * Forwarding a stale {@code Content-Length} causes the downstream server to truncate the new body and return HTTP 400.
 *
 * <p>{@code Content-Type} is also excluded because each downstream call constructs its own body; the transport layer
 * (e.g. {@code PinotAdminTransport}) always sets the correct {@code Content-Type: application/json} itself.
 */
public final class HttpProxyUtils {
  private HttpProxyUtils() {
  }

  /**
   * Hop-by-hop and transport-specific HTTP headers that must never be forwarded to downstream requests.
   * The set is case-insensitive and immutable.
   *
   * <ul>
   *   <li>{@code Content-Length} / {@code Transfer-Encoding} — the downstream body is different; the transport
   *       recalculates these from the actual body it sends.</li>
   *   <li>{@code Content-Type} — the downstream transport always sets {@code application/json} for its own body.</li>
   *   <li>{@code Host} — must be the destination host, not the origin client's host.</li>
   *   <li>{@code Connection}, {@code Keep-Alive}, {@code Proxy-Connection}, {@code TE}, {@code Trailer},
   *       {@code Upgrade}, {@code Expect} — per-hop connection-management headers (RFC 7230 §6.1).</li>
   * </ul>
   */
  public static final Set<String> NON_FORWARDABLE_HEADERS;

  static {
    TreeSet<String> tmp = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    tmp.addAll(List.of("Host", "Connection", "Content-Length", "Content-Type", "Transfer-Encoding", "Expect",
        "Keep-Alive", "Proxy-Connection", "TE", "Trailer", "Upgrade"));
    NON_FORWARDABLE_HEADERS = Collections.unmodifiableSet(tmp);
  }

  /**
   * Returns a new case-insensitive {@link TreeMap} containing only the forwardable entries from {@code headers}.
   * Hop-by-hop and transport-specific headers (see {@link #NON_FORWARDABLE_HEADERS}) are excluded.
   * Returns an empty map when {@code headers} is {@code null} or empty.
   */
  public static Map<String, String> copyForwardableHeaders(@Nullable Map<String, String> headers) {
    Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        if (!NON_FORWARDABLE_HEADERS.contains(entry.getKey())) {
          result.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return result;
  }
}
