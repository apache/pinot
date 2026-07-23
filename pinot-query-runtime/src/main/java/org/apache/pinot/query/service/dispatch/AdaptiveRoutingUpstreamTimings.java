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
package org.apache.pinot.query.service.dispatch;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Holds per-upstream-sender timing data for adaptive routing, encoded for transport via StatMap STRING keys.
 *
 * <p>Format: {@code "key1=elapsedMs1;key2=elapsedMs2"}
 * <ul>
 *   <li>Entries are separated by {@code ';'}</li>
 *   <li>Key and value are separated by {@code '='}</li>
 *   <li>Keys are of the form {@code "hostname|mailboxPort"} (pipe cannot appear in DNS names or port numbers)</li>
 *   <li>elapsedMs values are non-negative longs</li>
 * </ul>
 *
 * <p>This encoding is used in
 * {@link org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator.StatKey#UPSTREAM_SERVER_RESPONSE_TIMES_MS}.
 */
public class AdaptiveRoutingUpstreamTimings {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveRoutingUpstreamTimings.class);

  public static final String COLLECT_UPSTREAM_TIMING_KEY = "collectUpstreamTiming";

  static final char ENTRY_SEPARATOR = ';';
  static final char KV_SEPARATOR = '=';

  private AdaptiveRoutingUpstreamTimings() {
  }

  /**
   * Returns the key used to identify a sender in the timing map, given its hostname and mailbox port.
   * <p>The {@code '|'} separator is chosen because it cannot appear in DNS hostnames or port numbers.
   */
  public static String senderKey(String hostname, int mailboxPort) {
    return hostname + "|" + mailboxPort;
  }

  /**
   * Encode a map of senderKey -> elapsedMs into a string.
   *
   * @return encoded string, or {@code null} if the map is empty (null = absent in StatMap)
   */
  @Nullable
  public static String encode(Map<String, Long> timings) {
    if (timings.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Long> entry : timings.entrySet()) {
      if (sb.length() > 0) {
        sb.append(ENTRY_SEPARATOR);
      }
      sb.append(entry.getKey()).append(KV_SEPARATOR).append(entry.getValue());
    }
    return sb.toString();
  }

  /**
   * Decode a string (possibly null) into a mutable map of senderKey -> elapsedMs.
   */
  public static Map<String, Long> decode(@Nullable String encoded) {
    Map<String, Long> result = new HashMap<>();
    if (encoded == null || encoded.isEmpty()) {
      return result;
    }
    int start = 0;
    int len = encoded.length();
    while (start < len) {
      int end = encoded.indexOf(ENTRY_SEPARATOR, start);
      if (end < 0) {
        end = len;
      }
      int eq = encoded.indexOf(KV_SEPARATOR, start);
      if (eq > start && eq < end) {
        String key = encoded.substring(start, eq);
        try {
          long value = Long.parseLong(encoded.substring(eq + 1, end));
          result.put(key, value);
        } catch (NumberFormatException e) {
          LOGGER.warn("Skipping malformed timing entry '{}': {}", encoded.substring(start, end), e.getMessage());
        }
      }
      start = end + 1;
    }
    return result;
  }

  /**
   * Merge two encoded timing strings, taking the max elapsedMs per senderKey.
   * Either or both arguments may be null (treated as empty).
   */
  @Nullable
  public static String mergeEncodings(@Nullable String enc1, @Nullable String enc2) {
    Map<String, Long> merged = decode(enc1);
    Map<String, Long> incoming = decode(enc2);
    for (Map.Entry<String, Long> entry : incoming.entrySet()) {
      merged.merge(entry.getKey(), entry.getValue(), Math::max);
    }
    return encode(merged);
  }
}
