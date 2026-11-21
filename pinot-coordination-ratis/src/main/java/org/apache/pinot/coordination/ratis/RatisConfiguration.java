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
package org.apache.pinot.coordination.ratis;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;


/// Immutable bootstrap configuration. This foundation accepts only loopback peers until authenticated transport is
/// integrated; it is not a remotely deployable controller backend.
final class RatisConfiguration {
  static final String PREFIX = "pinot.coordination.ratis.";
  final UUID _clusterId;
  final UUID _historyId;
  final RaftGroup _group;
  final long _requestTimeoutMs;

  RatisConfiguration(PinotConfiguration config) {
    _clusterId = UUID.fromString(required(config, "clusterId"));
    _historyId = UUID.fromString(required(config, "historyId"));
    List<RaftPeer> peers = new ArrayList<>();
    Set<String> ids = new HashSet<>();
    Set<String> addresses = new HashSet<>();
    for (String entry : required(config, "voters").split(",", -1)) {
      String[] parts = entry.trim().split("@", -1);
      Preconditions.checkArgument(parts.length == 2 && parts[0].matches("[A-Za-z0-9_-]+"),
          "Expected voter id@host:port: %s", entry);
      HostAndPort address = HostAndPort.fromString(parts[1]);
      Preconditions.checkArgument(address.hasPort() && address.getPort() > 0, "Expected voter host:port: %s", entry);
      Preconditions.checkArgument(address.getHost().equals("127.0.0.1") || address.getHost().equals("::1"),
          "Only literal loopback voters are supported before authenticated transport: %s", entry);
      Preconditions.checkArgument(ids.add(parts[0]) && addresses.add(address.toString()),
          "Voter ids and addresses must be unique: %s", entry);
      peers.add(RaftPeer.newBuilder().setId(parts[0]).setAddress(address.toString()).build());
    }
    Preconditions.checkArgument(peers.size() == 3, "The initial metadata core requires exactly three voters");
    // History changes create a separate Raft group as well as a separate application fencing domain.
    _group = RaftGroup.valueOf(RaftGroupId.valueOf(_historyId), peers);
    _requestTimeoutMs = config.getProperty(PREFIX + "request.timeoutMs", 15_000L);
    Preconditions.checkArgument(_requestTimeoutMs > 0 && _requestTimeoutMs <= 300_000,
        "Request timeout must be between 1 and 300000 milliseconds");
  }

  static String required(PinotConfiguration config, String suffix) {
    String value = config.getProperty(PREFIX + suffix);
    Preconditions.checkArgument(value != null && !value.isBlank(), "Missing configuration: %s%s", PREFIX, suffix);
    return value.trim();
  }
}
