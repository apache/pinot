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

import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.coordination.MetadataRpcConnection.Endpoint;
import org.apache.pinot.spi.env.PinotConfiguration;

import static com.google.common.base.Preconditions.checkArgument;


/// Immutable bootstrap identity, TLS and bounded connection settings shared by native participant starters.
/// Scheduling intervals are transport controls, never qualifications of authority clocks or serving lifetimes.
public final class NativeMetadataConfig {
  public static final String PREFIX = "pinot.coordination.ratis.";
  private final UUID _historyId;
  private final String _participantId;
  private final List<Endpoint> _endpoints;
  private final Path _certificate;
  private final Path _privateKey;
  private final Path _trustCertificates;
  private final long _rpcTimeoutMillis;
  private final long _heartbeatMillis;

  public NativeMetadataConfig(PinotConfiguration config) {
    _historyId = UUID.fromString(required(config, "historyId"));
    _participantId = required(config, "participantId");
    checkArgument(_participantId.length() <= 128, "Participant ID must contain at most 128 characters");
    List<Endpoint> endpoints = new ArrayList<>();
    Set<String> unique = new HashSet<>();
    for (String value : required(config, "metadata.endpoints").split(",", -1)) {
      HostAndPort address = HostAndPort.fromString(value.trim());
      checkArgument(address.hasPort() && unique.add(address.toString()), "Missing port or duplicate metadata endpoint");
      endpoints.add(new Endpoint(address.getHost(), address.getPort()));
    }
    checkArgument(!endpoints.isEmpty() && endpoints.size() <= 16, "Between one and sixteen endpoints are required");
    _endpoints = List.copyOf(endpoints);
    _certificate = readableFile(config, "tls.certChain");
    _privateKey = readableFile(config, "tls.privateKey");
    _trustCertificates = readableFile(config, "tls.trustCerts");
    _rpcTimeoutMillis = config.getProperty(PREFIX + "rpc.timeoutMs", 5000L);
    _heartbeatMillis = config.getProperty(PREFIX + "heartbeatMillis", 1000L);
    checkArgument(_rpcTimeoutMillis > 0 && _rpcTimeoutMillis <= 30_000, "RPC timeout must be between 1 and 30000 ms");
    checkArgument(_heartbeatMillis > 0 && _heartbeatMillis <= 30_000,
        "Heartbeat interval must be between 1 and 30000 ms");
  }

  public static String required(PinotConfiguration config, String suffix) {
    String value = config.getProperty(PREFIX + suffix);
    checkArgument(value != null && !value.isBlank(), "Missing required configuration: %s%s", PREFIX, suffix);
    return value.trim();
  }

  public static Path readableFile(PinotConfiguration config, String suffix) {
    Path path = Path.of(required(config, suffix)).toAbsolutePath().normalize();
    checkArgument(Files.isRegularFile(path) && Files.isReadable(path), "Unreadable file for %s%s", PREFIX, suffix);
    return path;
  }

  public UUID historyId() {
    return _historyId;
  }

  public String participantId() {
    return _participantId;
  }

  public List<Endpoint> endpoints() {
    return _endpoints;
  }

  public Path certificate() {
    return _certificate;
  }

  public Path privateKey() {
    return _privateKey;
  }

  public Path trustCertificates() {
    return _trustCertificates;
  }

  public long rpcTimeoutMillis() {
    return _rpcTimeoutMillis;
  }

  public long heartbeatMillis() {
    return _heartbeatMillis;
  }

  public MetadataRpcConnection openConnection() throws IOException {
    return new MetadataRpcConnection(_endpoints, _certificate, _privateKey, _trustCertificates, _rpcTimeoutMillis);
  }
}
