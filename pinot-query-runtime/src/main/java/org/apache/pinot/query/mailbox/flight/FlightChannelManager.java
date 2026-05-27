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
package org.apache.pinot.query.mailbox.flight;

import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages Arrow Flight client connections to remote Pinot nodes.
 *
 * <p>Clients are created lazily on first use and reused across queries to amortise connection setup cost.
 * All clients share a single child {@link BufferAllocator} sourced from the global {@link ArrowBuffers} root.
 */
public class FlightChannelManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightChannelManager.class);

  private final ConcurrentHashMap<Pair<String, Integer>, FlightClient> _clientMap = new ConcurrentHashMap<>();
  @Nullable
  private final TlsConfig _tlsConfig;
  private final BufferAllocator _allocator;

  public FlightChannelManager(@Nullable TlsConfig tlsConfig, ArrowBuffers arrowBuffers) {
    _tlsConfig = tlsConfig;
    _allocator = arrowBuffers.newQueryAllocator("flight-channel-manager");
  }

  /**
   * Returns a {@link FlightClient} for the given host and port, creating one if it does not yet exist.
   */
  public FlightClient getClient(String hostname, int port) {
    return _clientMap.computeIfAbsent(Pair.of(hostname, port), k -> {
      LOGGER.info("Creating Arrow Flight client for {}:{}", k.getLeft(), k.getRight());
      // TODO: Add TLS support for Flight clients (requires PEM cert/key, not JKS)
      Location location = Location.forGrpcInsecure(k.getLeft(), k.getRight());
      return FlightClient.builder(_allocator, location).build();
    });
  }

  @Override
  public void close() {
    LOGGER.info("Closing {} Arrow Flight client(s)", _clientMap.size());
    _clientMap.values().forEach(client -> {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing Arrow Flight client", e);
      }
    });
    _clientMap.clear();
    _allocator.close();
  }
}
