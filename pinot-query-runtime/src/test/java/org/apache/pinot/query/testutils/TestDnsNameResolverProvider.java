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
package org.apache.pinot.query.testutils;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.StatusOr;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/// A gRPC name resolver whose answers a test controls, standing in for DNS.
///
/// It is registered in the JVM-wide default registry with a higher priority than gRPC's own DNS resolver, so it also
/// serves the channels that production code builds with `NettyChannelBuilder.forAddress(host, port)`. Hostnames it
/// does not know fall through to real DNS. Surefire runs every test class in its own JVM, which keeps the
/// registration from leaking into other classes; still, [#close] it when done.
///
/// A name maps to a whole socket address, port included, and the port in the gRPC target is ignored. That is how a
/// test emulates IP addresses on a single host: every server is addressed by its own name and one shared, pinned
/// port, exactly as in production, while each "IP" is in fact a different loopback port.
///
/// Like DNS, it never pushes changes: a channel only sees a new mapping when it resolves again.
public class TestDnsNameResolverProvider extends NameResolverProvider implements AutoCloseable {
  private static final String SCHEME = "dns";

  /// gRPC's own DNS resolver, for the hostnames this one does not know
  private final NameResolverProvider _dns;
  private final Map<String, InetSocketAddress> _addresses = new ConcurrentHashMap<>();
  private final AtomicInteger _numResolutions = new AtomicInteger();

  private TestDnsNameResolverProvider(NameResolverProvider dns) {
    _dns = dns;
  }

  /// Creates a resolver and registers it in the default registry.
  public static TestDnsNameResolverProvider register() {
    NameResolverRegistry registry = NameResolverRegistry.getDefaultRegistry();
    TestDnsNameResolverProvider provider = new TestDnsNameResolverProvider(registry.getProviderForScheme(SCHEME));
    registry.register(provider);
    return provider;
  }

  /// Makes `hostname` resolve to the given port on the loopback address, from the next resolution on.
  public void map(String hostname, int port) {
    _addresses.put(hostname, new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
  }

  /// How many times any mapped name has been resolved so far.
  public int getNumResolutions() {
    return _numResolutions.get();
  }

  @Override
  public void close() {
    NameResolverRegistry.getDefaultRegistry().deregister(this);
  }

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  protected int priority() {
    // gRPC's DNS resolver has priority 5
    return 10;
  }

  @Override
  public String getDefaultScheme() {
    return SCHEME;
  }

  @Override
  public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
    if (!SCHEME.equals(targetUri.getScheme())) {
      return null;
    }
    // A target such as `dns:///host:port` carries the host and port as its path
    String authority = targetUri.getPath().substring(1);
    int portSeparator = authority.lastIndexOf(':');
    String hostname = portSeparator > 0 ? authority.substring(0, portSeparator) : authority;
    if (!_addresses.containsKey(hostname)) {
      return _dns.newNameResolver(targetUri, args);
    }
    return new NameResolver() {
      private Listener2 _listener;

      @Override
      public String getServiceAuthority() {
        return authority;
      }

      @Override
      public void start(Listener2 listener) {
        _listener = listener;
        resolve();
      }

      @Override
      public void refresh() {
        resolve();
      }

      private void resolve() {
        _numResolutions.incrementAndGet();
        _listener.onResult(ResolutionResult.newBuilder()
            .setAddressesOrError(StatusOr.fromValue(List.of(new EquivalentAddressGroup(_addresses.get(hostname)))))
            .build());
      }

      @Override
      public void shutdown() {
      }
    };
  }
}
