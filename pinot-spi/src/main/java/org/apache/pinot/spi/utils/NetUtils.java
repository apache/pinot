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
package org.apache.pinot.spi.utils;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NetUtils {
  private NetUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(NetUtils.class);
  // Google Public DNS IPs dns.google
  private static final int HTTP_PORT = 80;
  private static final String DUMMY_OUT_IPV4 = "8.8.8.8";
  private static final String DUMMY_OUT_IPV6 = "2001:4860:4860::8888";

  /**
   * Get the ip address of local host.
   *
   * @return IP address {@link String}, either IPv4 or IPv6 format depending on java.net.preferIPv6Addresses property.
   */
  public static String getHostAddress()
      throws SocketException, UnknownHostException {
    boolean isIPv6Preferred = Boolean.parseBoolean(System.getProperty("java.net.preferIPv6Addresses"));
    DatagramSocket ds = new DatagramSocket();
    try {
      ds.connect(isIPv6Preferred ? Inet6Address.getByName(DUMMY_OUT_IPV6) : Inet4Address.getByName(DUMMY_OUT_IPV4),
          HTTP_PORT);
    } catch (java.io.UncheckedIOException e) {
      LOGGER.warn(e.getMessage());
      if (isIPv6Preferred) {
        LOGGER.warn("No IPv6 route available on host, falling back to IPv4");
        ds.connect(Inet4Address.getByName(DUMMY_OUT_IPV4), HTTP_PORT);
      } else {
        LOGGER.warn("No IPv4 route available on host, falling back to IPv6");
        ds.connect(Inet6Address.getByName(DUMMY_OUT_IPV6), HTTP_PORT);
      }
    }

    InetAddress localAddress = ds.getLocalAddress();
    if (localAddress.isAnyLocalAddress()) {
      localAddress = isIPv6Preferred ? getLocalIPv6Address() : InetAddress.getLocalHost();
    }

    return localAddress.getHostAddress();
  }

  /**
   * Get a local IPv6 address out of IPv4/IPv6 addresses queried based on the local hostname.
   * If no IPv6 address is found, fall back to default InetAddress.getLocalHost() behavior.
   *
   * @return {@link InetAddress} object representing a local IPv6 address.
   */
  private static InetAddress getLocalIPv6Address() throws UnknownHostException {
    for (InetAddress address : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
      if (address instanceof Inet6Address && !address.isAnyLocalAddress()) {
        return address;
      }
    }

    LOGGER.warn("Failed to find a non-wildcard IPv6 address, falling back to localhost");
    return Inet4Address.getLocalHost();
  }

  /**
   * Get the hostname or IP address.
   *
   * @return The hostname if available, otherwise a dotted quad address. Returns null if neither can be determined.
   */
  public static String getHostnameOrAddress() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ignored) {
      try {
        return getHostAddress();
      } catch (Exception e) {
        return null;
      }
    }
  }

  /**
   * Find an open port.
   * @return an open port
   * @throws IOException
   */
  public static int findOpenPort()
      throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * Find the first open port from default port in an incremental order.
   * @param basePort
   * @return an open port
   */
  public static int findOpenPort(int basePort) {
    while (!available(basePort)) {
      basePort++;
      // Add error handling for the case that no port is available
    }
    return basePort;
  }

  /**
   * Checks to see if a specific port is available.
   *
   * @param port the port to check for availability
   */
  public static boolean available(int port) {
    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
      return false;
    } finally {
      if (ds != null) {
        ds.close();
      }
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }
  }
}
