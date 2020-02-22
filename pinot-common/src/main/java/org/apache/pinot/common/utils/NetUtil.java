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
package org.apache.pinot.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;


public class NetUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetUtil.class);

  private static final String LOCALHOST_NAME = "localhost";
  private static final String DUMMY_OUT_IP = "74.125.224.0";

  /**
   * Get the ip address of local host.
   */
  public static String getHostAddress()
      throws SocketException, UnknownHostException {
    DatagramSocket ds = new DatagramSocket();
    ds.connect(InetAddress.getByName(DUMMY_OUT_IP), 80);
    InetAddress localAddress = ds.getLocalAddress();
    if (localAddress.getHostAddress().equals("0.0.0.0")) {
      localAddress = InetAddress.getLocalHost();
    }
    return localAddress.getHostAddress();
  }

  /**
   * Get the hostname or IP address.
   *
   * @return The hostname if available, otherwise a dotted quad address. Returns null if neither can be determined.
   */
  public static String getHostnameOrAddress() {
    try {
      String hostName = InetAddress.getLocalHost().getCanonicalHostName();
      if (!hostName.equalsIgnoreCase(LOCALHOST_NAME)) {
        return hostName;
      } else {
        Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
        while (interfaceEnumeration.hasMoreElements()) {
          NetworkInterface networkInterface = interfaceEnumeration.nextElement();
          if (networkInterface != null) {
            Enumeration<InetAddress> networkAddressEnumeration = networkInterface.getInetAddresses();
            while(networkAddressEnumeration.hasMoreElements()) {
              InetAddress address = networkAddressEnumeration.nextElement();
              if (address != null && !LOCALHOST_NAME.equalsIgnoreCase(address.getCanonicalHostName())) {
                return address.getCanonicalHostName();
              }
            }
          }
        }
        return LOCALHOST_NAME;
      }
    } catch (UnknownHostException | SocketException ignored) {
      LOGGER.warn("failed to get host name", ignored);
      try {
        return getHostAddress();
      } catch (Exception e) {
        return null;
      }
    }
  }
}
