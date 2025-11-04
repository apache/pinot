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

package org.apache.pinot.common.function.scalar;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.PrefixLenException;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Inbuilt IP related transform functions
 *
 * Functions added:
 * isSubnetOf(String ipPrefix, String ipAddress) --> boolean
 *
 * Functions to add:
 * ipPrefix(String ipAddress, int prefixBits) -> String ipPrefix
 * ipSubnetMin(String ipPrefix) -> String ipMin
 * ipSubnetMax(String ipPrefix) -> String ipMax
 */
public class IpAddressFunctions {

  private IpAddressFunctions() {
  }

  /**
   * Validates IP prefix prefixStr and returns IPAddress if validated
   */
  private static IPAddress getPrefix(String prefixStr) {
    IPAddress prefixAddr = getAddress(prefixStr);
    if (!prefixAddr.isPrefixed()) {
      throw new IllegalArgumentException("IP Address " + prefixStr + " should be prefixed.");
    }
    try {
      return prefixAddr.toPrefixBlock();
    } catch (PrefixLenException e) {
      throw e;
    }
  }

  /**
   * Validates IP address ipString and returns IPAddress if validated
   */
  private static IPAddress getAddress(String ipString) {
    try {
      return new IPAddressString(ipString).toAddress();
    } catch (AddressStringException e) {
      throw new IllegalArgumentException("Invalid IP Address format for " + ipString);
    }
  }

  @ScalarFunction
  public static boolean isSubnetOf(String ipPrefix, String ipAddress) {
    IPAddress prefix = getPrefix(ipPrefix);
    IPAddress ip = getAddress(ipAddress);
    if (ip.isPrefixed()) {
      throw new IllegalArgumentException("IP Address " + ipAddress + " should not be prefixed.");
    }
    return prefix.contains(ip);
  }

  /**
   * Returns the IP prefix for the given IP address with the specified prefix length.
   *
   * @param ipAddress IP address string (e.g., "192.168.1.100")
   * @param prefixBits Prefix length in bits (e.g., 24 for /24 subnet)
   * @return IP prefix string in CIDR notation (e.g., "192.168.1.0/24")
   * @throws IllegalArgumentException if the IP address is already prefixed or if prefix length is invalid
   */
  @ScalarFunction
  public static String ipPrefix(String ipAddress, int prefixBits) {
    IPAddress ip = getAddress(ipAddress);
    if (ip.isPrefixed()) {
      throw new IllegalArgumentException("IP Address " + ipAddress + " should not be prefixed.");
    }
    if (prefixBits < 0 || prefixBits > ip.getBitCount()) {
      throw new IllegalArgumentException(
          "Invalid prefix length " + prefixBits + " for IP address " + ipAddress
          + ". Must be between 0 and " + ip.getBitCount());
    }
    try {
      return ip.setPrefixLength(prefixBits).toPrefixBlock().toString();
    } catch (PrefixLenException e) {
      throw new IllegalArgumentException("Invalid prefix length: " + prefixBits, e);
    }
  }

  /**
   * Returns the minimum (first) IP address in the given subnet.
   *
   * @param ipPrefix IP prefix string in CIDR notation (e.g., "192.168.1.0/24")
   * @return Minimum IP address in the subnet (e.g., "192.168.1.0")
   * @throws IllegalArgumentException if the IP address is not a valid prefix
   */
  @ScalarFunction
  public static String ipSubnetMin(String ipPrefix) {
    IPAddress prefix = getPrefix(ipPrefix);
    return prefix.getLower().withoutPrefixLength().toCanonicalString();
  }

  /**
   * Returns the maximum (last) IP address in the given subnet.
   *
   * @param ipPrefix IP prefix string in CIDR notation (e.g., "192.168.1.0/24")
   * @return Maximum IP address in the subnet (e.g., "192.168.1.255")
   * @throws IllegalArgumentException if the IP address is not a valid prefix
   */
  @ScalarFunction
  public static String ipSubnetMax(String ipPrefix) {
    IPAddress prefix = getPrefix(ipPrefix);
    return prefix.getUpper().withoutPrefixLength().toCanonicalString();
  }
}
