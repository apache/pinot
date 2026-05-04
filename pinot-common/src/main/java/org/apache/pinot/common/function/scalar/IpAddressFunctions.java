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
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv6.IPv6Address;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Scalar functions for IP address manipulation.
 *
 * <p>All functions are stateless and thread-safe.
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

  /**
   * Returns true if the input string is a valid IPv4 address (not a CIDR prefix).
   *
   * @param ipString the string to validate
   * @return true if the string is a valid IPv4 address without a prefix length
   */
  @ScalarFunction
  public static boolean isIPv4String(String ipString) {
    if (ipString.isEmpty()) {
      return false;
    }
    IPAddressString addr = new IPAddressString(ipString);
    return addr.isValid() && addr.isIPv4() && !addr.isPrefixed();
  }

  /**
   * Returns true if the input string is a valid IPv6 address (not a CIDR prefix).
   *
   * @param ipString the string to validate
   * @return true if the string is a valid IPv6 address without a prefix length
   */
  @ScalarFunction
  public static boolean isIPv6String(String ipString) {
    if (ipString.isEmpty()) {
      return false;
    }
    IPAddressString addr = new IPAddressString(ipString);
    return addr.isValid() && addr.isIPv6() && !addr.isPrefixed();
  }

  /**
   * Converts an IPv4 address string to its unsigned 32-bit integer representation as a long.
   *
   * @param ipString IPv4 address string (e.g., "192.168.1.1")
   * @return unsigned 32-bit integer value (e.g., 3232235777)
   * @throws IllegalArgumentException if the string is not a valid IPv4 address
   */
  @ScalarFunction
  public static long ipv4ToLong(String ipString) {
    IPAddress addr = getAddress(ipString);
    if (!addr.isIPv4()) {
      throw new IllegalArgumentException("Not an IPv4 address: " + ipString);
    }
    return Integer.toUnsignedLong(addr.toIPv4().intValue());
  }

  /**
   * Converts an unsigned 32-bit integer (as long) to an IPv4 address string.
   *
   * @param value unsigned 32-bit integer value (0 to 4294967295)
   * @return IPv4 address string in dotted-decimal notation
   * @throws IllegalArgumentException if the value is outside the valid IPv4 range
   */
  @ScalarFunction
  public static String longToIpv4(long value) {
    if (value < 0 || value > 0xFFFFFFFFL) {
      throw new IllegalArgumentException("Value out of IPv4 range: " + value);
    }
    return new IPv4Address((int) value).toCanonicalString();
  }

  /**
   * Converts an IPv6 address string to its 16-byte representation.
   *
   * @param ipString IPv6 address string (e.g., "2001:db8::1")
   * @return 16-byte array containing the IPv6 address
   * @throws IllegalArgumentException if the string is not a valid IPv6 address
   */
  @ScalarFunction
  public static byte[] ipv6ToBytes(String ipString) {
    IPAddress addr = getAddress(ipString);
    if (!addr.isIPv6()) {
      throw new IllegalArgumentException("Not an IPv6 address: " + ipString);
    }
    return addr.toIPv6().getBytes();
  }

  /**
   * Converts a 16-byte array to an IPv6 address string in canonical form.
   *
   * @param bytes 16-byte array containing the IPv6 address
   * @return IPv6 address string in canonical form (e.g., "2001:db8::1")
   * @throws IllegalArgumentException if the array is not exactly 16 bytes
   */
  @ScalarFunction
  public static String bytesToIpv6(byte[] bytes) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException("IPv6 address requires exactly 16 bytes, got " + bytes.length);
    }
    return new IPv6Address(bytes).toCanonicalString();
  }

  /**
   * Maps an IPv4 address to its IPv4-mapped IPv6 representation.
   *
   * @param ipString IPv4 address string (e.g., "192.168.1.1")
   * @return IPv4-mapped IPv6 address string (e.g., "::ffff:c0a8:101")
   * @throws IllegalArgumentException if the string is not a valid IPv4 address
   */
  @ScalarFunction
  public static String ipv4ToIpv6(String ipString) {
    IPAddress addr = getAddress(ipString);
    if (!addr.isIPv4()) {
      throw new IllegalArgumentException("Not an IPv4 address: " + ipString);
    }
    return addr.toIPv4().toIPv6().toCanonicalString();
  }

  /**
   * Returns the min and max addresses of an IPv4 CIDR range as a two-element string array.
   *
   * @param cidr IPv4 CIDR string (e.g., "192.168.1.0/24")
   * @return two-element array [min, max] (e.g., ["192.168.1.0", "192.168.1.255"])
   * @throws IllegalArgumentException if the CIDR is invalid or not IPv4
   */
  @ScalarFunction
  public static String[] ipv4CIDRToRange(String cidr) {
    IPAddress prefix = getPrefix(cidr);
    if (!prefix.isIPv4()) {
      throw new IllegalArgumentException("Not an IPv4 CIDR: " + cidr);
    }
    String min = prefix.getLower().withoutPrefixLength().toCanonicalString();
    String max = prefix.getUpper().withoutPrefixLength().toCanonicalString();
    return new String[]{min, max};
  }

  /**
   * Returns the IP version family: 4 for IPv4, 6 for IPv6.
   *
   * @param ipString IP address string (e.g., "192.168.1.1" or "2001:db8::1")
   * @return 4 or 6
   * @throws IllegalArgumentException if the string is not a valid IP address
   */
  @ScalarFunction(names = {"ipFamily", "ip_family"})
  public static int ipFamily(String ipString) {
    IPAddress addr = getAddress(ipString);
    return addr.isIPv4() ? 4 : 6;
  }

  /**
   * Extracts the prefix length (mask length) from a CIDR notation string.
   *
   * @param cidr CIDR string (e.g., "192.168.1.0/24" or "2001:db8::/32")
   * @return the prefix length (e.g., 24)
   * @throws IllegalArgumentException if the input is not a valid CIDR prefix
   */
  @ScalarFunction(names = {"ipMaskLen", "ip_mask_len"})
  public static int ipMaskLen(String cidr) {
    IPAddress prefix = getPrefix(cidr);
    return prefix.getNetworkPrefixLength();
  }

  /**
   * Builds a network or host mask for the given prefix.
   *
   * @param prefix the validated CIDR prefix
   * @param invert false for network mask (1s in network portion), true for host mask (1s in host portion)
   * @return mask as an IP address string
   */
  private static String buildMask(IPAddress prefix, boolean invert) {
    int prefixLen = prefix.getNetworkPrefixLength();
    int bitCount = prefix.getBitCount();
    byte[] maskBytes = new byte[bitCount / 8];
    int fullBytes = prefixLen / 8;
    int remainingBits = prefixLen % 8;
    // Build network mask: 1s for network portion, 0s for host portion
    for (int i = 0; i < fullBytes; i++) {
      maskBytes[i] = (byte) 0xFF;
    }
    if (remainingBits > 0 && fullBytes < maskBytes.length) {
      maskBytes[fullBytes] = (byte) (0xFF << (8 - remainingBits));
    }
    // Invert for host mask
    if (invert) {
      for (int i = 0; i < maskBytes.length; i++) {
        maskBytes[i] = (byte) ~maskBytes[i];
      }
    }
    if (prefix.isIPv4()) {
      return new IPv4Address(maskBytes).toCanonicalString();
    }
    return new IPv6Address(maskBytes).toCanonicalString();
  }

  /**
   * Returns the network mask for a CIDR prefix as an IP address string.
   *
   * @param cidr CIDR string (e.g., "192.168.1.0/24")
   * @return network mask string (e.g., "255.255.255.0")
   * @throws IllegalArgumentException if the input is not a valid CIDR prefix
   */
  @ScalarFunction(names = {"ipNetmask", "ip_netmask"})
  public static String ipNetmask(String cidr) {
    return buildMask(getPrefix(cidr), false);
  }

  /**
   * Returns the host mask (inverse of network mask) for a CIDR prefix as an IP address string.
   *
   * @param cidr CIDR string (e.g., "192.168.1.0/24")
   * @return host mask string (e.g., "0.0.0.255")
   * @throws IllegalArgumentException if the input is not a valid CIDR prefix
   */
  @ScalarFunction(names = {"ipHostmask", "ip_hostmask"})
  public static String ipHostmask(String cidr) {
    return buildMask(getPrefix(cidr), true);
  }
}
