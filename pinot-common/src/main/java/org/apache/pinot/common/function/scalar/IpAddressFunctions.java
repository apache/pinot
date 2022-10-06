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

import com.google.common.net.InetAddresses;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import java.math.BigInteger;
import java.net.UnknownHostException;
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

  private static String[] fromPrefixToPair(String ipPrefix) {
    if (!ipPrefix.contains("/")) {
      throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
    }
    return ipPrefix.split("/");
  }

  private static byte[] fromStringToBytes(String ipAddress) {
    byte[] address;
    try {
      address = InetAddresses.forString(ipAddress).getAddress();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid IP: " + ipAddress + " due to " + e.getMessage());
    }
    if (address.length != 4 && address.length != 16) {
      throw new IllegalArgumentException(
          "Valid IP address should have length 32 (IPv4) or 128 (IPv6). Invalid IP " + ipAddress + " has length "
              + address.length);
    }
    return address;
  }

  private static byte[] getAddrMin(byte[] addr, int subnetSize) {
    int numRangeBits = addr.length * 8 - subnetSize;
    for (int i = 0; i < addr.length; i++) {
      if (numRangeBits > i * 8) {
        int shift = (numRangeBits - i * 8) < 8 ? (numRangeBits - i * 8) : 8;
        addr[addr.length - 1 - i] &= -0x1 << shift;
      }
    }
    return addr;
  }

  private static byte[] getAddrMax(byte[] addr, int subnetSize) {
    int numRangeBits = addr.length * 8 - subnetSize;
    for (int i = 0; i < addr.length; i++) {
      if (numRangeBits > i * 8) {
        int shift = (numRangeBits - i * 8) < 8 ? (numRangeBits - i * 8) : 8;
        addr[addr.length - 1 - i] |= ~(-0x1 << shift);
      }
    }
    return addr;
  }

  @ScalarFunction
  public static boolean isSubnetOf(String ipPrefix, String ipAddress) {
    IPAddress prefix = new IPAddressString(ipPrefix).getAddress().toPrefixBlock();
    return prefix.contains(new IPAddressString(ipAddress).getAddress());
  }
  /**
   * Returns true if ipAddress is in the subnet of ipPrefix (IPv4 or IPv6)
   */
  @ScalarFunction
  public static boolean isSubnetOfV1(String ipPrefix, String ipAddress)
      throws UnknownHostException {
    String[] prefixLengthPair = fromPrefixToPair(ipPrefix);
    byte[] addr = fromStringToBytes(prefixLengthPair[0]);
    int subnetSize = Integer.parseInt(prefixLengthPair[1]);
    if (subnetSize < 0 || addr.length * 8 < subnetSize) {
      throw new IllegalArgumentException("Invalid subnet size " + subnetSize
          + ". IPv4 subnet size should be in range [0, 32]. IPv6 subnet size should be in range [0, 128].");
    }
    byte[] argAddress = fromStringToBytes(ipAddress);
    if (argAddress.length != addr.length) {
      String argType = argAddress.length == 32 ? "IPv4" : "IPv6";
      String addrType = addr.length == 32 ? "IPv4" : "IPv6";
      throw new IllegalArgumentException(ipAddress + " is " + argType + ", but " + ipPrefix + " is " + addrType);
    }
    if (subnetSize == 0) {
      // all IPs are in range
      return true;
    }

    /**
     * Alg for checking if IP address arg is in the subnet of IP address addr with subnetSize
     * which is the number of network bits (= number of 1's in the subnet mask).
     *
     * Given
     * addr: [---network bits---][---random bits---]
     *
     * Compute
     * addrMin: [---network bits---][---all 0's---]
     *    [---network bits---][---random bits---]
     *    &
     *   [-----all 1's------][-----all 0's------]
     *
     * addrMax: [---network bits---][---all 1's---]
     *    [---network bits---][---random bits---]
     *    |
     *   [-----all 0's------][-----all 1's------]
     *
     * Check
     * addrMin <= arg <= addMax
     */
    // create a copy of addr for computing addrMax
    byte[] maxBits = new byte[addr.length];
    byte[] minBits = new byte[addr.length];
    for (int i = 0; i < maxBits.length; i++) {
      minBits[i] = addr[i];
      maxBits[i] = addr[i];
    }
    // min
    minBits = getAddrMin(minBits, subnetSize);
    BigInteger addrMin = new BigInteger(minBits);

    // max
    maxBits = getAddrMax(maxBits, subnetSize);
    BigInteger addrMax = new BigInteger(maxBits);
    BigInteger arg = new BigInteger(argAddress);
    return addrMin.compareTo(arg) <= 0 && addrMax.compareTo(arg) >= 0;
  }
}
