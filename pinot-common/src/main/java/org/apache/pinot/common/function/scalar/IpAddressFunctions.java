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
import java.math.BigInteger;
import java.net.UnknownHostException;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Inbuilt IP related transform functions
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
      throw new IllegalArgumentException("Invalid IP: " + ipAddress);
    }
    if (address.length != 4 && address.length != 16) {
      throw new IllegalArgumentException("Invalid IP: " + ipAddress);
    }
    return address;
  }

  /**
   * Returns true if ipAddress is in the subnet of ipPrefix (IPv4 or IPv6)
   */
  @ScalarFunction
  public static boolean isSubnetOf(String ipPrefix, String ipAddress)
      throws UnknownHostException {
    String[] prefixLengthPair = fromPrefixToPair(ipPrefix);
    byte[] addr = fromStringToBytes(prefixLengthPair[0]);
    int subnetSize = Integer.parseInt(prefixLengthPair[1]);
    if (subnetSize < 0 || addr.length * 8 < subnetSize) {
      throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
    }
    byte[] argAddress = fromStringToBytes(ipAddress);
    if (argAddress.length != addr.length) {
      throw new IllegalArgumentException("IP type of " + ipAddress + " is different from " + ipPrefix);
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
    int numRangeBits = addr.length * 8 - subnetSize;

    // create a copy of addr for computing addrMax
    byte[] maxBits = new byte[addr.length];
    for (int i = 0; i < maxBits.length; i++) {
      maxBits[i] = addr[i];
    }
    // min
    for (int i = 0; i < addr.length; i++) {
      if (numRangeBits > i * 8) {
        int shift = (numRangeBits - i * 8) < 8 ? (numRangeBits - i * 8) : 8;
        addr[addr.length - 1 - i] &= -0x1 << shift;
      }
    }
    BigInteger addrMin = new BigInteger(addr);

    // max
    for (int i = 0; i < maxBits.length; i++) {
      if (numRangeBits > i * 8) {
        int shift = (numRangeBits - i * 8) < 8 ? (numRangeBits - i * 8) : 8;
        maxBits[maxBits.length - 1 - i] |= ~(-0x1 << shift);
      }
    }
    BigInteger addrMax = new BigInteger(maxBits);
    BigInteger arg = new BigInteger(argAddress);
    return addrMin.compareTo(arg) <= 0 && addrMax.compareTo(arg) >= 0;
  }
}
