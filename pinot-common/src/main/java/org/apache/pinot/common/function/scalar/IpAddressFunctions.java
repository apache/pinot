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


public class IpAddressFunctions {

  private IpAddressFunctions() {
  }

  /**
   * Returns true if ipAddress is in the subnet of ipPrefix
   * ipPrefix is in cidr format (IPv4 or IPv6)
   */
  @ScalarFunction
  public static boolean isSubnetOf(String ipPrefix, String ipAddress)
      throws UnknownHostException {
    if (!ipPrefix.contains("/")) {
      throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
    }
    byte[] address;
    byte[] argAddress;
    int subnetSize;

    String[] prefixLengthPair = ipPrefix.split("/");
    try {
      address = InetAddresses.forString(prefixLengthPair[0]).getAddress();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
    }
    subnetSize = Integer.parseInt(prefixLengthPair[1]);
    if (subnetSize < 0 || address.length * 8 < subnetSize) {
      throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
    }
    try {
      argAddress = InetAddresses.forString(ipAddress).getAddress();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid IP: " + ipAddress);
    }
    if (address.length != 4 && address.length != 16) {
      throw new IllegalArgumentException("Invalid IP: " + ipAddress);
    }
    if (argAddress.length != address.length) {
      throw new IllegalArgumentException("IP type of " + ipAddress + " is different from " + ipPrefix);
    }
    if (subnetSize == 0) {
      // all IPs are in range
      return true;
    }

    int shift;
    int numRangeBits = address.length * 8 - subnetSize;
    BigInteger arg = new BigInteger(argAddress);
    byte[] maxBits = new byte[address.length];
    for (int i = 0; i < maxBits.length; i++) {
      maxBits[i] = address[i];
    }
    // min
    for (int i = 0; i < address.length; i++) {
      if (numRangeBits > i * 8) {
        shift = (numRangeBits - i * 8) < 8 ? (numRangeBits - i * 8) : 8;
        address[address.length - 1 - i] &= -0x1 << shift;
      }
    }
    BigInteger min = new BigInteger(address);

    // max
    for (int i = 0; i < maxBits.length; i++) {
      if (numRangeBits > i * 8) {
        shift = (numRangeBits - i * 8) < 8 ? (numRangeBits - i * 8) : 8;
        maxBits[maxBits.length - 1 - i] |= ~(-0x1 << shift);
      }
    }
    BigInteger max = new BigInteger(maxBits);

    return min.compareTo(arg) <= 0 && max.compareTo(arg) >= 0;
  }
}
