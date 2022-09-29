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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.pinot.spi.annotations.ScalarFunction;
import com.google.common.net.InetAddresses;

import static java.lang.Math.max;
import static java.lang.Math.min;


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
    try {
      argAddress = InetAddresses.forString(ipAddress).getAddress();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid IP: " + ipAddress);
    }
    BigInteger arg = new BigInteger(argAddress);
    byte[] copy = new byte[address.length];
    for (int i = 0; i < copy.length; i++) {
      copy[i] = address[i];
    }

    if (address.length == 4) {
      // IPv4
      if (subnetSize < 0 || 32 < subnetSize) {
        throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
      }
      if (subnetSize == 0) {
        return true;
      }
      int shift;
      for (int i = 0; i < 4; i++) {
        if (32 - subnetSize > i * 8) {
          shift = (32 - subnetSize - i * 8) < 8 ? (32 - subnetSize - i * 8) : 8;
          address[3 - i] &= -0x1 << shift;
        }
      }
      BigInteger min = new BigInteger(address);

      for (int i = 0; i < 4; i++) {
        if (32 - subnetSize > i * 8) {
          shift = (32 - subnetSize - i * 8) < 8 ? (32 - subnetSize - i * 8) : 8;
          copy[3 - i] |= ~(-0x1 << shift);
        }
      }
      BigInteger max = new BigInteger(copy);
      return min.compareTo(arg) <= 0 && max.compareTo(arg) >= 0;
    } else if (address.length == 16) {
      // IPv6
      if (subnetSize < 0 || 128 < subnetSize) {
        throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
      }
      if (subnetSize == 0) {
        return true;
      }
      int shift;
      for (int i = 0; i < 16; i++) {
        if (128 - subnetSize > i * 8) {
          shift = (128 - subnetSize - i * 8) < 8 ? (128 - subnetSize - i * 8) : 8;
          address[15 - i] &= -0x1 << shift;
        }
      }
      BigInteger min = new BigInteger(address);

      for (int i = 0; i < 16; i++) {
        if (128 - subnetSize > i * 8) {
          shift = (128 - subnetSize - i * 8) < 8 ? (128 - subnetSize - i * 8) : 8;
          copy[15 - i] |= ~(-0x1 << shift);
        }
      }
      BigInteger max = new BigInteger(copy);
      return min.compareTo(arg) <= 0 && max.compareTo(arg) >= 0;
    } else {
      throw new IllegalArgumentException("Invalid IP prefix: " + ipPrefix);
    }
  }
}
