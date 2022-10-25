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
}
