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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Tests for IP address functions
 */
public class IpAddressFunctionsTest {

  // ==================== Tests for isSubnetOf ====================

  @Test
  public void testIsSubnetOf() {
    // IPv4 - Basic subnet membership tests
    assertTrue(IpAddressFunctions.isSubnetOf("192.168.1.0/24", "192.168.1.100"));
    assertTrue(IpAddressFunctions.isSubnetOf("192.168.1.0/24", "192.168.1.0"));
    assertTrue(IpAddressFunctions.isSubnetOf("192.168.1.0/24", "192.168.1.255"));
    assertFalse(IpAddressFunctions.isSubnetOf("192.168.1.0/24", "192.168.2.1"));

    // IPv4 - Different subnet sizes
    assertTrue(IpAddressFunctions.isSubnetOf("10.0.0.0/8", "10.20.30.40"));
    assertTrue(IpAddressFunctions.isSubnetOf("172.16.0.0/12", "172.16.5.10"));
    assertFalse(IpAddressFunctions.isSubnetOf("172.16.0.0/12", "172.32.0.1"));

    // IPv6 - Basic tests
    assertTrue(IpAddressFunctions.isSubnetOf("2001:db8::/32", "2001:db8::1"));
    assertTrue(IpAddressFunctions.isSubnetOf("2001:db8::/32", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"));
    assertFalse(IpAddressFunctions.isSubnetOf("2001:db8::/32", "2001:db9::1"));

    // Single host subnet (/32 for IPv4, /128 for IPv6)
    assertTrue(IpAddressFunctions.isSubnetOf("192.168.1.1/32", "192.168.1.1"));
    assertFalse(IpAddressFunctions.isSubnetOf("192.168.1.1/32", "192.168.1.2"));
  }

  @Test
  public void testIsSubnetOfInvalidInputs() {
    // IP address should not be prefixed
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.isSubnetOf("192.168.1.0/24", "192.168.1.1/32"));

    // Prefix must be prefixed
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.isSubnetOf("192.168.1.0", "192.168.1.1"));

    // Invalid IP formats
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.isSubnetOf("192.168.1.0/24", "invalid-ip"));
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.isSubnetOf("invalid-prefix/24", "192.168.1.1"));
  }

  // ==================== Tests for ipPrefix ====================

  @Test
  public void testIpPrefix() {
    // IPv4 - Standard subnet sizes
    assertEquals(IpAddressFunctions.ipPrefix("192.168.1.100", 24), "192.168.1.0/24");
    assertEquals(IpAddressFunctions.ipPrefix("10.20.30.40", 16), "10.20.0.0/16");
    assertEquals(IpAddressFunctions.ipPrefix("172.16.5.10", 8), "172.0.0.0/8");
    assertEquals(IpAddressFunctions.ipPrefix("203.0.113.50", 32), "203.0.113.50/32");

    // IPv4 - Edge cases
    assertEquals(IpAddressFunctions.ipPrefix("255.255.255.255", 0), "0.0.0.0/0");
    assertEquals(IpAddressFunctions.ipPrefix("192.168.1.1", 30), "192.168.1.0/30");
    assertEquals(IpAddressFunctions.ipPrefix("10.0.0.128", 25), "10.0.0.128/25");

    // IPv6 - Standard prefixes
    assertEquals(IpAddressFunctions.ipPrefix("2001:db8::1", 64), "2001:db8::/64");
    assertEquals(IpAddressFunctions.ipPrefix("2001:db8::1", 32), "2001:db8::/32");
    assertEquals(IpAddressFunctions.ipPrefix("2001:db8:abcd:ef01::1", 48), "2001:db8:abcd::/48");
    assertEquals(IpAddressFunctions.ipPrefix("2001:db8::1", 128), "2001:db8::1/128");

    // IPv6 - Edge cases
    assertEquals(IpAddressFunctions.ipPrefix("::1", 128), "::1/128");
    assertEquals(IpAddressFunctions.ipPrefix("fe80::1", 10), "fe80::/10");
  }

  @Test
  public void testIpPrefixInvalidInputs() {
    // IP address should not be already prefixed
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipPrefix("192.168.1.0/24", 16));

    // Negative prefix length
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipPrefix("192.168.1.1", -1));

    // Prefix length too large for IPv4 (max 32)
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipPrefix("192.168.1.1", 33));

    // Prefix length too large for IPv6 (max 128)
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipPrefix("2001:db8::1", 129));

    // Invalid IP address format
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipPrefix("invalid-ip", 24));
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipPrefix("999.999.999.999", 24));
  }

  // ==================== Tests for ipSubnetMin ====================

  @Test
  public void testIpSubnetMin() {
    // IPv4 - Standard subnets
    assertEquals(IpAddressFunctions.ipSubnetMin("192.168.1.0/24"), "192.168.1.0");
    assertEquals(IpAddressFunctions.ipSubnetMin("10.0.0.0/8"), "10.0.0.0");
    assertEquals(IpAddressFunctions.ipSubnetMin("172.16.0.0/12"), "172.16.0.0");
    assertEquals(IpAddressFunctions.ipSubnetMin("192.168.1.128/25"), "192.168.1.128");

    // IPv4 - Single host
    assertEquals(IpAddressFunctions.ipSubnetMin("192.168.1.1/32"), "192.168.1.1");

    // IPv4 - Full range
    assertEquals(IpAddressFunctions.ipSubnetMin("0.0.0.0/0"), "0.0.0.0");

    // IPv6 - Standard subnets
    assertEquals(IpAddressFunctions.ipSubnetMin("2001:db8::/32"), "2001:db8::");
    assertEquals(IpAddressFunctions.ipSubnetMin("2001:db8::/64"), "2001:db8::");
    assertEquals(IpAddressFunctions.ipSubnetMin("fe80::/10"), "fe80::");

    // IPv6 - Single host
    assertEquals(IpAddressFunctions.ipSubnetMin("2001:db8::1/128"), "2001:db8::1");

    // IPv6 - Link local
    assertEquals(IpAddressFunctions.ipSubnetMin("fe80::/64"), "fe80::");
  }

  @Test
  public void testIpSubnetMinInvalidInputs() {
    // Must be a prefixed address
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipSubnetMin("192.168.1.0"));

    // Invalid IP format
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipSubnetMin("invalid-ip/24"));
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipSubnetMin("999.999.999.999/24"));
  }

  // ==================== Tests for ipSubnetMax ====================

  @Test
  public void testIpSubnetMax() {
    // IPv4 - Standard subnets
    assertEquals(IpAddressFunctions.ipSubnetMax("192.168.1.0/24"), "192.168.1.255");
    assertEquals(IpAddressFunctions.ipSubnetMax("10.0.0.0/8"), "10.255.255.255");
    assertEquals(IpAddressFunctions.ipSubnetMax("172.16.0.0/12"), "172.31.255.255");
    assertEquals(IpAddressFunctions.ipSubnetMax("192.168.1.128/25"), "192.168.1.255");

    // IPv4 - Single host
    assertEquals(IpAddressFunctions.ipSubnetMax("192.168.1.1/32"), "192.168.1.1");

    // IPv4 - Full range
    assertEquals(IpAddressFunctions.ipSubnetMax("0.0.0.0/0"), "255.255.255.255");

    // IPv4 - Small subnets
    assertEquals(IpAddressFunctions.ipSubnetMax("192.168.1.0/30"), "192.168.1.3");
    assertEquals(IpAddressFunctions.ipSubnetMax("10.0.0.0/31"), "10.0.0.1");

    // IPv6 - Standard subnets
    assertTrue(IpAddressFunctions.ipSubnetMax("2001:db8::/32").contains("ffff"));
    assertTrue(IpAddressFunctions.ipSubnetMax("2001:db8::/64").contains("ffff"));

    // IPv6 - Single host
    assertEquals(IpAddressFunctions.ipSubnetMax("2001:db8::1/128"), "2001:db8::1");

    // IPv6 - Verify it's actually the max
    String ipv6Max = IpAddressFunctions.ipSubnetMax("2001:db8::/32");
    assertTrue(ipv6Max.startsWith("2001:db8:"));
  }

  @Test
  public void testIpSubnetMaxInvalidInputs() {
    // Must be a prefixed address
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipSubnetMax("192.168.1.0"));

    // Invalid IP format
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipSubnetMax("invalid-ip/24"));
    assertThrows(IllegalArgumentException.class,
        () -> IpAddressFunctions.ipSubnetMax("999.999.999.999/24"));
  }

  // ==================== Integration Tests ====================

  @Test
  public void testCombinedFunctions() {
    // Test that ipPrefix output works with ipSubnetMin/Max
    String prefix = IpAddressFunctions.ipPrefix("192.168.1.100", 24);
    assertEquals(prefix, "192.168.1.0/24");
    assertEquals(IpAddressFunctions.ipSubnetMin(prefix), "192.168.1.0");
    assertEquals(IpAddressFunctions.ipSubnetMax(prefix), "192.168.1.255");

    // Test that isSubnetOf works with ipPrefix
    String subnet = IpAddressFunctions.ipPrefix("10.20.30.40", 16);
    assertTrue(IpAddressFunctions.isSubnetOf(subnet, "10.20.50.60"));
    assertFalse(IpAddressFunctions.isSubnetOf(subnet, "10.21.0.1"));

    // IPv6 combined test
    String ipv6Prefix = IpAddressFunctions.ipPrefix("2001:db8::1234", 64);
    assertEquals(ipv6Prefix, "2001:db8::/64");
    assertEquals(IpAddressFunctions.ipSubnetMin(ipv6Prefix), "2001:db8::");
    assertTrue(IpAddressFunctions.isSubnetOf(ipv6Prefix, "2001:db8::ffff"));
  }

  @Test
  public void testBoundaryConditions() {
    // Test /31 subnet (2 IPs)
    assertEquals(IpAddressFunctions.ipSubnetMin("192.168.1.0/31"), "192.168.1.0");
    assertEquals(IpAddressFunctions.ipSubnetMax("192.168.1.0/31"), "192.168.1.1");

    // Test /30 subnet (4 IPs)
    assertEquals(IpAddressFunctions.ipSubnetMin("192.168.1.0/30"), "192.168.1.0");
    assertEquals(IpAddressFunctions.ipSubnetMax("192.168.1.0/30"), "192.168.1.3");

    // Test very large subnet
    assertEquals(IpAddressFunctions.ipSubnetMin("0.0.0.0/1"), "0.0.0.0");
    assertEquals(IpAddressFunctions.ipSubnetMax("0.0.0.0/1"), "127.255.255.255");

    // Test IPv6 /127 (2 IPs)
    String ipv6Subnet127 = "2001:db8::/127";
    assertEquals(IpAddressFunctions.ipSubnetMin(ipv6Subnet127), "2001:db8::");
  }

  @Test
  public void testPrivateIPRanges() {
    // Test common private IP ranges
    // 10.0.0.0/8
    String prefix10 = IpAddressFunctions.ipPrefix("10.5.10.15", 8);
    assertEquals(IpAddressFunctions.ipSubnetMin(prefix10), "10.0.0.0");
    assertEquals(IpAddressFunctions.ipSubnetMax(prefix10), "10.255.255.255");

    // 172.16.0.0/12
    String prefix172 = IpAddressFunctions.ipPrefix("172.20.5.10", 12);
    assertEquals(IpAddressFunctions.ipSubnetMin(prefix172), "172.16.0.0");
    assertEquals(IpAddressFunctions.ipSubnetMax(prefix172), "172.31.255.255");

    // 192.168.0.0/16
    String prefix192 = IpAddressFunctions.ipPrefix("192.168.50.100", 16);
    assertEquals(IpAddressFunctions.ipSubnetMin(prefix192), "192.168.0.0");
    assertEquals(IpAddressFunctions.ipSubnetMax(prefix192), "192.168.255.255");
  }

  @Test
  public void testIPv6SpecialAddresses() {
    // Loopback
    assertEquals(IpAddressFunctions.ipPrefix("::1", 128), "::1/128");

    // Link-local
    String linkLocal = IpAddressFunctions.ipPrefix("fe80::1", 64);
    assertEquals(linkLocal, "fe80::/64");
    assertEquals(IpAddressFunctions.ipSubnetMin(linkLocal), "fe80::");

    // Documentation prefix
    String docPrefix = IpAddressFunctions.ipPrefix("2001:db8::1", 32);
    assertEquals(docPrefix, "2001:db8::/32");
  }
}
