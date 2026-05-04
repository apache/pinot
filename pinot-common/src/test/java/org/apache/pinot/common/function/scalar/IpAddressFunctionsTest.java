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

import inet.ipaddr.IPAddressString;
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

  // ==================== Tests for isIPv4String ====================

  @Test
  public void testIsIPv4String() {
    assertTrue(IpAddressFunctions.isIPv4String("192.168.1.1"));
    assertTrue(IpAddressFunctions.isIPv4String("0.0.0.0"));
    assertTrue(IpAddressFunctions.isIPv4String("255.255.255.255"));
    assertTrue(IpAddressFunctions.isIPv4String("10.0.0.1"));

    assertFalse(IpAddressFunctions.isIPv4String("2001:db8::1"));
    assertFalse(IpAddressFunctions.isIPv4String("::1"));
    assertFalse(IpAddressFunctions.isIPv4String("not-an-ip"));
    assertFalse(IpAddressFunctions.isIPv4String(""));
    assertFalse(IpAddressFunctions.isIPv4String("999.999.999.999"));
    assertFalse(IpAddressFunctions.isIPv4String("192.168.1.1/24"));
  }

  // ==================== Tests for isIPv6String ====================

  @Test
  public void testIsIPv6String() {
    assertTrue(IpAddressFunctions.isIPv6String("2001:db8::1"));
    assertTrue(IpAddressFunctions.isIPv6String("::1"));
    assertTrue(IpAddressFunctions.isIPv6String("fe80::1"));
    assertTrue(IpAddressFunctions.isIPv6String("::"));

    assertFalse(IpAddressFunctions.isIPv6String("192.168.1.1"));
    assertFalse(IpAddressFunctions.isIPv6String("not-an-ip"));
    assertFalse(IpAddressFunctions.isIPv6String(""));
    assertFalse(IpAddressFunctions.isIPv6String("2001:db8::1/64"));
  }

  // ==================== Tests for ipv4ToLong ====================

  @Test
  public void testIpv4ToLong() {
    assertEquals(IpAddressFunctions.ipv4ToLong("0.0.0.0"), 0L);
    assertEquals(IpAddressFunctions.ipv4ToLong("0.0.0.1"), 1L);
    assertEquals(IpAddressFunctions.ipv4ToLong("0.0.1.0"), 256L);
    assertEquals(IpAddressFunctions.ipv4ToLong("192.168.1.1"), 3232235777L);
    assertEquals(IpAddressFunctions.ipv4ToLong("255.255.255.255"), 4294967295L);
    assertEquals(IpAddressFunctions.ipv4ToLong("10.0.0.1"), 167772161L);
  }

  @Test
  public void testIpv4ToLongInvalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv4ToLong("2001:db8::1"));
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv4ToLong("not-an-ip"));
  }

  // ==================== Tests for longToIpv4 ====================

  @Test
  public void testLongToIpv4() {
    assertEquals(IpAddressFunctions.longToIpv4(0L), "0.0.0.0");
    assertEquals(IpAddressFunctions.longToIpv4(1L), "0.0.0.1");
    assertEquals(IpAddressFunctions.longToIpv4(256L), "0.0.1.0");
    assertEquals(IpAddressFunctions.longToIpv4(3232235777L), "192.168.1.1");
    assertEquals(IpAddressFunctions.longToIpv4(4294967295L), "255.255.255.255");
  }

  @Test
  public void testLongToIpv4Invalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.longToIpv4(-1L));
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.longToIpv4(4294967296L));
  }

  @Test
  public void testIpv4RoundTrip() {
    String[] addresses = {"0.0.0.0", "192.168.1.1", "10.20.30.40", "255.255.255.255", "127.0.0.1"};
    for (String addr : addresses) {
      assertEquals(IpAddressFunctions.longToIpv4(IpAddressFunctions.ipv4ToLong(addr)), addr);
    }
  }

  // ==================== Tests for ipv6ToBytes / bytesToIpv6 ====================

  @Test
  public void testIpv6ToBytes() {
    byte[] bytes = IpAddressFunctions.ipv6ToBytes("::1");
    assertEquals(bytes.length, 16);
    assertEquals(bytes[15], 1);
    for (int i = 0; i < 15; i++) {
      assertEquals(bytes[i], 0);
    }
  }

  @Test
  public void testIpv6ToBytesInvalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv6ToBytes("192.168.1.1"));
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv6ToBytes("not-an-ip"));
  }

  @Test
  public void testBytesToIpv6() {
    byte[] loopback = new byte[16];
    loopback[15] = 1;
    assertEquals(IpAddressFunctions.bytesToIpv6(loopback), "::1");
  }

  @Test
  public void testBytesToIpv6Invalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.bytesToIpv6(new byte[4]));
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.bytesToIpv6(new byte[15]));
  }

  @Test
  public void testIpv6RoundTrip() {
    String[] addresses = {"::1", "2001:db8::1", "fe80::1", "::"};
    for (String addr : addresses) {
      assertEquals(IpAddressFunctions.bytesToIpv6(IpAddressFunctions.ipv6ToBytes(addr)), addr);
    }
  }

  // ==================== Tests for ipv4ToIpv6 ====================

  @Test
  public void testIpv4ToIpv6() {
    assertEquals(IpAddressFunctions.ipv4ToIpv6("192.168.1.1"), "::ffff:c0a8:101");
  }

  @Test
  public void testIpv4ToIpv6Invalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv4ToIpv6("2001:db8::1"));
  }

  // ==================== Tests for ipv4CIDRToRange ====================

  @Test
  public void testIpv4CIDRToRange() {
    String[] range = IpAddressFunctions.ipv4CIDRToRange("192.168.1.0/24");
    assertEquals(range[0], "192.168.1.0");
    assertEquals(range[1], "192.168.1.255");

    range = IpAddressFunctions.ipv4CIDRToRange("10.0.0.0/8");
    assertEquals(range[0], "10.0.0.0");
    assertEquals(range[1], "10.255.255.255");

    range = IpAddressFunctions.ipv4CIDRToRange("192.168.1.1/32");
    assertEquals(range[0], "192.168.1.1");
    assertEquals(range[1], "192.168.1.1");
  }

  @Test
  public void testIpv4CIDRToRangeInvalid() {
    // Not a prefix
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv4CIDRToRange("192.168.1.0"));
    // IPv6 CIDR rejected
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipv4CIDRToRange("2001:db8::/32"));
  }

  // ==================== Tests for ipFamily ====================

  @Test
  public void testIpFamily() {
    assertEquals(IpAddressFunctions.ipFamily("192.168.1.1"), 4);
    assertEquals(IpAddressFunctions.ipFamily("10.0.0.1"), 4);
    assertEquals(IpAddressFunctions.ipFamily("0.0.0.0"), 4);
    assertEquals(IpAddressFunctions.ipFamily("2001:db8::1"), 6);
    assertEquals(IpAddressFunctions.ipFamily("::1"), 6);
    assertEquals(IpAddressFunctions.ipFamily("fe80::1"), 6);
  }

  @Test
  public void testIpFamilyInvalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipFamily("not-an-ip"));
  }

  // ==================== Tests for ipMaskLen ====================

  @Test
  public void testIpMaskLen() {
    assertEquals(IpAddressFunctions.ipMaskLen("192.168.1.0/24"), 24);
    assertEquals(IpAddressFunctions.ipMaskLen("10.0.0.0/8"), 8);
    assertEquals(IpAddressFunctions.ipMaskLen("192.168.1.1/32"), 32);
    assertEquals(IpAddressFunctions.ipMaskLen("0.0.0.0/0"), 0);
    assertEquals(IpAddressFunctions.ipMaskLen("2001:db8::/32"), 32);
    assertEquals(IpAddressFunctions.ipMaskLen("2001:db8::/64"), 64);
    assertEquals(IpAddressFunctions.ipMaskLen("::1/128"), 128);
  }

  @Test
  public void testIpMaskLenInvalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipMaskLen("192.168.1.0"));
  }

  // ==================== Tests for ipNetmask ====================

  @Test
  public void testIpNetmask() {
    assertEquals(IpAddressFunctions.ipNetmask("192.168.1.0/24"), "255.255.255.0");
    assertEquals(IpAddressFunctions.ipNetmask("10.0.0.0/8"), "255.0.0.0");
    assertEquals(IpAddressFunctions.ipNetmask("192.168.1.0/16"), "255.255.0.0");
    assertEquals(IpAddressFunctions.ipNetmask("192.168.1.1/32"), "255.255.255.255");
    assertEquals(IpAddressFunctions.ipNetmask("0.0.0.0/0"), "0.0.0.0");
    assertEquals(IpAddressFunctions.ipNetmask("192.168.1.0/25"), "255.255.255.128");
  }

  @Test
  public void testIpNetmaskIpv6() {
    // /64 → first 8 bytes all 0xFF, rest 0
    assertEquals(IpAddressFunctions.ipNetmask("2001:db8::/64"), "ffff:ffff:ffff:ffff::");
    // /128 → all 0xFF
    assertEquals(IpAddressFunctions.ipNetmask("::1/128"), "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
    // /0 → all 0
    assertEquals(IpAddressFunctions.ipNetmask("::/0"), "::");
  }

  @Test
  public void testIpNetmaskInvalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipNetmask("192.168.1.0"));
  }

  // ==================== Tests for ipHostmask ====================

  @Test
  public void testIpHostmask() {
    assertEquals(IpAddressFunctions.ipHostmask("192.168.1.0/24"), "0.0.0.255");
    assertEquals(IpAddressFunctions.ipHostmask("10.0.0.0/8"), "0.255.255.255");
    assertEquals(IpAddressFunctions.ipHostmask("192.168.1.0/16"), "0.0.255.255");
    assertEquals(IpAddressFunctions.ipHostmask("192.168.1.1/32"), "0.0.0.0");
    assertEquals(IpAddressFunctions.ipHostmask("0.0.0.0/0"), "255.255.255.255");
    assertEquals(IpAddressFunctions.ipHostmask("192.168.1.0/25"), "0.0.0.127");
  }

  @Test
  public void testIpHostmaskIpv6() {
    // /64 → first 8 bytes all 0, rest 0xFF
    assertEquals(IpAddressFunctions.ipHostmask("2001:db8::/64"), "::ffff:ffff:ffff:ffff");
    // /128 → all 0
    assertEquals(IpAddressFunctions.ipHostmask("::1/128"), "::");
    // /0 → all 0xFF
    assertEquals(IpAddressFunctions.ipHostmask("::/0"), "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
  }

  @Test
  public void testIpHostmaskInvalid() {
    assertThrows(IllegalArgumentException.class, () -> IpAddressFunctions.ipHostmask("192.168.1.0"));
  }

  // ==================== Netmask/Hostmask consistency ====================

  @Test
  public void testNetmaskHostmaskConsistency() {
    // For any CIDR, netmask and hostmask must be bitwise complements:
    // (netmask | hostmask) == all 0xFF, (netmask & hostmask) == all 0x00
    String[] cidrs = {"192.168.1.0/24", "10.0.0.0/8", "172.16.0.0/12", "192.168.1.0/25", "0.0.0.0/0",
        "192.168.1.1/32"};
    for (String cidr : cidrs) {
      String netmask = IpAddressFunctions.ipNetmask(cidr);
      String hostmask = IpAddressFunctions.ipHostmask(cidr);
      byte[] netBytes = new IPAddressString(netmask).getAddress().getBytes();
      byte[] hostBytes = new IPAddressString(hostmask).getAddress().getBytes();
      assertEquals(netBytes.length, hostBytes.length);
      for (int i = 0; i < netBytes.length; i++) {
        assertEquals((byte) (netBytes[i] | hostBytes[i]), (byte) 0xFF,
            "netmask | hostmask must be all 1s at byte " + i + " for " + cidr);
        assertEquals((byte) (netBytes[i] & hostBytes[i]), (byte) 0x00,
            "netmask & hostmask must be all 0s at byte " + i + " for " + cidr);
      }
    }
  }
}
