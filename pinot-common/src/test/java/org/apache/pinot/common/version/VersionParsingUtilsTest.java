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
package org.apache.pinot.common.version;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class VersionParsingUtilsTest {

  @Test
  public void testParseRelease() {
    ParsedVersion v = VersionParsingUtils.parse("1.2.3");
    assertNotNull(v);
    assertEquals(v.getMajor(), 1);
    assertEquals(v.getMinor(), 2);
    assertEquals(v.getPatch(), 3);
    assertFalse(v.isSnapshot());
    assertEquals(v.getRawVersion(), "1.2.3");
  }

  @Test
  public void testParseSnapshot() {
    ParsedVersion v = VersionParsingUtils.parse("1.2.3-SNAPSHOT");
    assertNotNull(v);
    assertEquals(v.getMajor(), 1);
    assertEquals(v.getMinor(), 2);
    assertEquals(v.getPatch(), 3);
    assertTrue(v.isSnapshot());
  }

  @Test
  public void testParseRc() {
    ParsedVersion v1 = VersionParsingUtils.parse("1.2.3-rc1");
    ParsedVersion v2 = VersionParsingUtils.parse("1.2.3-rc2");
    ParsedVersion v10 = VersionParsingUtils.parse("1.2.3-rc10");
    assertNotNull(v1);
    assertNotNull(v2);
    assertNotNull(v10);
    assertTrue(v1.compareTo(v2) < 0);
    // Numeric (not lexical) compare: rc10 > rc2.
    assertTrue(v2.compareTo(v10) < 0);
  }

  @Test
  public void testParseUnknownReturnsNull() {
    assertNull(VersionParsingUtils.parse(null));
    assertNull(VersionParsingUtils.parse(""));
    assertNull(VersionParsingUtils.parse("  "));
    assertNull(VersionParsingUtils.parse(PinotVersion.UNKNOWN));
    assertNull(VersionParsingUtils.parse("${project.version}"));
  }

  @Test
  public void testParseMalformedReturnsNull() {
    assertNull(VersionParsingUtils.parse("1.2"));
    assertNull(VersionParsingUtils.parse("not.a.version"));
    assertNull(VersionParsingUtils.parse("a.b.c"));
  }

  @Test
  public void testParseStrictFormatRejectsExtraQualifiers() {
    // The `-rc<N>` suffix must immediately follow the patch component; arbitrary intermediate
    // qualifiers must NOT be silently mis-parsed.
    assertNull(VersionParsingUtils.parse("1.2.3-foo-rc10"));
    assertNull(VersionParsingUtils.parse("1.2.3-bench-rc"));
    assertNull(VersionParsingUtils.parse("1.2.3-rc"));
    assertNull(VersionParsingUtils.parse("1.2.3-rcX"));
    assertNull(VersionParsingUtils.parse("1.2.3-rc1-extra"));
    assertNull(VersionParsingUtils.parse("1.2.3.4"));
    assertNull(VersionParsingUtils.parse("v1.2.3"));
    // Leading / trailing whitespace is trimmed before matching.
    assertNotNull(VersionParsingUtils.parse("  1.2.3  "));
  }

  @Test
  public void testCompareReleaseOrdering() {
    // Snapshot < rc < release within the same base version
    assertTrue(VersionParsingUtils.compare("1.2.3-SNAPSHOT", "1.2.3-rc1") < 0);
    assertTrue(VersionParsingUtils.compare("1.2.3-rc1", "1.2.3") < 0);
    assertTrue(VersionParsingUtils.compare("1.2.3-SNAPSHOT", "1.2.3") < 0);
    // Base version comparison takes precedence
    assertTrue(VersionParsingUtils.compare("1.2.3", "1.3.0") < 0);
    assertTrue(VersionParsingUtils.compare("1.2.3", "2.0.0") < 0);
    assertEquals(VersionParsingUtils.compare("1.2.3", "1.2.3"), 0);
  }

  @Test
  public void testCompareUnknownIsLeast() {
    // UNKNOWN < any parseable version
    assertTrue(VersionParsingUtils.compare(PinotVersion.UNKNOWN, "1.0.0") < 0);
    assertTrue(VersionParsingUtils.compare("1.0.0", PinotVersion.UNKNOWN) > 0);
    // Two UNKNOWNs compare equal
    assertEquals(VersionParsingUtils.compare(PinotVersion.UNKNOWN, PinotVersion.UNKNOWN), 0);
    assertEquals(VersionParsingUtils.compare(null, null), 0);
    assertEquals(VersionParsingUtils.compare(null, PinotVersion.UNKNOWN), 0);
  }

  @Test
  public void testIsKnown() {
    assertTrue(VersionParsingUtils.isKnown("1.2.3"));
    assertTrue(VersionParsingUtils.isKnown("1.2.3-SNAPSHOT"));
    assertFalse(VersionParsingUtils.isKnown(null));
    assertFalse(VersionParsingUtils.isKnown(""));
    assertFalse(VersionParsingUtils.isKnown(PinotVersion.UNKNOWN));
    assertFalse(VersionParsingUtils.isKnown("not-a-version"));
  }

  @Test
  public void testIsSnapshot() {
    assertTrue(VersionParsingUtils.isSnapshot("1.2.3-SNAPSHOT"));
    assertFalse(VersionParsingUtils.isSnapshot("1.2.3"));
    assertFalse(VersionParsingUtils.isSnapshot("1.2.3-rc1"));
    assertFalse(VersionParsingUtils.isSnapshot(null));
    assertFalse(VersionParsingUtils.isSnapshot(PinotVersion.UNKNOWN));
  }

  @Test
  public void testIsAtLeast() {
    ParsedVersion v123 = VersionParsingUtils.parse("1.2.3");
    ParsedVersion v120 = VersionParsingUtils.parse("1.2.0");
    assertNotNull(v123);
    assertNotNull(v120);
    assertTrue(v123.isAtLeast(v120));
    assertFalse(v120.isAtLeast(v123));
    assertTrue(v123.isAtLeast(v123));
  }
}
