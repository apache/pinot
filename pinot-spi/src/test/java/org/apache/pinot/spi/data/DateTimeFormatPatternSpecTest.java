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
package org.apache.pinot.spi.data;

import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

public class DateTimeFormatPatternSpecTest {

  @Test
  public void testValidateFormat() {
    DateTimeFormatPatternSpec.validateFormat("yyyy-MM-dd");
    DateTimeFormatPatternSpec.validateFormat("yyyyMMdd tz(CST)");
    DateTimeFormatPatternSpec.validateFormat("yyyyMMdd HH tz(GMT+0700)");
    DateTimeFormatPatternSpec.validateFormat("yyyyMMddHH tz(America/Chicago)");

    // Unknown tz is treated as UTC
    DateTimeFormatPatternSpec.validateFormat("yyyyMMdd tz(CSEMT)");
    DateTimeFormatPatternSpec.validateFormat("yyyyMMdd tz(GMT+5000)");
    DateTimeFormatPatternSpec.validateFormat("yyyyMMddHH tz(HAHA/Chicago)");

    // invalid chars will throw
    assertThrows(IllegalStateException.class, () -> DateTimeFormatPatternSpec.validateFormat("yyyc-MM-dd"));
    assertThrows(IllegalStateException.class, () -> DateTimeFormatPatternSpec.validateFormat("yyyy-MM-dd ff(a)"));
  }
}
