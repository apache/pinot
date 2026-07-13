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
package org.apache.pinot.spi.utils;

import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests UTF-8 sizing contracts used by segment metadata and compression statistics.
public class Utf8UtilsTest {
  @Test
  public void testEncodedLengthWithReplacementMatchesJdkEncoding() {
    for (String value : new String[]{"ascii", "caf\u00e9", "\uD83D\uDE00", "\uD800", "\uDC00", "a\uD800b"}) {
      assertEquals(Utf8Utils.encodedLengthWithReplacement(value), value.getBytes(StandardCharsets.UTF_8).length,
          "Unexpected serialized UTF-8 length for: " + value);
    }
  }
}
