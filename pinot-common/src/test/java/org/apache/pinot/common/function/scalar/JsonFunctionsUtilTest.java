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

/**
 * Tests for utility methods in JsonFunctions class
 */
public class JsonFunctionsUtilTest {

  @Test
  public void testConvertToDotNotation() {
    // Test null and empty cases
    assertEquals(JsonFunctions.convertToDotNotation(null), "");
    assertEquals(JsonFunctions.convertToDotNotation(""), "");
    assertEquals(JsonFunctions.convertToDotNotation("$"), "");

    // Test single level keys
    assertEquals(JsonFunctions.convertToDotNotation("$['a']"), "a");
    assertEquals(JsonFunctions.convertToDotNotation("$['name']"), "name");

    // Test array indices
    assertEquals(JsonFunctions.convertToDotNotation("$[0]"), "0");
    assertEquals(JsonFunctions.convertToDotNotation("$[123]"), "123");

    // Test multi-level object keys
    assertEquals(JsonFunctions.convertToDotNotation("$['a']['b']"), "a.b");
    assertEquals(JsonFunctions.convertToDotNotation("$['user']['profile']['name']"), "user.profile.name");

    // Test mixed object keys and array indices
    assertEquals(JsonFunctions.convertToDotNotation("$['users'][0]['name']"), "users.0.name");
    assertEquals(JsonFunctions.convertToDotNotation("$[0]['data'][1]['value']"), "0.data.1.value");

    // Test complex nested structure
    assertEquals(JsonFunctions.convertToDotNotation("$['a']['b'][2]['c']['d']"), "a.b.2.c.d");

    // Test special characters in keys
    assertEquals(JsonFunctions.convertToDotNotation("$['field-with-dash']"), "field-with-dash");
    assertEquals(JsonFunctions.convertToDotNotation("$['field_with_underscores']"), "field_with_underscores");
    assertEquals(JsonFunctions.convertToDotNotation("$['field with spaces']"), "field with spaces");
    assertEquals(JsonFunctions.convertToDotNotation("$['field.with.dots']"), "field.with.dots");

    // Test edge cases with brackets
    assertEquals(JsonFunctions.convertToDotNotation("$['']"), "");
    assertEquals(JsonFunctions.convertToDotNotation("$['a']['']"), "a.");

    // Test malformed paths (should still process what it can)
    assertEquals(JsonFunctions.convertToDotNotation("$['a']["), "a[");
    assertEquals(JsonFunctions.convertToDotNotation("$['a']['b"), "a['b");

    // Test non-standard but possible patterns
    assertEquals(JsonFunctions.convertToDotNotation("$['0']"), "0");
    assertEquals(JsonFunctions.convertToDotNotation("$['1']['key']"), "1.key");
  }

  @Test
  public void testGetKeyDepth() {
    // Test null and empty cases
    assertEquals(JsonFunctions.getKeyDepth(null), 0);
    assertEquals(JsonFunctions.getKeyDepth(""), 0);

    // Test JsonPath format
    assertEquals(JsonFunctions.getKeyDepth("$['a']"), 1);
    assertEquals(JsonFunctions.getKeyDepth("$['a']['b']"), 2);
    assertEquals(JsonFunctions.getKeyDepth("$['a']['b']['c']"), 3);
    assertEquals(JsonFunctions.getKeyDepth("$['user']['profile']['settings']['theme']"), 4);

    // Test array indices in JsonPath format
    assertEquals(JsonFunctions.getKeyDepth("$[0]"), 1);
    assertEquals(JsonFunctions.getKeyDepth("$[0]['name']"), 2);
    assertEquals(JsonFunctions.getKeyDepth("$['users'][0]['profile']"), 3);

    // Test dot notation format
    assertEquals(JsonFunctions.getKeyDepth("a"), 1);
    assertEquals(JsonFunctions.getKeyDepth("a.b"), 2);
    assertEquals(JsonFunctions.getKeyDepth("a.b.c"), 3);
    assertEquals(JsonFunctions.getKeyDepth("user.profile.settings.theme"), 4);
    assertEquals(JsonFunctions.getKeyDepth("users.0.name"), 3);
    assertEquals(JsonFunctions.getKeyDepth("data.0.items.1.value"), 5);

    // Test single keys without dots or brackets
    assertEquals(JsonFunctions.getKeyDepth("name"), 1);
    assertEquals(JsonFunctions.getKeyDepth("id"), 1);
    assertEquals(JsonFunctions.getKeyDepth("123"), 1);

    // Test edge cases with dots
    assertEquals(JsonFunctions.getKeyDepth("."), 0); // Split of "." results in empty array
    assertEquals(JsonFunctions.getKeyDepth("a."), 1); // "a." appears to be treated as single key
    assertEquals(JsonFunctions.getKeyDepth(".b"), 2); // Empty string before dot
    assertEquals(JsonFunctions.getKeyDepth("a..b"), 3); // Empty string between dots

    // Test mixed cases that might appear in real data
    assertEquals(JsonFunctions.getKeyDepth("field-with-dash"), 1);
    assertEquals(JsonFunctions.getKeyDepth("field_with_underscores"), 1);
    assertEquals(JsonFunctions.getKeyDepth("field.with.dots.in.name"), 5);

    // Test complex JsonPath with multiple levels
    assertEquals(JsonFunctions.getKeyDepth("$['a']['b']['c']['d']['e']"), 5);
    assertEquals(JsonFunctions.getKeyDepth("$[0][1][2]"), 3);

    // Test malformed JsonPath (should still count brackets)
    assertEquals(JsonFunctions.getKeyDepth("$['a']["), 2);
    assertEquals(JsonFunctions.getKeyDepth("$[0]['incomplete"), 2);
  }
}
