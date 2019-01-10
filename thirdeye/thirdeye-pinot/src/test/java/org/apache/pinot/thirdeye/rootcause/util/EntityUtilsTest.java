/*
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

package org.apache.pinot.thirdeye.rootcause.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.rootcause.impl.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javax.validation.constraints.AssertTrue;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EntityUtilsTest {
  private static final String URN_TEST_VECTOR_DECODED = " A BCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()-_=+`~[{]}\\|\'\";:/?,<. > ";
  private static final String URN_TEST_VECTOR_ENCODED = "%20A%20BCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890!%40%23%24%25%5E%26*()-_%3D%2B%60~%5B%7B%5D%7D%5C%7C'%22%3B%3A%2F%3F%2C%3C.%20%3E%20";

  @Test
  public void testEncodeURN() {
    Assert.assertEquals(EntityUtils.encodeURNComponent(URN_TEST_VECTOR_DECODED), URN_TEST_VECTOR_ENCODED);
  }

  @Test
  public void testDecodeURN() {
    Assert.assertEquals(EntityUtils.decodeURNComponent(URN_TEST_VECTOR_ENCODED), URN_TEST_VECTOR_DECODED);
  }

  @Test
  public void testParseUrnString() {
    ParsedUrn expected = new ParsedUrn(
        Arrays.asList("thirdeye", "metric", "12345"),
        new HashSet<FilterPredicate>()
    );

    ParsedUrn actual = EntityUtils.parseUrnString("thirdeye:metric:12345");

    Assert.assertEquals(actual, expected);
  }


  @Test
  public void testParseUrnStringWithFilters() {
    ParsedUrn expected = new ParsedUrn(
        Arrays.asList("thirdeye", "metric", "12345"),
        new HashSet<>(Arrays.asList(
            new FilterPredicate("key", "==", "value"),
            new FilterPredicate("key", "=", "value"),
            new FilterPredicate("key", "!=", "value"),
            new FilterPredicate("key", "<=", "value"),
            new FilterPredicate("key", ">=", "value"),
            new FilterPredicate("key", "<", "value"),
            new FilterPredicate("key", ">", "value")
        )));

    ParsedUrn actual = EntityUtils.parseUrnString("thirdeye:metric:12345:key=value:key==value:key!=value:key<=value:key>=value:key<value:key>value", 3);

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testParseUrnStringWithFiltersAmbiguous() {
    ParsedUrn expected = new ParsedUrn(
        Arrays.asList("thirdeye", "metric", "12345"),
        new HashSet<>(Arrays.asList(
            new FilterPredicate("key", "=", "value"),
            new FilterPredicate("key", "!=", ":::"),
            new FilterPredicate("key", "<=", "value")
        )));

    ParsedUrn actual = EntityUtils.parseUrnString("thirdeye:metric:12345:key=value:key!=::::key<=value", 3);

    Assert.assertEquals(actual, expected);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseUrnStringWithFiltersAmbiguousInvalid() {
    EntityUtils.parseUrnString("thirdeye:metric:12345:key:value:key!=double:colon:key=value", 3);
  }

  @Test
  public void testParseUrnStringWithType() {
    EntityType type = new EntityType("thirdeye:entity:");

    ParsedUrn expected = new ParsedUrn(
        Arrays.asList("thirdeye", "entity", "abc"),
        Collections.singleton(new FilterPredicate("key", "=", "value"))
        );

    ParsedUrn actual = EntityUtils.parseUrnString("thirdeye:entity:abc:key=value", type, 3);

    Assert.assertEquals(actual, expected);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseUrnStringWithTypeFail() {
    EntityType type = new EntityType("thirdeye:entity:");
    EntityUtils.parseUrnString("thirdeye:notentity:abc:key=value", type, 3);
  }

  @Test
  public void testAssertPrefixOnly() {
    new ParsedUrn(Arrays.asList("thirdeye", "entity", "abc")).assertPrefixOnly();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAssertPrefixOnlyFail() {
    new ParsedUrn(Arrays.asList("thirdeye", "entity", "abc"), Collections.singleton(new FilterPredicate("key", "=", "value"))).assertPrefixOnly();
  }

  @Test
  public void testToFilterStratification() {
    ParsedUrn parsedUrn = new ParsedUrn(
        Arrays.asList("thirdeye", "entity"),
        new HashSet<>(Arrays.asList(
            new FilterPredicate("key", "=", "value"),
            new FilterPredicate("key", "!=", ":::"),
            new FilterPredicate("key", "<=", "value")
        )));

    Assert.assertEquals(parsedUrn.toFilters().get("key"), Arrays.asList("!:::", "<=value", "value"));
  }

  @Test
  public void testEncodeDimensionsSingle() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("a", "b");

    List<String> encoded = EntityUtils.encodeDimensions(filters);
    Assert.assertEquals(encoded.size(), 1);
    Assert.assertEquals(encoded.get(0), "a%3Db");
  }

  @Test
  public void testEncodeDimensionsMultiKeySorting() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("c", "d");
    filters.put("a", "b");

    List<String> encoded = EntityUtils.encodeDimensions(filters);
    Assert.assertEquals(encoded.size(), 2);
    Assert.assertEquals(encoded.get(0), "a%3Db");
    Assert.assertEquals(encoded.get(1), "c%3Dd");
  }

  @Test
  public void testEncodeDimensionsMultiKeyMultiValueSorting() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("c", "d");
    filters.put("a", "b");
    filters.put("c", "e");

    List<String> encoded = EntityUtils.encodeDimensions(filters);
    Assert.assertEquals(encoded.size(), 3);
    Assert.assertEquals(encoded.get(0), "a%3Db");
    Assert.assertEquals(encoded.get(1), "c%3Dd");
    Assert.assertEquals(encoded.get(2), "c%3De");
  }

  @Test
  public void testEncodeDimensionsExclusions() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("a", "A");
    filters.put("b", "!B");
    filters.put("c", "<C");
    filters.put("d", ">D");
    filters.put("e", "<=E");
    filters.put("f", ">=F");

    List<String> encoded = EntityUtils.encodeDimensions(filters);
    Assert.assertEquals(encoded.size(), 6);
    Assert.assertEquals(encoded.get(0), "a%3DA");
    Assert.assertEquals(encoded.get(1), "b!%3DB");
    Assert.assertEquals(encoded.get(2), "c%3CC");
    Assert.assertEquals(encoded.get(3), "d%3ED");
    Assert.assertEquals(encoded.get(4), "e%3C%3DE");
    Assert.assertEquals(encoded.get(5), "f%3E%3DF");
  }

  @Test
  public void testEncodeDimensionsNonExclusions() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("a", "xA");
    filters.put("b", "x!B");
    filters.put("c", "x<C");
    filters.put("d", "x>D");
    filters.put("e", "x<=E");
    filters.put("f", "x>=F");

    List<String> encoded = EntityUtils.encodeDimensions(filters);
    Assert.assertEquals(encoded.size(), 6);
    Assert.assertEquals(encoded.get(0), "a%3DxA");
    Assert.assertEquals(encoded.get(1), "b%3Dx!B");
    Assert.assertEquals(encoded.get(2), "c%3Dx%3CC");
    Assert.assertEquals(encoded.get(3), "d%3Dx%3ED");
    Assert.assertEquals(encoded.get(4), "e%3Dx%3C%3DE");
    Assert.assertEquals(encoded.get(5), "f%3Dx%3E%3DF");
  }
}
