package com.linkedin.thirdeye.rootcause.util;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.rootcause.impl.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

}
