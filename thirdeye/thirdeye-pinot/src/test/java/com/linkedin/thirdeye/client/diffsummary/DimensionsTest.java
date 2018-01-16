package com.linkedin.thirdeye.client.diffsummary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.ListUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DimensionsTest {

  @Test
  public void testDefaultCreation() {
    Dimensions dimensions = new Dimensions();
    Assert.assertEquals(dimensions.size(), 0);
  }

  @Test
  public void testListCreation() {
    List<String> names = Arrays.asList("a", "b");
    Dimensions dimensions = new Dimensions(names);
    Assert.assertEquals(dimensions.size(), 2);
    Assert.assertEquals(dimensions.names(), names);
    Assert.assertEquals(dimensions.get(0), "a");
    Assert.assertEquals(dimensions.get(1), "b");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullListCreation() {
    new Dimensions(null);
  }

  @Test
  public void testGroupByStringsAtLevel() {
    List<String> names = Arrays.asList("a", "b");
    Dimensions dimensions = new Dimensions(names);
    List<String> subDimensions = dimensions.namesToDepth(1);

    Assert.assertTrue(ListUtils.isEqualList(subDimensions, Collections.singletonList("a")));
  }

  @Test
  public void testNamesToDepth() {
    List<String> names = Arrays.asList("a", "b");
    Dimensions dimensions = new Dimensions(names);

    Assert.assertEquals(dimensions.namesToDepth(0), Collections.<String>emptyList());
    Assert.assertEquals(dimensions.namesToDepth(1), Collections.singletonList("a"));
    Assert.assertEquals(dimensions.namesToDepth(2), names);
  }

  @Test
  public void testIsParentOf() {
    Dimensions dimensions1 = new Dimensions();
    Assert.assertFalse(dimensions1.isParentOf(null));

    Dimensions dimensions2 = new Dimensions(Arrays.asList("country"));
    Assert.assertFalse(dimensions2.isParentOf(dimensions2));
    Assert.assertTrue(dimensions1.isParentOf(dimensions2));
    Assert.assertFalse(dimensions2.isParentOf(dimensions1));

    Dimensions dimensions3 = new Dimensions(Arrays.asList("country", "page"));
    Assert.assertTrue(dimensions2.isParentOf(dimensions3));
    Assert.assertFalse(dimensions3.isParentOf(dimensions2));

    Dimensions dimensions4 = new Dimensions(Arrays.asList("page"));
    Assert.assertTrue(dimensions4.isParentOf(dimensions3));
    Assert.assertFalse(dimensions3.isParentOf(dimensions4));

    Dimensions dimensions5 = new Dimensions(Arrays.asList("random"));
    Assert.assertFalse(dimensions5.isParentOf(dimensions3));
    Assert.assertFalse(dimensions3.isParentOf(dimensions5));
  }

  @Test
  public void testEquals() {
    List<String> names = Arrays.asList("a", "b");
    Dimensions dimensions1 = new Dimensions(names);
    Dimensions dimensions2 = new Dimensions(names);

    Assert.assertTrue(dimensions1.equals(dimensions2));
  }

  @Test
  public void testHashCode() {
    List<String> names = Arrays.asList("a", "b");
    Dimensions dimensions = new Dimensions(names);
    Assert.assertEquals(dimensions.hashCode(), Objects.hash(names));
  }
}
