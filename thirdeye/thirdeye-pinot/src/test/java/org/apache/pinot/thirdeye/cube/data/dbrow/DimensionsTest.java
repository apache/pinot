/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.cube.data.dbrow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections4.ListUtils;
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
