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
import org.testng.Assert;
import org.testng.annotations.Test;

public class DimensionValuesTest {

  @Test
  public void testDefaultCreation() {
    DimensionValues dimensionValues = new DimensionValues();
    Assert.assertEquals(dimensionValues.size(), 0);
  }

  @Test
  public void testListCreation() {
    List<String> names = Arrays.asList("a", "b");
    DimensionValues dimensionValues = new DimensionValues(names);
    Assert.assertEquals(dimensionValues.size(), 2);
    Assert.assertEquals(dimensionValues.values(), names);
    Assert.assertEquals(dimensionValues.get(0), "a");
    Assert.assertEquals(dimensionValues.get(1), "b");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullListCreation() {
    new DimensionValues(null);
  }

  @Test
  public void testCompareTo() throws Exception {
    List<String> dimValueString1 = Collections.singletonList("a");
    List<String> dimValueString2 = Collections.singletonList("b");
    List<String> dimValueString3 = Arrays.asList("a", "b");
    List<String> dimValueString4 = Arrays.asList("s", "t");
    List<String> dimValueString5 = Arrays.asList("s", "u");
    List<String> dimValueString6 = Collections.singletonList("s");
    DimensionValues dimensionValues0 = new DimensionValues();
    DimensionValues dimensionValues1 = new DimensionValues(dimValueString1);
    DimensionValues dimensionValues1Dup = new DimensionValues(dimValueString1);
    DimensionValues dimensionValues2 = new DimensionValues(dimValueString2);
    DimensionValues dimensionValues3 = new DimensionValues(dimValueString3);
    DimensionValues dimensionValues4 = new DimensionValues(dimValueString4);
    DimensionValues dimensionValues5 = new DimensionValues(dimValueString5);
    DimensionValues dimensionValues6 = new DimensionValues(dimValueString6);

    Assert.assertTrue(dimensionValues1.compareTo(dimensionValues1Dup) == 0);
    Assert.assertTrue(dimensionValues1Dup.compareTo(dimensionValues1) == 0);

    Assert.assertTrue(dimensionValues1.compareTo(dimensionValues0) < 0);
    Assert.assertTrue(dimensionValues0.compareTo(dimensionValues1) > 0);

    Assert.assertTrue(dimensionValues1.compareTo(dimensionValues2) < 0);
    Assert.assertTrue(dimensionValues2.compareTo(dimensionValues1) > 0);

    Assert.assertTrue(dimensionValues1.compareTo(dimensionValues3) > 0);
    Assert.assertTrue(dimensionValues3.compareTo(dimensionValues1) < 0);

    Assert.assertTrue(dimensionValues1.compareTo(dimensionValues4) < 0);
    Assert.assertTrue(dimensionValues4.compareTo(dimensionValues1) > 0);

    Assert.assertTrue(dimensionValues3.compareTo(dimensionValues4) < 0);
    Assert.assertTrue(dimensionValues4.compareTo(dimensionValues3) > 0);

    Assert.assertTrue(dimensionValues4.compareTo(dimensionValues5) < 0);
    Assert.assertTrue(dimensionValues5.compareTo(dimensionValues4) > 0);

    Assert.assertTrue(dimensionValues6.compareTo(dimensionValues3) > 0);
    Assert.assertTrue(dimensionValues3.compareTo(dimensionValues6) < 0);
  }
}
