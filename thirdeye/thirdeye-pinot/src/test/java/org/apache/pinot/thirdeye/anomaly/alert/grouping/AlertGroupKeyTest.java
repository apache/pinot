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

package org.apache.pinot.thirdeye.anomaly.alert.grouping;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertGroupKeyTest {
  @Test
  public void testEqualsEmptyKey() {
    AlertGroupKey<DimensionMap> alertGroupKey = AlertGroupKey.emptyKey();
    Assert.assertEquals(alertGroupKey.getKey(), null);
    Assert.assertEquals(alertGroupKey, AlertGroupKey.emptyKey());
  }

  @Test
  public void testEqualsDimensionKey() {
    DimensionMap dimensionMap1 = new DimensionMap();
    dimensionMap1.put("D1", "V1");
    AlertGroupKey<DimensionMap> alertGroupKey1 = new AlertGroupKey<>(dimensionMap1);

    DimensionMap dimensionMap2 = new DimensionMap();
    dimensionMap2.put("D1", "V1");
    AlertGroupKey<DimensionMap> alertGroupKey2 = new AlertGroupKey<>(dimensionMap2);

    Assert.assertEquals(alertGroupKey1, alertGroupKey2);
  }
}
