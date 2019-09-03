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

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertGrouperFactoryTest {
  @Test
  public void testFromSpecNull() throws Exception {
    AlertGrouper dataFilter = AlertGrouperFactory.fromSpec(null);
    Assert.assertEquals(dataFilter.getClass(), DummyAlertGrouper.class);
  }

  @Test
  public void testAlertGrouperCreation() {
    Map<String, String> spec = new HashMap<>();
    spec.put(AlertGrouperFactory.GROUPER_TYPE_KEY, "diMenSionAL");
    AlertGrouper alertGrouper = AlertGrouperFactory.fromSpec(spec);
    Assert.assertEquals(alertGrouper.getClass(), DimensionalAlertGrouper.class);
  }
}
