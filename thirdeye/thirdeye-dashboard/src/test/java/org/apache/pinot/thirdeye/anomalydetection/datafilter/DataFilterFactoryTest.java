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

package org.apache.pinot.thirdeye.anomalydetection.datafilter;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataFilterFactoryTest {

  @Test
  public void testFromSpecNull() throws Exception {
    DataFilter dataFilter = DataFilterFactory.fromSpec(null);
    Assert.assertEquals(dataFilter.getClass(), DummyDataFilter.class);
  }

  @Test
  public void testDataFilterCreation() {
    Map<String, String> spec = new HashMap<>();
    spec.put(AverageThresholdDataFilter.METRIC_NAME_KEY, "metricName");
    spec.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    DataFilter dataFilter = DataFilterFactory.fromSpec(spec);
    Assert.assertEquals(dataFilter.getClass(), AverageThresholdDataFilter.class);
  }
}
