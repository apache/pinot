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

package org.apache.pinot.thirdeye.anomaly.alert.grouping.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SizeSeverityAlertGroupFilterTest {
  @Test
  public void testCreate() {
    // Create size severity filter
    Map<String, String> sizeSeverityFilterParams = new HashMap<>();
    sizeSeverityFilterParams.put(AlertGroupFilterFactory.GROUP_FILTER_TYPE_KEY, "sIze_SevEriTy");
    sizeSeverityFilterParams.put(SizeSeverityAlertGroupFilter.THRESHOLD_KEY, "10");
    String overrideThresholdJson = "{\"dimensionName1,dimensionName11\":3,\"dimensionName2\":4}";
    sizeSeverityFilterParams.put(SizeSeverityAlertGroupFilter.OVERRIDE_THRESHOLD_KEY, overrideThresholdJson);

    // Initialize filter from factory
    AlertGroupFilter filter = AlertGroupFilterFactory.fromSpec(sizeSeverityFilterParams);
    Assert.assertTrue(filter.getClass() == SizeSeverityAlertGroupFilter.class);
    SizeSeverityAlertGroupFilter sizeSeverityFilter = (SizeSeverityAlertGroupFilter) filter;

    // Construct golden override threshold
    Map<Set<String>, Integer> overrideThreshold = new HashMap<>();
    Set<String> groupName1 = new HashSet<>();
    groupName1.add("dimensionName1");
    groupName1.add("dimensionName11");
    overrideThreshold.put(groupName1, 3);

    Set<String> groupName2 = new HashSet<>();
    groupName2.add("dimensionName2");
    overrideThreshold.put(groupName2, 4);

    // Compare results
    Assert.assertEquals(sizeSeverityFilter.getOverrideThreshold(), overrideThreshold);
    Assert.assertEquals(sizeSeverityFilter.getThreshold(), 10);
  }
}
