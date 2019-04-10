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
 *
 */

package org.apache.pinot.thirdeye.detection.components;

import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spec.DurationAnomalyFilterSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionTestUtils.*;


public class DurationAnomalyFilterTest {
  @Test
  public void testIsQualified() {
    AnomalyFilter anomalyFilter = new DurationAnomalyFilter();
    DurationAnomalyFilterSpec spec = new DurationAnomalyFilterSpec();
    spec.setMaxDuration("PT3H");
    spec.setMinDuration("PT2H");
    anomalyFilter.init(spec, new DefaultInputDataFetcher(new MockDataProvider(), -1));
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547168400000L)), false);
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547172000000L)), true);
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547175600000L)), true);
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547179200000L)), false);
  }

  @Test
  public void testDefaultQualified() {
    AnomalyFilter anomalyFilter = new DurationAnomalyFilter();
    DurationAnomalyFilterSpec spec = new DurationAnomalyFilterSpec();
    anomalyFilter.init(spec, new DefaultInputDataFetcher(new MockDataProvider(), -1));
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547168400000L)), true);
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547172000000L)), true);
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547175600000L)), true);
    Assert.assertEquals(anomalyFilter.isQualified(makeAnomaly(1547164800000L, 1547179200000L)), true);
  }

}
