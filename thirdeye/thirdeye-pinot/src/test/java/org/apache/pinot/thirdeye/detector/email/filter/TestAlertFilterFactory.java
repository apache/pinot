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

package org.apache.pinot.thirdeye.detector.email.filter;

import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.testng.Assert;
import org.testng.annotations.Test;



public class TestAlertFilterFactory {
  private static AlertFilterFactory alertFilterFactory;
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  private TestAlertFilterFactory() {
    String mappingsPath = ClassLoader.getSystemResource("sample-alertfilter.properties").getPath();
    alertFilterFactory = new AlertFilterFactory(mappingsPath);
  }

  @Test
  public void fromSpecNullAlertFilter() throws Exception {
    AlertFilter alertFilter = alertFilterFactory.fromSpec(null);
    Assert.assertEquals(alertFilter.getClass(), DummyAlertFilter.class);
  }

  @Test
  public void testFromAnomalyFunctionSpecToAlertFilter() throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = DaoTestUtils.getTestFunctionSpec(metricName, collection);
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    Assert.assertEquals(alertFilter.getClass(), DummyAlertFilter.class);

    anomalyFunctionSpec = DaoTestUtils.getTestFunctionAlphaBetaAlertFilterSpec(metricName, collection);
    alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    Assert.assertEquals(alertFilter.getClass(), AlphaBetaAlertFilter.class);
  }
}