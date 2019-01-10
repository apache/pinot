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
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBaseAlertFilter {
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  // test set up double, Double, string for alpha_beta
  // also test missing input parameter, will use default value defined in the specified class
  @Test
  public void testSetAlphaBetaParamter(){
    AnomalyFunctionDTO anomalyFunctionSpec = DaoTestUtils.getTestFunctionAlphaBetaAlertFilterSpec(metricName, collection);
    Map<String, String> alertfilter = anomalyFunctionSpec.getAlertFilter();
    AlphaBetaAlertFilter alphaBetaAlertFilter = new AlphaBetaAlertFilter();
    alphaBetaAlertFilter.setParameters(alertfilter);
    Assert.assertEquals(alphaBetaAlertFilter.getAlpha(), Double.valueOf(alertfilter.get("alpha")));
    Assert.assertEquals(alphaBetaAlertFilter.getBeta(), Double.valueOf(alertfilter.get("beta")));
    Assert.assertEquals(alphaBetaAlertFilter.getType(), alertfilter.get("type"));
    Assert.assertEquals(alphaBetaAlertFilter.getThreshold(), Double.valueOf(alertfilter.get("threshold")));

    // test scientific decimal
    double threshold = 1E-10;
    alertfilter.put("threshold", String.valueOf(threshold));
    alphaBetaAlertFilter.setParameters(alertfilter);
    Assert.assertEquals(alphaBetaAlertFilter.getThreshold(), Double.valueOf(alertfilter.get("threshold")));

    // test missing field
    alertfilter.remove("threshold");
    AlphaBetaAlertFilter alphaBetaAlertFilter1 = new AlphaBetaAlertFilter();
    alphaBetaAlertFilter1.setParameters(alertfilter);
    Assert.assertEquals(alphaBetaAlertFilter1.getThreshold(), Double.valueOf(AlphaBetaAlertFilter.DEFAULT_THRESHOLD));
  }

}