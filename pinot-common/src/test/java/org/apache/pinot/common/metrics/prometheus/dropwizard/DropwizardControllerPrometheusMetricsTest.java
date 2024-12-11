/**
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
 */

package org.apache.pinot.common.metrics.prometheus.dropwizard;

import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.metrics.prometheus.ControllerPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.Test;


/**
 * Disabling tests as Pinot currently uses Yammer and these tests fail for for {@link DropwizardMetricsFactory}
 */
@Test(enabled = false) // enabled=false on class level doesn't seem to work in intellij
public class DropwizardControllerPrometheusMetricsTest extends ControllerPrometheusMetricsTest {

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new DropwizardMetricsFactory();
  }

  @Override
  protected String getConfigFile() {
    //todo: return the correct dir once this test is enabled
    return null;
  }

  @Test(dataProvider = "controllerTimers", enabled = false)
  public void timerTest(ControllerTimer controllerTimer) {
    super.timerTest(controllerTimer);
  }

  @Test(dataProvider = "controllerMeters", enabled = false)
  public void meterTest(ControllerMeter meter) {
    super.meterTest(meter);
  }

  @Test(dataProvider = "controllerGauges", enabled = false)
  public void gaugeTest(ControllerGauge controllerGauge) {
    super.gaugeTest(controllerGauge);
  }
}
