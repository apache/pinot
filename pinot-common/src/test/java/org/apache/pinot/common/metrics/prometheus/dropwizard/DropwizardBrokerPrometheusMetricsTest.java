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

import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.metrics.prometheus.BrokerPrometheusMetricsTest;
import org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.Test;


/**
 * Disabling tests as Pinot currently uses Yammer and these tests fail for for {@link DropwizardMetricsFactory}
 */
@Test(enabled = false) // enabled=false on class level doesn't seem to work in intellij
public class DropwizardBrokerPrometheusMetricsTest extends BrokerPrometheusMetricsTest {
  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new DropwizardMetricsFactory();
  }

  @Override
  protected String getConfigFile() {
    //todo: return the correct dir once this test is enabled
    return null;
  }

  @Test(dataProvider = "brokerGauges", enabled = false)
  public void timerTest(BrokerTimer timer) {
    super.timerTest(timer);
  }

  @Test(dataProvider = "brokerMeters", enabled = false)
  public void meterTest(BrokerMeter meter) {
    super.meterTest(meter);
  }

  @Test(dataProvider = "brokerGauges", enabled = false)
  public void gaugeTest(BrokerGauge gauge) {
    super.gaugeTest(gauge);
  }
}
