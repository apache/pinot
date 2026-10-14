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

package org.apache.pinot.plugin.metrics.dropwizard;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DropwizardMetricNameTest {
  @Test
  public void testMetricClassDoesNotChangeRegistryKey() {
    String name = "pinot.compatibilityMetric";
    DropwizardMetricName brokerName = new DropwizardMetricName(BrokerMetrics.class, name);
    DropwizardMetricName serverName = new DropwizardMetricName(ServerMetrics.class, name);
    assertEquals(brokerName, serverName);
    assertEquals(brokerName, new DropwizardMetricName(name));
    assertEquals(brokerName.hashCode(), serverName.hashCode());
    assertEquals(brokerName.getMetricName(), name);
    assertEquals(brokerName.toString(), name);
  }

  @Test
  public void testEqualsContract() {
    EqualsVerifier.forClass(DropwizardMetricName.class).withIgnoredFields("_metricClass").verify();
  }
}
