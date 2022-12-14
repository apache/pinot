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
package org.apache.pinot.plugin.metrics.yammer;

import org.testng.Assert;
import org.testng.annotations.Test;


public class YammerSettableGaugeTest {
  @Test
  public void testCreateDropwizardSettableGaugeImplWithValue() {
    YammerSettableGauge<Long> yammerSettableGauge = new YammerSettableGauge<>(1L);
    setAngCheck(yammerSettableGauge);
  }

  @Test
  public void testCreateDropwizardSettableGaugeImplWithValueSupplier() {
    YammerSettableGauge<Long> yammerSettableGauge = new YammerSettableGauge<>(() -> 1L);
    setAngCheck(yammerSettableGauge);
  }

  private void setAngCheck(YammerSettableGauge<Long> yammerSettableGauge) {
    Assert.assertEquals(yammerSettableGauge.value(), Long.valueOf(1L));
    yammerSettableGauge.setValue(2L);
    Assert.assertEquals(yammerSettableGauge.value(), Long.valueOf(2L));
    yammerSettableGauge.setValueSupplier(() -> 3L);
    Assert.assertEquals(yammerSettableGauge.value(), Long.valueOf(3L));
  }
}
