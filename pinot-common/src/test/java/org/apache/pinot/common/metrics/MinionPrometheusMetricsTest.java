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

package org.apache.pinot.common.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class MinionPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  //all exported minion metrics have this prefix
  protected static final String EXPORTED_METRIC_PREFIX = "pinot_minion_";
  protected static final String METER_PREFIX_NO_TASKS = "numberTasks";

  @DataProvider(name = "minionTimers")
  public Object[] minionTimers() {
    return MinionTimer.values();
  }

  @DataProvider(name = "minionMeters")
  public Object[] minionMeters() {
    return MinionMeter.values();
  }

  @DataProvider(name = "minionGauges")
  public Object[] minionGauges() {
    return MinionGauge.values();
  }

  @Override
  protected PinotComponent getPinotComponent() {
    return PinotComponent.MINION;
  }
}
