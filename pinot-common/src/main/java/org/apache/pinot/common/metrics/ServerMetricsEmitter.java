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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotMeter;


/**
 * Default {@link MseMetricsEmitter} implementation that forwards every emission to the
 * live {@link ServerMetrics} singleton.
 *
 * <p>The {@link ServerMetrics} singleton is resolved at call time via
 * {@link ServerMetrics#get()}, so the order in which {@link ServerMetrics#register} and
 * {@link MseMetricsEmitter#register} are invoked does not affect correctness. This is
 * intentional: it eliminates the NOOP-binding hazard that previously affected MSE call
 * sites which cached {@link PinotMeter} handles at construction time.
 *
 * <p>This class is stateless and safe to share across threads.
 */
public class ServerMetricsEmitter implements MseMetricsEmitter {

  @Override
  public void addMeteredGlobalValue(ServerMeter meter, long unitCount) {
    ServerMetrics.get().addMeteredGlobalValue(meter, unitCount);
  }

  @Override
  public PinotMeter getMeteredValue(ServerMeter meter) {
    return ServerMetrics.get().getMeteredValue(meter);
  }

  @Override
  public void addTimedValue(ServerTimer timer, long duration, TimeUnit timeUnit) {
    ServerMetrics.get().addTimedValue(timer, duration, timeUnit);
  }
}
