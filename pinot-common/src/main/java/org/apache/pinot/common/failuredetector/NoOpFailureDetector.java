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
package org.apache.pinot.common.failuredetector;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;


@ThreadSafe
public class NoOpFailureDetector implements FailureDetector {

  @Override
  public void init(PinotConfiguration config, BrokerMetrics brokerMetrics) {
  }

  @Override
  public void registerUnhealthyServerRetrier(Function<String, Boolean> unhealthyServerRetrier) {
  }

  @Override
  public void registerHealthyServerNotifier(Consumer<String> healthyServerNotifier) {
  }

  @Override
  public void registerUnhealthyServerNotifier(Consumer<String> unhealthyServerNotifier) {
  }

  @Override
  public void start() {
  }

  @Override
  public void markServerHealthy(String instanceId) {
  }

  @Override
  public void markServerUnhealthy(String instanceId) {
  }

  @Override
  public Set<String> getUnhealthyServers() {
    return Collections.emptySet();
  }

  @Override
  public void stop() {
  }
}
