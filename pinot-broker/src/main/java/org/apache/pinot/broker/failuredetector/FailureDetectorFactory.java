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
package org.apache.pinot.broker.failuredetector;

import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FailureDetectorFactory {
  private FailureDetectorFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectorFactory.class);

  public static FailureDetector getFailureDetector(PinotConfiguration config, BrokerMetrics brokerMetrics) {
    String className = config.getProperty(Broker.FailureDetector.CONFIG_OF_CLASS_NAME);
    if (StringUtils.isEmpty(className)) {
      LOGGER.info("Class name is not configured, falling back to NoOpFailureDetector");
      return new NoOpFailureDetector();
    } else {
      LOGGER.info("Initializing failure detector with class: {}", className);
      try {
        FailureDetector failureDetector =
            (FailureDetector) Class.forName(className).getDeclaredConstructor().newInstance();
        failureDetector.init(config, brokerMetrics);
        LOGGER.info("Initialized failure detector with class: {}", className);
        return failureDetector;
      } catch (Exception e) {
        LOGGER.error(
            "Caught exception while initializing failure detector with class: {}, falling back to NoOpFailureDetector",
            className);
        return new NoOpFailureDetector();
      }
    }
  }
}
